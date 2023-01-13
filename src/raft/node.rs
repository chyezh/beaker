use super::state::State;
use super::{
    event::{Event, EventHandler},
    rpc::*,
};
use rand::Rng;
use std::{
    ops::Add,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::time::{self, Instant};
use tonic::{Request, Response, Status};

// Rafter must use the raft rpc client (RaftClient)
pub struct RafterInner<C> {
    // Manage all peers rpc client
    peers: Vec<C>,

    // State of this node
    state: State,

    // Next election time instant
    election_instant: Instant,
}

impl<C: RaftClient> RafterInner<C> {
    fn new(peers: Vec<C>, me: usize) -> Self {
        let peers_count = peers.len();
        RafterInner {
            peers,
            state: State::new(peers_count, me),
            election_instant: Instant::now(),
        }
    }

    // Prepare to do a election
    pub fn prepare_election(&mut self) -> Option<(Vec<C>, State)> {
        // Leader always don't trigger election
        if self.state.is_leader() {
            return None;
        }

        // Set as candidate and vote for myself
        self.state.set_as_candidate();

        // Return election-needed information
        Some((self.peers.clone(), self.state.clone()))
    }

    // Prepare to do a heartbeat
    pub fn prepare_heartbeat(&self) -> Option<(Vec<C>, State)> {
        // Only leader trigger a heartbeat
        if !self.state.is_leader() {
            return None;
        }

        // Return heartbeat-needed information
        Some((self.peers.clone(), self.state.clone()))
    }

    // Set this node as follower
    #[inline]
    pub fn set_as_follower_in_new_term(&mut self, term: u64) -> bool {
        self.state.set_as_follower_in_new_term(term)
    }

    // Set this node as leader
    #[inline]
    pub fn set_as_leader_in_this_term(&mut self, term: u64) -> bool {
        self.state.set_as_leader_in_this_term(term)
    }

    // Implement server side of request vote rpc
    fn request_vote(&mut self, args: &RequestVoteArgs) -> RequestVoteReply {
        let current_term = self.state.term();
        if args.term < current_term {
            // Request vote rpc's term is out of date
            return RequestVoteReply {
                term: current_term,
                vote_granted: false,
            };
        }

        // Refresh election timer
        self.force_refresh_election_instant();

        if args.term == current_term {
            // Vote in current term, every node should vote only once time
            match self.state.voted_for() {
                Some(id) if id == args.candidate_id as usize => {
                    // Already vote for this candidate in current term
                    // Maybe request was duplicated because of network problem
                    self.state.set_as_follower_in_new_term(args.term);
                    RequestVoteReply {
                        term: current_term,
                        vote_granted: true,
                    }
                }
                None => {
                    // May never hit in normal implementation
                    self.state.set_as_follower_in_new_term(args.term);
                    self.state.vote_for(args.candidate_id);
                    RequestVoteReply {
                        term: current_term,
                        vote_granted: true,
                    }
                }
                _ => RequestVoteReply {
                    term: current_term,
                    vote_granted: false,
                },
            }
        } else {
            // New term is coming, vote following first-come-first-served basis
            self.state.set_as_follower_in_new_term(args.term);
            self.state.vote_for(args.candidate_id);
            RequestVoteReply {
                term: args.term,
                vote_granted: true,
            }
        }
    }

    // Implement server side of append-entries rpc
    fn append_entries(&mut self, args: &AppendEntriesArgs) -> AppendEntriesReply {
        let current_term = self.state.term();
        if args.term < current_term {
            return AppendEntriesReply {
                term: current_term,
                success: false,
            };
        }

        // Set as follower if new term is coming
        self.state.set_as_follower_in_new_term(args.term);

        // Refresh election timer if receive append entries or heartbeat
        self.force_refresh_election_instant();

        AppendEntriesReply {
            term: current_term,
            success: true,
        }
    }

    // Refresh election instant
    #[inline]
    fn refresh_election_instant(&mut self) -> (Instant, bool) {
        // TODO: configure for election instant refresh range
        let now = Instant::now();
        if self.election_instant < now {
            (self.force_refresh_election_instant(), true)
        } else {
            (self.election_instant, false)
        }
    }

    // Refresh election instant
    #[inline]
    fn force_refresh_election_instant(&mut self) -> Instant {
        // TODO: configure for election instant refresh range
        self.election_instant = Instant::now().add(Duration::from_millis(
            rand::thread_rng().gen_range(150..=300),
        ));
        self.election_instant
    }
}

#[derive(Clone)]
// Implementation for Raft algorithm
pub struct Rafter<C> {
    // TODO: Lock optimization
    // Raft node inner state
    inner: Arc<Mutex<RafterInner<C>>>,
    // Raft event handler
    event_handler: EventHandler<C>,
    // election timeout
    election_instant: Arc<Mutex<Instant>>,
}

impl<C: RaftClient> Rafter<C> {
    // Create a new Rafter
    pub fn new(peers: Vec<C>, me_index: usize) -> Self {
        let inner = Arc::new(Mutex::new(RafterInner::new(peers, me_index)));
        let event_handler = EventHandler::new(Arc::clone(&inner));

        let rf = Rafter {
            inner,
            event_handler,
            election_instant: Arc::new(Mutex::new(Instant::now())),
        };

        rf.start_election_timed_task();
        rf.start_heartbeat_period_task();

        rf
    }

    // Start election timed task
    fn start_election_timed_task(&self) {
        let inner = Arc::clone(&self.inner);
        let event_channel = self.event_handler.clone_channel();
        tokio::spawn(async move {
            // Sleep until next election time instant
            inner.lock().unwrap().force_refresh_election_instant();
            loop {
                let (next_election_instant, refresh) =
                    inner.lock().unwrap().refresh_election_instant();

                if refresh {
                    // Start a new election event if reaching next election time
                    // Send operation would never failed
                    event_channel.send(Event::Election).unwrap();
                }

                // sleep util next election instant
                time::sleep_until(next_election_instant).await;
            }
        });
    }

    // Start heartbeat period task
    fn start_heartbeat_period_task(&self) {
        let event_channel = self.event_handler.clone_channel();
        tokio::spawn(async move {
            // TODO: should be configurable
            let mut interval = time::interval(Duration::from_millis(100));
            loop {
                // Send heartbeat periodically
                // Send operation would never failed
                interval.tick().await;
                event_channel.send(Event::Heartbeat).unwrap();
            }
        });
    }
}

// Use tonic rpc library to implement raft rpc server
#[tonic::async_trait]
impl<C: RaftClient> Raft for Rafter<C> {
    // Implement server side of request-vote rpc for raft
    async fn request_vote(
        &self,
        request: Request<RequestVoteArgs>,
    ) -> std::result::Result<Response<RequestVoteReply>, Status> {
        // TODO: Lock optimize
        Ok(Response::new(
            self.inner.lock().unwrap().request_vote(request.get_ref()),
        ))
    }

    // Implement server side of append-entries rpc for raft
    async fn append_entries(
        &self,
        request: Request<AppendEntriesArgs>,
    ) -> std::result::Result<Response<AppendEntriesReply>, Status> {
        Ok(Response::new(
            self.inner.lock().unwrap().append_entries(request.get_ref()),
        ))
    }
}

#[cfg(test)]
mod test {
    const REQUEST_VOTE_SERVICE_NAME: &str = "request_vote";
    const APPEND_ENTRIES_SERVICE_NAME: &str = "append_entries";

    use super::{
        super::{
            rpc::RaftClient,
            simrpc::{Client, Network, Request, Server, ServerBuilder, Service, TestError},
            Result,
        },
        AppendEntriesArgs, AppendEntriesReply, Rafter, RequestVoteArgs, RequestVoteReply,
    };
    use prost::Message;
    use tokio::sync::mpsc;
    use tonic::async_trait;

    #[test]
    fn test_leader_election() {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .expect("Failed building the tokio runtime");
        let _guard = runtime.enter();

        let (network, channel) = create_network(5);

        runtime.block_on(async move { network.start(channel).await });
    }

    // Implement simulation environment for testing
    #[derive(Clone)]
    struct TestRaftClient {
        client: Client,
    }

    #[async_trait]
    impl RaftClient for TestRaftClient {
        // Request vote rpc
        async fn request_vote(&mut self, request: RequestVoteArgs) -> Result<RequestVoteReply> {
            let v = request.encode_to_vec();

            let response = self
                .client
                .rpc(Request::new(REQUEST_VOTE_SERVICE_NAME.to_string(), v))
                .await?;

            let response = RequestVoteReply::decode(&response.data[..])
                .map_err(|_| TestError::SerializationFailed)?;
            Ok(response)
        }

        // Append entries rpc
        async fn append_entries(
            &mut self,
            request: AppendEntriesArgs,
        ) -> Result<AppendEntriesReply> {
            let v = request.encode_to_vec();
            let response = self
                .client
                .rpc(Request::new(APPEND_ENTRIES_SERVICE_NAME.to_string(), v))
                .await?;

            let response = AppendEntriesReply::decode(&response.data[..])
                .map_err(|_| TestError::SerializationFailed)?;
            Ok(response)
        }
    }

    struct RequestVoteService<C>(Rafter<C>);
    struct AppendEntriesService<C>(Rafter<C>);

    #[async_trait]
    impl<C: RaftClient> Service for RequestVoteService<C> {
        async fn handle(&self, data: &[u8]) -> Result<Vec<u8>> {
            let request =
                RequestVoteArgs::decode(data).map_err(|_| TestError::SerializationFailed)?;
            let response = self.0.inner.lock().unwrap().request_vote(&request);
            Ok(response.encode_to_vec())
        }
    }

    #[async_trait]
    impl<C: RaftClient> Service for AppendEntriesService<C> {
        async fn handle(&self, data: &[u8]) -> Result<Vec<u8>> {
            let request =
                AppendEntriesArgs::decode(data).map_err(|_| TestError::SerializationFailed)?;
            let response = self.0.inner.lock().unwrap().append_entries(&request);
            Ok(response.encode_to_vec())
        }
    }

    fn create_raft_server<C: RaftClient>(peers: Vec<C>, me_index: usize) -> Server {
        let rafter = Rafter::new(peers, me_index);

        let mut server = ServerBuilder::new(format!("server_{}", me_index));
        server.add_service(
            REQUEST_VOTE_SERVICE_NAME.to_string(),
            Box::new(RequestVoteService(rafter.clone())),
        );
        server.add_service(
            APPEND_ENTRIES_SERVICE_NAME.to_string(),
            Box::new(AppendEntriesService(rafter)),
        );
        server.build()
    }

    fn create_network(node_num: usize) -> (Network, mpsc::Receiver<Request>) {
        let mut network = Network::new();
        // Create a network channel
        let (tx, rx) = mpsc::channel(10);

        for server_index in 0..node_num {
            // Create client in every server
            let mut clients = Vec::with_capacity(node_num);
            for client_index in 0..node_num {
                // Create client
                let client = Client::new(
                    format!("client_{}_on_server_{}", client_index, server_index),
                    tx.clone(),
                );
                clients.push(TestRaftClient {
                    client: client.clone(),
                });

                // add connection
                network.connect(client.name(), &format!("server_{}", client_index));
            }

            // Create server and add to network
            let server = create_raft_server(clients.clone(), server_index);
            network.add_server(server.clone());
        }

        (network, rx)
    }
}
