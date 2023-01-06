use super::rpc::*;
use super::state::State;
use rand::Rng;
use std::{
    ops::Add,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::{
    sync::mpsc::channel,
    time::{self, Instant},
};
use tonic::{Request, Response, Status};

pub struct RafterInner {
    // Manage all peers rpc client
    peers: Vec<RaftClient>,

    // State of this node
    state: Arc<Mutex<State>>,

    // next triggering election time
    election_instant: Instant,
}

impl RafterInner {
    fn new(peers: Vec<RaftClient>, me: usize) -> Self {
        let peers_count = peers.len();
        RafterInner {
            peers,
            state: Arc::new(Mutex::new(State::new(peers_count, me))),
            election_instant: Instant::now(),
        }
    }

    // Try to start a new election
    fn trigger_election(&mut self) {
        let mut state = self.state.lock().unwrap();
        // Leader always don't trigger election.
        if state.is_leader() {
            return;
        }

        // Set as candidate and vote for myself
        state.set_as_candidate();
        let current_term = state.term();
        let me = state.me();

        // Send request to all peers except this node
        let (tx, mut rx) = channel(self.peers.len() - 1);
        for (node_id, peer) in self.peers.iter().enumerate() {
            if node_id != me {
                let mut peer = peer.clone();
                let tx = tx.clone();

                let args = RequestVoteArgs {
                    term: current_term,
                    candidate_id: me as u64,
                    // TODO: check last log index
                    last_log_index: 0,
                    last_log_term: 0,
                };

                tokio::spawn(async move {
                    let vote_reply = peer.request_vote(args).await;
                    tx.send(vote_reply).await.ok();
                });
            }
        }

        // Async receiving the vote reply, and handle the election
        let voted_threshold = self.peers.len() / 2;
        let state = Arc::clone(&self.state);
        tokio::spawn(async move {
            // Candidates always voted for myself
            let mut voted = 1;

            while let Some(Ok(vote_reply)) = rx.recv().await {
                let vote_reply = vote_reply.get_ref();
                if vote_reply.term == current_term && vote_reply.vote_granted {
                    voted += 1;
                    if voted > voted_threshold {
                        // Quorum of peers voted for this node, break to set this node as leader
                        break;
                    }
                } else if current_term < vote_reply.term {
                    // Find a new term
                    // Set this node into a follower, give up election process in this term
                    state
                        .lock()
                        .unwrap()
                        .set_as_follower_in_new_term(vote_reply.term);
                    return;
                }
            }

            if voted > voted_threshold {
                // Election success, set this peer as leader if current term is not changed
                state
                    .lock()
                    .unwrap()
                    .set_as_leader_in_this_term(current_term);
            }
        });

        // Release the lock for current node's state by RAII
    }

    fn trigger_heartbeat(&self) {
        let state = self.state.lock().unwrap();

        // Only leader trigger a heartbeat test
        if !state.is_leader() {
            return;
        }

        // Sending heartbeat test to every follower
        let (tx, mut rx) = channel(self.peers.len() - 1);
        let current_term = state.term();
        let leader_id = state.me();
        for (node_id, peer) in self.peers.iter().enumerate() {
            if node_id != leader_id {
                let mut peer = peer.clone();
                let args = AppendEntriesArgs {
                    term: current_term,
                    leader_id: leader_id as u64,
                    // TODO: fill other field
                    prev_log_index: 0,
                    prev_log_term: 0,
                    leader_commit: 0,
                    entries: 0,
                };
                let tx = tx.clone();
                tokio::spawn(async move {
                    let heartbeat_reply = peer.append_entries(args).await;
                    tx.send(heartbeat_reply).await.ok();
                });
            }
        }

        let state = Arc::clone(&self.state);
        tokio::spawn(async move {
            while let Some(Ok(heartbeat_reply)) = rx.recv().await {
                let heartbeat_reply = heartbeat_reply.get_ref();
                if heartbeat_reply.term > current_term {
                    // New term and new election was coming, give up being a leader.
                    state
                        .lock()
                        .unwrap()
                        .set_as_follower_in_new_term(heartbeat_reply.term);
                    // No need to do remaining heartbeat test
                    return;
                }
            }
        });
    }

    // Implement server side of request vote rpc
    fn vote(&mut self, args: &RequestVoteArgs) -> RequestVoteReply {
        // refresh election timer
        self.refresh_election_instant();

        let mut state = self.state.lock().unwrap();
        let current_term = state.term();
        if args.term < current_term {
            // Request vote rpc's term is out of date
            return RequestVoteReply {
                term: current_term,
                vote_granted: false,
            };
        }

        if args.term == current_term {
            // Vote in current term, every node should vote only once time
            match state.voted_for() {
                Some(id) if id == args.candidate_id as usize => {
                    // Already vote for this candidate in current term
                    // Maybe request was duplicated because of network problem
                    state.set_as_follower_in_new_term(args.term);
                    RequestVoteReply {
                        term: current_term,
                        vote_granted: true,
                    }
                }
                None => {
                    // May never hit in normal implementation
                    state.set_as_follower_in_new_term(args.term);
                    state.vote_for(args.candidate_id);
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
            state.set_as_follower_in_new_term(args.term);
            state.vote_for(args.candidate_id);
            RequestVoteReply {
                term: current_term,
                vote_granted: true,
            }
        }
    }

    // Implement server side of append-entries rpc
    fn append_entries(&mut self, args: &AppendEntriesArgs) -> AppendEntriesReply {
        // Refresh election timer
        self.refresh_election_instant();

        let mut state = self.state.lock().unwrap();
        let current_term = state.term();
        if args.term < current_term {
            return AppendEntriesReply {
                term: current_term,
                success: false,
            };
        }

        // Set as follower if new term is coming
        state.set_as_follower_in_new_term(args.term);

        AppendEntriesReply {
            term: current_term,
            success: true,
        }
    }

    // Refresh election instant
    #[inline]
    fn refresh_election_instant(&mut self) {
        // TODO: configure for election instant refresh range
        self.election_instant = Instant::now().add(Duration::from_millis(
            rand::thread_rng().gen_range(150..=300),
        ));
    }
}

// Implementation for Raft algorithm
pub struct Rafter {
    inner: Arc<Mutex<RafterInner>>,
}

impl Rafter {
    pub fn new(peers: Vec<RaftClient>, me_index: usize) -> Self {
        let raft = Rafter {
            inner: Arc::new(Mutex::new(RafterInner::new(peers, me_index))),
        };

        raft.start_election_task();
        raft.start_heartbeat_task();

        raft
    }

    fn start_election_task(&self) {
        let raft = Arc::clone(&self.inner);
        raft.lock().unwrap().refresh_election_instant();

        tokio::spawn(async move {
            loop {
                // Sleep until next election time instant and check the instant again
                let next_election_instatnt = raft.lock().unwrap().election_instant;
                if next_election_instatnt < Instant::now() {
                    raft.lock().unwrap().trigger_election();
                    raft.lock().unwrap().refresh_election_instant();
                }
                time::sleep_until(next_election_instatnt).await;
            }
        });
    }

    fn start_heartbeat_task(&self) {
        let raft = Arc::clone(&self.inner);
        tokio::spawn(async move {
            // TODO: should configurable
            let mut interval = time::interval(Duration::from_millis(25));
            loop {
                // Send heartbeat periodically
                interval.tick().await;
                raft.lock().unwrap().trigger_heartbeat();
            }
        });
    }
}

#[tonic::async_trait]
impl Raft for Rafter {
    // Implement server side of request-vote rpc for raft
    async fn request_vote(
        &self,
        request: Request<RequestVoteArgs>,
    ) -> Result<Response<RequestVoteReply>, Status> {
        // TODO: Lock optimize
        Ok(Response::new(
            self.inner.lock().unwrap().vote(request.get_ref()),
        ))
    }

    // Implement server side of append-entries rpc for raft
    async fn append_entries(
        &self,
        request: Request<AppendEntriesArgs>,
    ) -> Result<Response<AppendEntriesReply>, Status> {
        Ok(Response::new(
            self.inner.lock().unwrap().append_entries(request.get_ref()),
        ))
    }
}
