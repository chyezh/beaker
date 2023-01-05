use super::rpc::*;
use super::state::State;
use std::sync::{Arc, Mutex};
use tokio::{sync::mpsc::channel, time::Instant};
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
    }
}

// Implementation for Raft algorithm
pub struct Rafter {}

#[tonic::async_trait]
impl Raft for Rafter {
    // Implement server side of request-vote rpc for raft
    async fn request_vote(
        &self,
        request: Request<RequestVoteArgs>,
    ) -> Result<Response<RequestVoteReply>, Status> {
        todo!()
    }

    // Implement server side of append-entries rpc for raft
    async fn append_entries(
        &self,
        request: Request<AppendEntriesArgs>,
    ) -> Result<Response<AppendEntriesReply>, Status> {
        todo!()
    }
}
