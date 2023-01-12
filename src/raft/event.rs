// Implement event loop and events handler in single raft node

use std::sync::{Arc, Mutex};

use super::rpc::*;
use super::{node::RafterInner, rpc::RaftClient};
use tokio::sync::mpsc::{self, channel};
use tokio::time::{self, Instant};

#[derive(Debug)]
pub enum Event {
    Election,
    Heartbeat,
    Request,
    AppendEntries,
}

#[derive(Clone)]
pub struct EventHandler<C> {
    rafter: Arc<Mutex<RafterInner<C>>>,
    tx: mpsc::UnboundedSender<Event>,
    election_instant: Arc<Mutex<Instant>>,
}

impl<C: RaftClient> EventHandler<C> {
    pub fn new(rafter: Arc<Mutex<RafterInner<C>>>) -> Self {
        let (tx, rx) = mpsc::unbounded_channel();
        let mut events = EventHandler {
            rafter,
            tx,
            election_instant: Arc::new(Mutex::new(Instant::now())),
        };

        events.start_loop(rx);
        events
    }

    // Clone new Event Channel
    pub fn clone_channel(&self) -> mpsc::UnboundedSender<Event> {
        self.tx.clone()
    }

    // Start a event loop with given event receiver
    fn start_loop(&mut self, mut rx: mpsc::UnboundedReceiver<Event>) {
        let rafter = Arc::clone(&self.rafter);
        tokio::spawn(async move {
            // rx would never closed
            while let Some(event) = rx.recv().await {
                let rf = Arc::clone(&rafter);
                match event {
                    Event::Election => election(rf).await,
                    Event::Heartbeat => heartbeat(rf).await,
                    Event::Request => unimplemented!(),
                    Event::AppendEntries => unimplemented!(),
                }
            }
        });
    }
}

// Process a election
async fn election<C: RaftClient>(rafter: Arc<Mutex<RafterInner<C>>>) {
    let prepare_result = rafter.lock().unwrap().prepare_election();
    if prepare_result.is_none() {
        return;
    }

    let (peers, state) = prepare_result.unwrap();

    // Set as candidate and vote for myself
    let current_term = state.term();
    let me = state.me();
    let peers_count = peers.len();
    let voted_threshold = peers_count / 2;

    // Send request to all peers except this node
    let (tx, mut rx) = channel(peers_count - 1);
    for (node_id, peer) in peers.iter().enumerate() {
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

    // Wait to handle the election reply
    // Async receiving the vote reply, and handle the election
    // Candidates always voted for myself
    tokio::spawn(async move {
        let mut voted = 1;
        while let Some(Ok(vote_reply)) = rx.recv().await {
            if vote_reply.term == current_term && vote_reply.vote_granted {
                voted += 1;
                if voted > voted_threshold {
                    // Quorum of peers voted for this node, break to set this node as leader
                    break;
                }
            } else if current_term < vote_reply.term {
                // Find a new term
                // Set this node into a follower, give up election process in this term
                rafter
                    .lock()
                    .unwrap()
                    .set_as_follower_in_new_term(vote_reply.term);
                return;
            }
        }

        // Election success, set this peer as leader if current term is not changed
        // Trigger a new heartbeat right now after becoming leader
        if voted > voted_threshold
            && rafter
                .lock()
                .unwrap()
                .set_as_leader_in_this_term(current_term)
        {
            heartbeat(rafter).await;
        }
    });
}

// Process the heartbeat
async fn heartbeat<C: RaftClient>(rafter: Arc<Mutex<RafterInner<C>>>) {
    let prepare_result = rafter.lock().unwrap().prepare_heartbeat();
    if prepare_result.is_none() {
        return;
    }

    let (peers, state) = prepare_result.unwrap();

    // Sending heartbeat test to every follower
    let current_term = state.term();
    let leader_id = state.me();

    let (tx, mut rx) = channel(peers.len() - 1);
    for (node_id, peer) in peers.iter().enumerate() {
        if node_id != leader_id {
            let mut peer = (*peer).clone();
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

    // Wait to handle the heartbeat reply
    // Async receiving the heartbeat reply
    tokio::spawn(async move {
        while let Some(Ok(heartbeat_reply)) = rx.recv().await {
            let heartbeat_reply = heartbeat_reply;
            if heartbeat_reply.term > current_term {
                // New term and new election was coming, give up being a leader.
                rafter
                    .lock()
                    .unwrap()
                    .set_as_follower_in_new_term(heartbeat_reply.term);
                // No need to do remaining heartbeat test
                return;
            }
        }
    });
}
