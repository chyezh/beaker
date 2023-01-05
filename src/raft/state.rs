#[derive(Default, Copy, Clone, Debug, PartialEq, Eq)]
enum Role {
    #[default]
    Follower,
    Candidate,
    Leader,
}

#[derive(Debug, Default)]
struct Peer {
    // Index of the next log entry to send to that server
    // Initialized to leader last log index + 1
    next_index: u64,

    // Index of highest log entry known to be replicated on server
    // Initialized to 0, increases monotonically
    match_index: u64,
}

#[derive(Debug, Default)]
pub struct State {
    // Role of this node
    role: Role,

    // This node's index in peers
    me_index: usize,

    // Latest term server has seen, increases monotonically
    current_term: u64,

    // Candidate id (index of peers array) that received voted in current term
    // None if not voted.
    voted_for: Option<usize>,

    // Log entries; each entry contains command for state machine,
    // and term when entry was received by leader
    log: Vec<u64>,

    // Index of highest log entry known to be committed
    commit_index: u64,

    // Index of highest log entry applied to state machine
    last_applied: u64,

    // List of Peer meta data, reinitialized after election
    // Only available on leader node
    peers: Vec<Peer>,
}

impl State {
    #[inline]
    pub fn is_leader(&self) -> bool {
        matches!(self.role, Role::Leader)
    }

    #[inline]
    pub fn term(&self) -> u64 {
        self.current_term
    }

    // Perform a CAS, set this node as leader, if term is matched.
    #[inline]
    pub fn set_as_leader_in_this_term(&mut self, term: u64) {
        if self.current_term == term {
            self.role = Role::Leader;
        }
    }

    // Set this node as follower and update the term if term is gte than current term
    #[inline]
    pub fn set_as_follower_in_new_term(&mut self, term: u64) {
        if term >= self.current_term {
            self.current_term = term;
            self.role = Role::Follower;
        }
    }

    // Set this node as candidate, increment current term
    // Always vote for myself
    #[inline]
    pub fn set_as_candidate(&mut self) {
        self.current_term += 1;
        self.role = Role::Candidate;
        self.voted_for = Some(self.me_index);
    }

    // Vote for peer at peers[candidate_id]
    #[inline]
    pub fn vote_for(&mut self, candidate_id: u64) {
        self.voted_for = Some(candidate_id as usize);
    }

    // Access me index
    #[inline]
    pub fn me(&self) -> usize {
        self.me_index
    }
}
