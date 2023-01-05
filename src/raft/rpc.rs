pub use raftrpc::{
    raft_server::{Raft, RaftServer},
    AppendEntriesArgs, AppendEntriesReply, RequestVoteArgs, RequestVoteReply,
};

pub type RaftClient = raftrpc::raft_client::RaftClient<tonic::transport::Channel>;

pub mod raftrpc {
    tonic::include_proto!("raftrpc");
}
