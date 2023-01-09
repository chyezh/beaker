use super::Result;
pub use raftrpc::{
    raft_server::{Raft, RaftServer},
    AppendEntriesArgs, AppendEntriesReply, RequestVoteArgs, RequestVoteReply,
};
use tonic::async_trait;

#[async_trait]
pub trait RaftClient: Clone + Send + Sync + 'static {
    // Request vote rpc
    async fn request_vote(&mut self, request: RequestVoteArgs) -> Result<RequestVoteReply>;

    // Append entries rpc
    async fn append_entries(&mut self, request: AppendEntriesArgs) -> Result<AppendEntriesReply>;
}

#[derive(Clone)]
pub struct TonicRaftClient(raftrpc::raft_client::RaftClient<tonic::transport::Channel>);

impl TonicRaftClient {
    // Connect to the endpoint
    #[inline]
    pub async fn connect<D>(dst: D) -> Result<Self>
    where
        D: std::convert::TryInto<tonic::transport::Endpoint>,
        D::Error: Into<tonic::codegen::StdError>,
    {
        let conn = raftrpc::raft_client::RaftClient::connect(dst).await?;
        Ok(TonicRaftClient(conn))
    }
}

#[async_trait]
impl RaftClient for TonicRaftClient {
    #[inline]
    async fn request_vote(&mut self, request: RequestVoteArgs) -> Result<RequestVoteReply> {
        Ok(self.0.request_vote(request).await?.into_inner())
    }

    #[inline]
    async fn append_entries(&mut self, request: AppendEntriesArgs) -> Result<AppendEntriesReply> {
        Ok(self.0.append_entries(request).await?.into_inner())
    }
}

pub mod raftrpc {
    tonic::include_proto!("raftrpc");
}
