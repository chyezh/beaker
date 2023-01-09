use tokio::sync::broadcast::error::SendError;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("tonic status")]
    TonicStatus {
        #[from]
        source: tonic::Status,
    },

    #[error("transport error")]
    Transport {
        #[from]
        source: tonic::transport::Error,
    },

    #[cfg(test)]
    #[error("test error")]
    Test {
        #[from]
        source: super::simrpc::TestError,
    },
}
