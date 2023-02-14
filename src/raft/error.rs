#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error(transparent)]
    Io(#[from] std::io::Error),

    #[error(transparent)]
    TonicStatus(#[from] tonic::Status),

    #[error(transparent)]
    Transport(#[from] tonic::transport::Error),

    #[error("illegal log record")]
    IllegalLogRecord,

    #[error("illegal log")]
    IllegalLog,

    #[cfg(test)]
    #[error("test error")]
    Test(#[from] super::simrpc::TestError),
}
