#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("io")]
    Io {
        #[from]
        source: std::io::Error,
    },

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

    #[error("illegal log record")]
    IllegalLogRecord,

    #[error("illegal log")]
    IllegalLog,

    #[cfg(test)]
    #[error("test error")]
    Test {
        #[from]
        source: super::simrpc::TestError,
    },
}
