#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error(transparent)]
    Io(#[from] std::io::Error),

    #[error("invalid format for RESP")]
    Invalid,

    #[error("incomplete frame for RESP")]
    Incomplete,

    #[error("connection is reset by peer")]
    ConnReset,

    #[error("end of stream")]
    Eos,
}

impl Error {
    #[inline]
    pub fn is_eos(&self) -> bool {
        matches!(self, Self::Eos)
    }
}
