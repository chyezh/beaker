#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error(transparent)]
    Resp(#[from] crate::resp::Error),

    #[error("unexpected command type")]
    UnexpectedCommandType,

    #[error("lost arguments: {0}")]
    LostArgs(String),

    #[error("{0}")]
    Any(String),
}
