#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error(transparent)]
    Resp(#[from] crate::resp::Error),

    #[error("unexpected command type")]
    UnexpectedCommandType,

    #[error("{0}")]
    Any(String),
}
