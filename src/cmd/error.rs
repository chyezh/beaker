#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("unexpected command type")]
    UnexpectedCommandType,

    #[error("resp error")]
    RespError {
        #[from]
        source: crate::resp::Error,
    },
}
