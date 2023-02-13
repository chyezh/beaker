#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error(transparent)]
    Io(#[from] std::io::Error),

    #[error(transparent)]
    Resp(#[from] crate::resp::Error),

    #[error(transparent)]
    Db(#[from] crate::engine::Error),

    #[error(transparent)]
    Cmd(#[from] crate::cmd::Error),
}
