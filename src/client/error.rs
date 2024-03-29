#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("io")]
    Io {
        #[from]
        source: std::io::Error,
    },

    #[error("resp")]
    Resp {
        #[from]
        source: crate::resp::Error,
    },

    #[error("cmd")]
    Cmd {
        #[from]
        source: crate::cmd::Error,
    },

    #[error("any")]
    Any(String),
}
