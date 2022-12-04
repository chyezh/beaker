use std::io;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("invalid format for Resp")]
    Invalid,

    #[error("incomplete frame for Resp")]
    Incomplete,

    #[error("end of stream")]
    EOS,

    #[error("io")]
    Io {
        #[from]
        source: io::Error,
    },
}
