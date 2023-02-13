use std::fmt::Debug;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("{0}")]
    Nom(String),

    #[error("incomplete command line input parse")]
    ParsingIncomplete,

    #[error("invalid arguments count")]
    InvalidArgsCount,

    #[error(transparent)]
    Client(#[from] crate::client::Error),

    #[error(transparent)]
    Io(#[from] std::io::Error),

    #[error(transparent)]
    Utf8(#[from] std::string::FromUtf8Error),
}

impl<E: Debug> From<nom::Err<E>> for Error {
    fn from(value: nom::Err<E>) -> Self {
        Error::Nom(format!("nom parsing failed: {:?}", value))
    }
}
