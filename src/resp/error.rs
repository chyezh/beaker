#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("invalid format for Resp")]
    Invalid,

    #[error("incomplete frame for Resp")]
    Incomplete,

    #[error("end of stream")]
    Eos,

    #[error("io")]
    Io {
        #[from]
        source: std::io::Error,
    },
}
