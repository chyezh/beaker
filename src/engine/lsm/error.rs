#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("io")]
    Io {
        #[from]
        source: std::io::Error,
    },

    #[error("illegal log record")]
    IllegalLogRecord,
}
