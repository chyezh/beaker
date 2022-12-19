#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("io")]
    Io {
        #[from]
        source: std::io::Error,
    },

    #[error("illegal log record")]
    IllegalLogRecord,

    #[error("illegal log")]
    IllegalLog,

    #[error("illegal table")]
    IllegalTable,

    #[error("bad sstable block")]
    BadSSTableBlock,
}
