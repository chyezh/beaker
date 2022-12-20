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

    #[error("bad sstable")]
    BadSSTable,

    #[error("bad sstable block")]
    BadSSTableBlock,

    #[error("bad sstable index")]
    BadSSTableIndex,

    #[error("bad sstable footer")]
    BadSSTableBlockFooter,
}
