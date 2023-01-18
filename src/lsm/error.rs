#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("io")]
    Io {
        #[from]
        source: std::io::Error,
    },

    #[error("illegal value binary format")]
    IllegalValueBinary,

    #[error("illegal record")]
    IllegalRecord,

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
