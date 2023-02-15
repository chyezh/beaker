#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error(transparent)]
    Io(#[from] std::io::Error),

    #[error("illegal value binary format")]
    IllegalValueBinary,

    #[error("illegal record")]
    IllegalRecord,

    #[error("illegal log")]
    IllegalLog,

    #[error("inconsistent log id")]
    InconsistentLogId,

    #[error("bad sstable")]
    BadSSTable,

    #[error("bad sstable block")]
    BadSSTableBlock,

    #[error("bad sstable index")]
    BadSSTableIndex,

    #[error("bad sstable footer")]
    BadSSTableBlockFooter,

    #[error("illegal sstable entry")]
    IllegalSSTableEntry,

    #[error("sstable entry lock failed")]
    SSTableEntryLockFailed,

    #[error("illegal manifest")]
    IllegalManifest,

    #[error("other")]
    Other,
}
