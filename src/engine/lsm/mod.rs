use bytes::Bytes;

mod error;
pub use error::Error;

mod log;
mod memtable;
mod sstable;

pub type Result<T> = std::result::Result<T, Error>;

// LSM-Tree kv element type
struct Pair {
    key: Bytes,
    value: Bytes,
}
