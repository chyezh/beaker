use bytes::Bytes;

mod error;
pub use error::Error;

mod memtable;

mod log;

mod sstable;

mod util;

// Module result type
pub type Result<T> = std::result::Result<T, Error>;

// LSM-Tree key
pub type Key = Vec<u8>;

// LSM-Tree value
#[derive(Debug, Clone)]
pub enum Value {
    Living(Vec<u8>),

    // Deleted value
    Tombstone,
}
