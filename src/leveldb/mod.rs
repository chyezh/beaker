mod error;
pub use error::Error;

mod block;
mod compact;
mod manifest;
mod memtable;
mod record;
mod sstable;
mod util;

// Module result type
pub type Result<T> = std::result::Result<T, Error>;
