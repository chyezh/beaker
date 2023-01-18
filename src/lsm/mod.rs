mod error;
pub use error::Error;

mod memtable;

mod record;

mod block;
mod sstable;

mod util;

// Module result type
pub type Result<T> = std::result::Result<T, Error>;
