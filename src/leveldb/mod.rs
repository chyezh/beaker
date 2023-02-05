mod block;
mod compact;
mod manifest;
mod memtable;
mod record;
mod sstable;
mod util;

mod db;
pub use db::DB;

mod error;
pub use error::Error;

// Module result type
pub type Result<T> = std::result::Result<T, Error>;
