mod block;
mod compact;
mod config;
mod db;
mod error;
mod event;
mod kvtable;
mod manifest;
mod memtable;
mod record;
mod snapshot;
mod sstable;
mod util;

pub use config::Config;
pub use db::DB;
pub use error::Error;

// Module result type
pub type Result<T> = std::result::Result<T, Error>;
