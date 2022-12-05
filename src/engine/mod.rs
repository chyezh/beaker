use crate::cmd::Command;

mod error;
pub use error::Error;

mod logger;
pub use logger::Logger;

pub type Result<T> = std::result::Result<T, Error>;
