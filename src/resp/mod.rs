mod connection;
mod error;
mod frame;
mod parser;

pub use connection::{Connection, ConnectionGuard, ConnectionPool, Connector};
pub use error::Error;
pub use frame::{AsFrame, Frame};
pub use parser::Parser;

pub type Result<T> = std::result::Result<T, Error>;
