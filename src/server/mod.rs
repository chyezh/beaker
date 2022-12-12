mod connection;
mod handler;

mod servers;
pub use servers::Server;

mod error;
pub use error::Error;

pub type Result<T> = std::result::Result<T, Error>;
