mod node;
mod rpc;
mod state;

mod error;
pub use error::Error;

pub type Result<T> = std::result::Result<T, Error>;

#[cfg(test)]
mod simrpc;
