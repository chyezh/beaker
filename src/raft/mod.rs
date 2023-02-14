mod error;
mod event;
mod node;
mod rpc;
mod state;

pub use error::Error;
pub use node::Rafter;
pub type Result<T> = std::result::Result<T, Error>;

#[cfg(test)]
mod simrpc;
