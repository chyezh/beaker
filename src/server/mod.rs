mod argument;
mod error;
mod handler;
mod server_impl;

pub use error::Error;
pub use server_impl::Server;

pub type Result<T> = std::result::Result<T, Error>;
