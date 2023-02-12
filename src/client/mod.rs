mod client_impl;
mod error;

pub use client_impl::Client;
pub use error::Error;
pub type Result<T> = std::result::Result<T, Error>;
