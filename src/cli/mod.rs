mod cli_impl;
mod command;
mod error;

pub use error::Error;

pub type Result<T> = std::result::Result<T, Error>;
