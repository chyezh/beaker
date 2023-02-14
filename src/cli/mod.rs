mod argument;
mod cli_impl;
mod command;
mod error;

pub use cli_impl::Cli;

pub type Result<T> = std::result::Result<T, error::Error>;
