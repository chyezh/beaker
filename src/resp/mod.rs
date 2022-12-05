mod error;
mod frame;
mod parser;

pub use error::Error;
pub use frame::Frame;
pub use parser::Parser;

pub type Result<T> = std::result::Result<T, Error>;
