use crate::resp::{Frame, Parser};

mod del;
mod error;
mod get;
mod ping;
mod set;

pub use del::{Del, DEL_COMMAND_NAME};
pub use error::Error;
pub use get::{Get, GET_COMMAND_NAME};
pub use ping::{Ping, PING_COMMAND_NAME};
pub use set::{Set, SET_COMMAND_NAME};
pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, PartialEq, Eq)]
pub enum Command {
    Get(Get),
    Ping(Ping),
    Set(Set),
    Del(Del),
}

impl Command {
    // Create new command from frame item
    pub fn from_frame(data: std::vec::IntoIter<Frame>) -> Result<Command> {
        let mut parser = Parser::new(data);

        let command_name = parser.next_string()?.to_uppercase();

        let cmd = match command_name.as_str() {
            GET_COMMAND_NAME => Get::from_parser(&mut parser)?.into(),
            PING_COMMAND_NAME => Ping::from_parser(&mut parser)?.into(),
            SET_COMMAND_NAME => Set::from_parser(&mut parser)?.into(),
            DEL_COMMAND_NAME => Del::from_parser(&mut parser)?.into(),
            _ => return Err(Error::UnexpectedCommandType),
        };
        parser.check_finish()?;

        Ok(cmd)
    }
}

pub trait ResponseParser {
    type Response;

    fn parse_response(&self, parser: &mut Parser) -> Result<Self::Response>;
}

/// Implement transformation from underlying command
impl From<Get> for Command {
    fn from(value: Get) -> Self {
        Self::Get(value)
    }
}

/// Implement transformation from underlying command
impl From<Ping> for Command {
    fn from(value: Ping) -> Self {
        Self::Ping(value)
    }
}

/// Implement transformation from underlying command
impl From<Del> for Command {
    fn from(value: Del) -> Self {
        Self::Del(value)
    }
}

/// Implement transformation from underlying command
impl From<Set> for Command {
    fn from(value: Set) -> Self {
        Self::Set(value)
    }
}
