use std::str::FromStr;

use crate::resp::{Frame, Parser};
mod error;
pub use error::Error;

mod get;
pub use get::Get;

mod ping;
pub use ping::Ping;

mod set;
pub use set::Set;

mod del;
pub use del::Del;

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

        let command_name = parser.next_string()?.to_lowercase();

        let cmd = match command_name.as_str() {
            "get" => Command::Get(Get::from_parser(&mut parser)?),
            "ping" => Command::Ping(Ping::from_parser(&mut parser)?),
            "set" => Command::Set(Set::from_parser(&mut parser)?),
            "del" => Command::Del(Del::from_parser(&mut parser)?),
            _ => return Err(Error::UnexpectedCommandType),
        };
        parser.check_finish()?;

        Ok(cmd)
    }
}

impl FromStr for Command {
    type Err = Error;

    // Convert string from terminal input into command struct
    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        let s = s.trim_start();

        let (command_name, remain) = if let Some((cmd, content)) = s.split_once(' ') {
            (cmd, content)
        } else {
            (s, "")
        };

        todo!()
    }
}

pub trait ResponseParser {
    type Response;

    fn parse_response(&self, parser: &mut Parser) -> Result<Self::Response>;
}
