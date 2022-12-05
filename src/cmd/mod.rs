use crate::resp::{Frame, Parser};
use serde::{Deserialize, Serialize};

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

#[derive(Debug, PartialEq, Eq, Deserialize, Serialize)]
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

    pub fn get(key: String) -> Self {
        Command::Get(Get::new(key))
    }

    pub fn ping() -> Self {
        Command::Ping(Ping::default())
    }

    pub fn set(key: String, val: String) -> Self {
        Command::Set(Set::new(key, val))
    }

    pub fn del(key: String) -> Self {
        Command::Del(Del::new(key))
    }
}
