use super::{Frame, Parser, Result};

#[derive(Debug, Eq, PartialEq, Default)]
pub struct Ping {}

impl Ping {
    // Create a new ping command from parser
    pub fn from_parser(_: &mut Parser) -> Result<Self> {
        Ok(Ping {})
    }

    // Transfer command to resp
    pub fn into_resp(self) -> Frame {
        Frame::Array(vec![Frame::Simple("ping".into())])
    }
}
