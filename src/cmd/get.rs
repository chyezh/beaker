use super::{Frame, Parser, Result};
use bytes::Bytes;
use serde::{Deserialize, Serialize};

#[derive(Debug, Eq, PartialEq, Deserialize, Serialize)]
pub struct Get {
    key: String,
}

impl Get {
    // Create a new get command with a key
    pub fn new(key: String) -> Self {
        Get { key }
    }

    // Create a new get command from parser
    pub fn from_parser(parser: &mut Parser) -> Result<Self> {
        Ok(Get {
            key: parser.next_string()?,
        })
    }

    // Get key
    pub fn key(&self) -> &str {
        &self.key
    }

    // transfer command to Frame
    pub fn into_frame(self) -> Frame {
        Frame::Array(vec![
            Frame::Simple("get".into()),
            Frame::Bulk(Bytes::from(self.key)),
        ])
    }
}
