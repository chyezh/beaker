use super::{Frame, Parser, Result};
use bytes::Bytes;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Eq, Deserialize, Serialize)]
pub struct Del {
    key: String,
}

impl Del {
    // Create a new del command with a key
    pub fn new(key: String) -> Self {
        Del { key }
    }

    // Create a new del command from parser
    pub fn from_parser(parser: &mut Parser) -> Result<Self> {
        Ok(Del {
            key: parser.next_string()?,
        })
    }

    // Get key
    pub fn key(&self) -> &str {
        &self.key
    }

    // Transfer command to frame
    pub fn into_frame(self) -> Frame {
        Frame::Array(vec![
            Frame::Simple("del".into()),
            Frame::Bulk(Bytes::from(self.key)),
        ])
    }
}
