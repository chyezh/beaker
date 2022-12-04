use super::{Frame, Parser, Result};
use bytes::Bytes;

#[derive(Debug, PartialEq, Eq)]
pub struct Set {
    key: String,
    val: String,
}

impl Set {
    // Create a new set command with a key
    pub fn new(key: String, val: String) -> Self {
        Set { key, val }
    }

    // Create a new set command from parser
    pub fn from_parser(parser: &mut Parser) -> Result<Self> {
        Ok(Set {
            key: parser.next_string()?,
            val: parser.next_string()?,
        })
    }

    // Get key
    pub fn key(&self) -> String {
        self.key.clone()
    }

    // Get val
    pub fn val(&self) -> Bytes {
        Bytes::from(self.val.clone().into_bytes())
    }

    // transfer command to resp
    pub fn into_resp(self) -> Frame {
        Frame::Array(vec![
            Frame::Simple("set".into()),
            Frame::Bulk(Bytes::from(self.key)),
            Frame::Bulk(Bytes::from(self.val)),
        ])
    }
}
