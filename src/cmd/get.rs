use super::{Frame, Parser, ResponseParser, Result};
use crate::resp::AsFrame;
use bytes::Bytes;

pub const GET_COMMAND_NAME: &str = "GET";

#[derive(Debug, Eq, PartialEq)]
pub struct Get {
    key: Bytes,
}

impl Get {
    // Create a new get command with a key
    #[inline]
    pub fn new(key: Bytes) -> Self {
        Get { key }
    }

    /// Create a new get command from parser
    #[inline]
    pub fn from_parser(parser: &mut Parser) -> Result<Self> {
        Ok(Get {
            key: parser.next_bytes()?,
        })
    }

    /// Get reference of key
    #[inline]
    pub fn raw_key(&self) -> &[u8] {
        &self.key
    }

    /// Get owned key
    #[inline]
    #[allow(dead_code)]
    pub fn key(&self) -> Bytes {
        self.key.clone()
    }

    /// Generate response
    #[inline]
    pub fn response(self, val: Option<Bytes>) -> Frame {
        match val {
            Some(val) => Frame::Bulk(val),
            None => Frame::Null,
        }
    }
}

impl AsFrame for Get {
    /// Transfer command to resp
    #[inline]
    fn as_frame(&self) -> Frame {
        Frame::Array(vec![
            Frame::Simple(GET_COMMAND_NAME.into()),
            Frame::Bulk(self.key.clone()),
        ])
    }
}

impl ResponseParser for Get {
    type Response = Option<Bytes>;

    /// Return response parsed from parser, otherwise error if response is not valid with request
    #[inline]
    fn parse_response(&self, parser: &mut Parser) -> Result<Self::Response> {
        let response = parser.next_optional_bytes()?;
        parser.check_finish()?;
        Ok(response)
    }
}
