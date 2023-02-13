use super::{Error, Frame, Parser, ResponseParser, Result};
use crate::resp::AsFrame;
use bytes::Bytes;

pub const DEL_COMMAND_NAME: &str = "DEL";
const DEL_DEFAULT_RESPONSE: &str = "OK";

#[derive(Debug, PartialEq, Eq)]
pub struct Del {
    key: Bytes,
}

impl Del {
    /// Create a new del command with a key
    #[inline]
    pub fn new(key: Bytes) -> Self {
        Del { key }
    }

    /// Create a new del command from parser
    #[inline]
    pub fn from_parser(parser: &mut Parser) -> Result<Self> {
        Ok(Del {
            key: parser.next_bytes()?,
        })
    }

    /// Get reference of key
    #[inline]
    #[allow(dead_code)]
    pub fn raw_key(&self) -> &[u8] {
        &self.key
    }

    /// Get key
    #[inline]
    pub fn key(&self) -> Bytes {
        self.key.clone()
    }

    /// Create
    #[inline]
    pub fn response(self) -> Frame {
        Frame::Simple(DEL_DEFAULT_RESPONSE.to_string())
    }
}

impl AsFrame for Del {
    /// Transfer command to resp
    #[inline]
    fn as_frame(&self) -> Frame {
        Frame::Array(vec![
            Frame::Simple(DEL_COMMAND_NAME.into()),
            Frame::Bulk(self.key.clone()),
        ])
    }
}

impl ResponseParser for Del {
    type Response = ();

    /// Return response parsed from parser, otherwise error if response is not valid with request
    #[inline]
    fn parse_response(&self, parser: &mut Parser) -> Result<Self::Response> {
        let response = parser.next_string()?;
        parser.check_finish()?;

        // Check if response is valid
        if response == DEL_DEFAULT_RESPONSE {
            return Ok(());
        }
        Err(Error::Any(
            "unexpected response from peer with set command".to_string(),
        ))
    }
}
