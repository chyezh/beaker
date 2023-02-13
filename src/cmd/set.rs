use crate::resp::AsFrame;

use super::{Error, Frame, Parser, ResponseParser, Result};
use bytes::Bytes;

pub const SET_COMMAND_NAME: &str = "SET";
const SET_DEFAULT_RESPONSE: &str = "OK";

#[derive(Debug, PartialEq, Eq)]
pub struct Set {
    key: Bytes,
    val: Bytes,
}

impl Set {
    /// Create a new set command with a key
    #[inline]
    pub fn new(key: Bytes, val: Bytes) -> Self {
        Set { key, val }
    }

    /// Create a new set command from parser
    #[inline]
    pub fn from_parser(parser: &mut Parser) -> Result<Self> {
        Ok(Set {
            key: parser.next_bytes()?,
            val: parser.next_bytes()?,
        })
    }

    /// Get reference of key
    #[inline]
    #[allow(dead_code)]
    pub fn raw_key(&self) -> &[u8] {
        &self.key
    }

    /// Get reference of val
    #[inline]
    #[allow(dead_code)]
    pub fn raw_val(&self) -> &[u8] {
        &self.val
    }

    /// Get owned key
    #[inline]
    pub fn key(&self) -> Bytes {
        self.key.clone()
    }

    /// Get owned val
    #[inline]
    pub fn val(&self) -> Bytes {
        self.val.clone()
    }

    /// Generate response
    #[inline]
    pub fn response(self) -> Frame {
        Frame::Simple(SET_DEFAULT_RESPONSE.to_string())
    }
}

impl AsFrame for Set {
    /// Transfer command to resp
    #[inline]
    fn as_frame(&self) -> Frame {
        Frame::Array(vec![
            Frame::Simple(SET_COMMAND_NAME.into()),
            Frame::Bulk(self.key.clone()),
            Frame::Bulk(self.val.clone()),
        ])
    }
}

impl ResponseParser for Set {
    type Response = ();

    /// Return response parsed from parser, otherwise error if response is not valid with request
    #[inline]
    fn parse_response(&self, parser: &mut Parser) -> Result<Self::Response> {
        let response = parser.next_string()?;
        parser.check_finish()?;

        // Check if response is valid
        if response == SET_DEFAULT_RESPONSE {
            return Ok(());
        }
        Err(Error::Any(
            "unexpected response from peer with set command".to_string(),
        ))
    }
}
