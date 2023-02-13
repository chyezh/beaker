use super::{Error, Frame, Parser, ResponseParser, Result};
use crate::resp::AsFrame;
use bytes::Bytes;

pub const PING_COMMAND_NAME: &str = "PING";
const PING_DEFAULT_RESPONSE: &str = "PONG";

#[derive(Debug, Eq, PartialEq, Default)]
pub struct Ping {
    msg: Option<Bytes>,
}

impl Ping {
    /// Create a new ping command
    #[inline]
    pub fn new(msg: Option<Bytes>) -> Self {
        Ping { msg }
    }

    /// Create a new ping command from parser
    #[inline]
    pub fn from_parser(parser: &mut Parser) -> Result<Self> {
        Ok(match parser.next_bytes() {
            Ok(msg) => Ping { msg: Some(msg) },
            Err(err) if err.is_eos() => Ping { msg: None },
            _ => Err(crate::resp::Error::Invalid)?,
        })
    }

    /// Generate response
    #[inline]
    pub fn response(self) -> Frame {
        match self.msg {
            Some(msg) => Frame::Bulk(msg),
            None => Frame::Simple(String::from(PING_DEFAULT_RESPONSE)),
        }
    }
}

impl AsFrame for Ping {
    /// Transfer command to resp
    fn as_frame(&self) -> Frame {
        let mut frame = Frame::Array(vec![PING_COMMAND_NAME.to_string().into()]);
        if let Some(msg) = &self.msg {
            frame.push_bulk(msg.clone());
        }
        frame
    }
}

impl ResponseParser for Ping {
    type Response = Bytes;

    /// Return response parsed from parser, otherwise error if response is not valid with request
    #[inline]
    fn parse_response(&self, parser: &mut Parser) -> Result<Self::Response> {
        let (response, expected_response): (_, &[u8]) = match &self.msg {
            Some(msg) => (parser.next_bytes()?, msg),
            None => (
                Bytes::from(parser.next_string()?),
                PING_DEFAULT_RESPONSE.as_bytes(),
            ),
        };
        parser.check_finish()?;

        if response != expected_response {
            return Err(Error::Any(
                "unexpected response from peer with ping command".to_string(),
            ));
        }
        Ok(response)
    }
}
