use bytes::Bytes;

use super::{Error, Frame, Result};

/// A RESP frame parser
/// An iterating helper for parsing frame sequence, consume a Frame::Array item
pub struct Parser {
    frame_iter: Box<dyn Iterator<Item = Frame>>,
}

impl Parser {
    /// Create a new `Parser` to parse frame data
    /// # Panics
    ///
    /// Frame data must be a Frame::Array
    pub fn new<I: Iterator<Item = Frame> + 'static>(iter: I) -> Self {
        Parser {
            frame_iter: Box::new(iter),
        }
    }

    /// Get next string type content.
    /// Return protocol error if next node is not string or not utf-8 representable bytes.
    pub fn next_string(&mut self) -> Result<String> {
        match self.next()? {
            Frame::Bulk(str) => Ok(String::from_utf8(str.to_vec()).map_err(|_| Error::Invalid)?),
            Frame::Simple(str) => Ok(str),
            _ => Err(Error::Invalid),
        }
    }

    /// Get next bytes contents if Frame is not NULL,
    /// otherwise get None.
    /// Return error if next node is not bulk or NULL.
    pub fn next_optional_bytes(&mut self) -> Result<Option<Bytes>> {
        match self.next()? {
            Frame::Bulk(b) => Ok(Some(b)),
            Frame::Null => Ok(None),
            _ => Err(Error::Invalid),
        }
    }

    /// Get next bytes content.
    /// Return protocol error if next node is not bulk.
    pub fn next_bytes(&mut self) -> Result<Bytes> {
        match self.next()? {
            Frame::Bulk(b) => Ok(b),
            _ => Err(Error::Invalid),
        }
    }

    /// Get next integer type content.
    /// Return protocol error if next node is not integer.
    pub fn next_integer(&mut self) -> Result<usize> {
        match self.next()? {
            Frame::Integer(i) => Ok(i),
            _ => Err(Error::Invalid),
        }
    }

    /// Check if parsing is finish
    pub fn check_finish(&mut self) -> Result<()> {
        if self.frame_iter.next().is_none() {
            Ok(())
        } else {
            Err(Error::Invalid)
        }
    }

    /// Get next frame item, return end of stream error if iterating completed
    fn next(&mut self) -> Result<Frame> {
        self.frame_iter.next().ok_or(Error::Eos)
    }
}
