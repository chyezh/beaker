use super::{Error, Frame, Result};

pub struct Parser {
    frame_iter: std::vec::IntoIter<Frame>,
}

// Iterator for parsing frame sequence, consume a Frame::Array item
impl Parser {
    // Create a new `Parser` to parse frame data
    // panic: frame data must be a Frame::Array
    pub fn new(data: std::vec::IntoIter<Frame>) -> Parser {
        Parser { frame_iter: data }
    }

    // Get next frame item, return end of stream error if iterating completed
    fn next(&mut self) -> Result<Frame> {
        self.frame_iter.next().ok_or(Error::Eos)
    }

    // Get next string type content.
    // return protocol error if next node is not string.
    pub fn next_string(&mut self) -> Result<String> {
        match self.next()? {
            Frame::Bulk(str) => Ok(String::from_utf8(str.to_vec()).map_err(|_| Error::Invalid)?),
            Frame::Simple(str) => Ok(str),
            _ => Err(Error::Invalid),
        }
    }

    // Get next integer type content.
    // return protocol error if next node is not integer.
    pub fn next_integer(&mut self) -> Result<usize> {
        match self.next()? {
            Frame::Integer(i) => Ok(i),
            _ => Err(Error::Invalid),
        }
    }

    // Check that parsing is finish
    pub fn check_finish(&mut self) -> Result<()> {
        if self.frame_iter.next().is_none() {
            Ok(())
        } else {
            Err(Error::Invalid)
        }
    }
}
