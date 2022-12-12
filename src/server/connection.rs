use std::io::Cursor;

use bytes::{Buf, BytesMut};
use tokio::{io::AsyncReadExt, net::TcpStream};

use super::{Error, Result};
use crate::resp::Frame;

pub struct Connection {
    stream: TcpStream,
    buffer: BytesMut,
}

impl Connection {
    // Create a new Connection
    pub fn new(stream: TcpStream) -> Connection {
        Connection {
            stream,
            buffer: BytesMut::with_capacity(4096),
        }
    }

    // Read new frame from stream
    pub async fn read_frame(&mut self) -> Result<Option<Frame>> {
        loop {
            // Try to parse new frame from buffer
            // Unexpected frame will deliver to the caller, then close connection and release resource by caller.
            if let Some(frame) = self.parse_frame()? {
                return Ok(Some(frame));
            }

            // If the frame in buffer is incomplete, fetch more data to buffer
            if self.stream.read_buf(&mut self.buffer).await? == 0 {
                // If read_buf read 0 bytes from stream, check if buffer is empty.
                // If buffer is not empty, connection is reset by peer with a incomplete frame in buffer.
                if self.buffer.is_empty() {
                    return Ok(None);
                } else {
                    return Err(Error::ConnReset);
                }
            }
        }
    }

    pub async fn write_frame(&mut self, frame: &Frame) -> Result<()> {
        // Write frame to stream directly
        frame.write_to(&mut self.stream).await.map_err(|e| e.into())
    }

    fn parse_frame(&mut self) -> Result<Option<Frame>> {
        let mut cursor = Cursor::new(&self.buffer[..]);

        match Frame::check(&mut cursor) {
            // If frame is complete, parse a frame and advance on buffer
            Ok(_) => {
                cursor.set_position(0);
                let frame = Frame::parse(&mut cursor)?;
                let advanced_len = cursor.position() as usize;
                self.buffer.advance(advanced_len);
                Ok(Some(frame))
            }
            // Frame is not complete, continue to fetch data from stream
            Err(e) if matches!(e, crate::resp::Error::Incomplete) => Ok(None),
            // Unexpected error, close the connection
            Err(e) => Err(e.into()),
        }
    }
}
