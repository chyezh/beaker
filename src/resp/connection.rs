use super::{Error, Result};
use crate::resp::Frame;
use bytes::{Buf, BytesMut};
use parking_lot::Mutex;
use std::io::Cursor;
use std::ops::{Deref, DerefMut};
use std::sync::Arc;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tracing::debug;

type Conns<T> = Arc<Mutex<Vec<Connection<T>>>>;

// Implement a connection to handle stream as frame
pub struct Connection<T> {
    stream: T,
    buffer: BytesMut,
}

impl<T: AsyncRead + AsyncWrite + Unpin> Connection<T> {
    // Create a new Connection
    pub fn new(stream: T) -> Self {
        Connection {
            stream,
            buffer: BytesMut::with_capacity(4096),
        }
    }

    /// Read new frame from stream.
    /// Fill buffer by reading bytes from stream util parse a complete frame.
    ///
    /// Return Ok(None) if connection is closed by remote peer in normal case,
    /// return Err(Error::ConnReset) if connection is closed by remote peer with exception.
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

    /// Write a frame into stream
    pub async fn write_frame(&mut self, frame: &Frame) -> Result<()> {
        frame.write_to(&mut self.stream).await
    }

    /// Flush inner buffer if needed
    pub async fn flush(&mut self) -> Result<()> {
        self.stream.flush().await?;
        Ok(())
    }

    /// Try to parse a frame from stream buffer
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
            Err(e) => Err(e),
        }
    }
}

/// An interface for creating a connection
#[async_trait::async_trait]
pub trait Connector {
    type Stream;

    // Create a new connection
    async fn connect(&self) -> Result<Connection<Self::Stream>>;
}

/// An implementation of connection pool
pub struct ConnectionPool<T: Connector> {
    conns: Conns<T::Stream>,
    connector: T,
}

impl<T: Connector> ConnectionPool<T> {
    /// Create a new connection pool with given connector
    pub fn new(connector: T) -> Self {
        ConnectionPool {
            conns: Arc::new(Mutex::new(Vec::new())),
            connector,
        }
    }

    /// Get a idle connection from pool if pool is not empty,
    /// otherwise create a new connection by connector.
    pub async fn connect(&self) -> Result<ConnectionGuard<T::Stream>> {
        // From pool
        if let Some(conn) = self.conns.lock().pop() {
            return Ok(ConnectionGuard::new(conn, Arc::clone(&self.conns)));
        }
        // Create a new connection
        let conn = self.connector.connect().await?;
        Ok(ConnectionGuard::new(conn, Arc::clone(&self.conns)))
    }
}

/// A RAII-implement connection guard.
/// Release inner connection back to connection pool if ConnectionGuard was dropped.
pub struct ConnectionGuard<T> {
    valid: bool,
    conn: Option<Connection<T>>,
    conns: Arc<Mutex<Vec<Connection<T>>>>,
}

impl<T> ConnectionGuard<T> {
    fn new(conn: Connection<T>, pool: Conns<T>) -> Self {
        ConnectionGuard {
            valid: false,
            conn: Some(conn),
            conns: pool,
        }
    }

    /// Set guard into valid status
    #[inline]
    pub fn ok(&mut self) {
        self.valid = true
    }
}

impl<T> Drop for ConnectionGuard<T> {
    // Release the connection and push into pool waiting for reuse if it's still valid.
    fn drop(&mut self) {
        if self.valid {
            self.conns.lock().push(self.conn.take().unwrap());
        }
    }
}

impl<T> Deref for ConnectionGuard<T> {
    type Target = Connection<T>;

    // ConnectionGuard acts like connection
    fn deref(&self) -> &Self::Target {
        self.conn.as_ref().unwrap()
    }
}

impl<T> DerefMut for ConnectionGuard<T> {
    // ConnectionGuard acts like connection
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.conn.as_mut().unwrap()
    }
}
