use super::Result;
use crate::client::Error;
use crate::cmd::{Del, Get, Ping, ResponseParser, Set};
use crate::resp::{AsFrame, Connection, ConnectionPool, Connector, Frame, Parser};
use bytes::Bytes;
use tokio::io::{AsyncRead, AsyncWrite, BufWriter};
use tokio::net::{TcpStream, ToSocketAddrs};
use tracing::debug;

pub struct TcpStreamConnector<T> {
    addr: T,
}

impl<T> TcpStreamConnector<T> {
    fn new(addr: T) -> Self {
        TcpStreamConnector { addr }
    }
}

#[tonic::async_trait]
impl<T: ToSocketAddrs + Send + Sync> Connector for TcpStreamConnector<T> {
    type Stream = BufWriter<TcpStream>;

    async fn connect(&self) -> crate::resp::Result<Connection<Self::Stream>> {
        let stream = TcpStream::connect(&self.addr).await?;
        let stream = BufWriter::new(stream);
        Ok(Connection::new(stream))
    }
}

pub struct Client<T: Connector> {
    pool: ConnectionPool<T>,
}

impl<T: ToSocketAddrs + Send + Sync> Client<TcpStreamConnector<T>> {
    // Connect to remote database server, get client for operation of databse
    pub async fn connect(addr: T) -> Result<Self> {
        // Create new pool and test connect
        let connector = TcpStreamConnector::new(addr);
        let pool = ConnectionPool::new(connector);

        // Try to connect
        pool.connect().await?;
        Ok(Client { pool })
    }
}

impl<S: AsyncRead + AsyncWrite + Unpin, T: Connector<Stream = S>> Client<T> {
    /// Send PING to server.
    /// Receive PONG if no msg is provided,
    /// otherwise receive a copy of msg like echo server.
    /// # Examples
    /// TODO
    #[inline]
    pub async fn ping(&self, msg: Option<Bytes>) -> Result<Bytes> {
        self.apply_cmd(Ping::new(msg)).await
    }

    /// Send GET command with given key to server.
    /// Receive value if key exists, otherwise none.
    /// # Examples
    /// TODO
    #[inline]
    pub async fn get(&self, key: Bytes) -> Result<Option<Bytes>> {
        self.apply_cmd(Get::new(key)).await
    }

    /// Send SET command with given key and value.
    /// Receive success if set operation success, otherwise error.
    /// # Examples
    /// TODO
    #[inline]
    pub async fn set(&self, key: Bytes, val: Bytes) -> Result<()> {
        self.apply_cmd(Set::new(key, val)).await
    }

    /// Send DEL command with given key.
    /// Receive success if  operation success, otherwise error
    /// # Examples
    /// TODO
    #[inline]
    pub async fn del(&self, key: Bytes) -> Result<()> {
        self.apply_cmd(Del::new(key)).await
    }

    /// Apply a command to client, and parse response
    async fn apply_cmd<C: AsFrame + ResponseParser>(&self, cmd: C) -> Result<C::Response> {
        let mut conn = self.pool.connect().await?;

        let frame = cmd.as_frame();
        debug!(cmd = ?frame);

        // Write frame into connection and flush
        conn.write_frame(&frame).await?;
        conn.flush().await?;

        // parse response from connection
        let mut parser = self.read_response(&mut conn).await?;
        let response = cmd.parse_response(&mut parser)?;

        // Connection work correctly
        conn.ok();
        Ok(response)
    }

    /// Read response from peer
    async fn read_response(&self, conn: &mut Connection<S>) -> Result<Parser> {
        let response = conn.read_frame().await?;
        debug!(?response);

        match response {
            Some(Frame::Array(arr)) => Ok(Parser::new(arr.into_iter())),
            Some(Frame::Error(msg)) => Err(Error::Any(msg)),
            Some(frame) => Ok(Parser::new(Some(frame).into_iter())),
            None => Err(Error::Any("connection reset by peer".to_string())),
        }
    }
}
