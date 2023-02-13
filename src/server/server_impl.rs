use tokio::io::BufWriter;
use tokio::net::{TcpListener, ToSocketAddrs};

use super::{handler::Handler, Result};
use crate::engine::DB;
use crate::resp::Connection;
use tracing::warn;

pub struct Server {
    db: DB,
}

impl Server {
    // Create a new Server with given engine
    pub fn new(db: DB) -> Self {
        Server { db }
    }

    // Run the server
    pub async fn run<T: ToSocketAddrs>(self, addr: T) -> Result<()> {
        let listener = TcpListener::bind(addr).await?;

        loop {
            // Accept a new stream and construct a new task to run concurrently
            let (stream, _) = listener.accept().await?;

            // Create new connection with write buffer
            let buffer_stream = BufWriter::new(stream);
            let conn = Connection::new(buffer_stream);

            // Create new handler and run asynchronously
            let db = self.db.clone();
            let mut handler = Handler::new(db, conn);
            tokio::spawn(async move {
                if let Err(err) = handler.run().await {
                    warn!(
                        err = err.to_string(),
                        "exception occurred on handling connection"
                    );
                }
            });
        }
    }
}
