use tokio::net::{TcpListener, ToSocketAddrs};

use super::{connection::Connection, handler::Handler, Result};
use crate::leveldb::DB;
use std::sync::Arc;

pub struct Server {
    db: DB,
}

impl Server {
    // Create a new Server with given engine
    pub fn new(db: DB) -> Self {
        Server { db }
    }

    // Run the server
    pub async fn run<A: ToSocketAddrs>(self, addr: A) -> Result<()> {
        let listener = TcpListener::bind(addr).await?;

        loop {
            // Accept a new stream and construct a new task to run concurrently
            let (stream, _) = listener.accept().await?;
            let conn = Connection::new(stream);
            let db = self.db.clone();
            let mut handler = Handler::new(db, conn);

            tokio::spawn(async move {
                handler.run().await.unwrap();
            });
        }
    }
}
