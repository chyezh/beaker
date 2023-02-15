use super::argument::Args;
use super::{handler::Handler, Result};
use crate::engine::DB;
use crate::resp::Connection;
use crate::util::shutdown::Notifier;
use clap::Parser;
use tokio::io::BufWriter;
use tokio::net::{TcpListener, TcpStream};
use tokio::signal;
use tracing::{info, warn};

pub struct Server {
    args: Args,
    db: DB,
    shutdown: Notifier,
}

impl Default for Server {
    fn default() -> Self {
        let args = Args::parse();
        // Open database
        let db = DB::open(args.config()).unwrap();
        let shutdown = Notifier::new();

        Server { args, db, shutdown }
    }
}

impl Server {
    // Run the server
    pub async fn run(mut self) -> Result<()> {
        let addr = self.args.addr();
        info!("bind tcp listener on address: {}...", addr);
        let listener = TcpListener::bind(addr).await?;

        info!("start listening incoming connection...");
        loop {
            // Accept a new stream and construct a new task to run concurrently
            tokio::select! {
                _ = signal::ctrl_c() => {
                    break
                }
                Ok((stream, _)) = listener.accept() => {
                    self.handle_new_connection(stream);
                }
            }
        }

        info!("stop listening incoming connection");
        info!("waiting for existed task stop handling...");
        self.shutdown.notify().await;

        info!("waiting for database engine to shutdown...");
        self.db.shutdown().await;

        info!("server shutdown");
        Ok(())
    }

    /// Handling for new incoming connection.
    /// Create a new async task and run it in tokio.
    pub fn handle_new_connection(&self, stream: TcpStream) {
        // Create new connection with write buffer
        let buffer_stream = BufWriter::new(stream);
        let conn = Connection::new(buffer_stream);

        // Create new handler and run asynchronously
        let shutdown = self.shutdown.listen().unwrap();
        let db = self.db.clone();

        let mut handler = Handler::new(db, conn, shutdown);

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
