use clap::Parser;
use tokio::io::BufWriter;
use tokio::net::TcpListener;

use super::argument::Args;
use super::{handler::Handler, Result};
use crate::engine::DB;
use crate::resp::Connection;
use tracing::warn;

pub struct Server {
    args: Args,
    db: DB,
}

impl Default for Server {
    fn default() -> Self {
        let args = Args::parse();
        // Open database
        let db = DB::open(args.root_path()).unwrap();

        Server { args, db }
    }
}

impl Server {
    // Run the server
    pub async fn run(self) -> Result<()> {
        let listener = TcpListener::bind(self.args.addr()).await?;

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
