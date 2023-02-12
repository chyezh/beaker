use super::Result;
use crate::cmd::Command;
use crate::engine::DB;
use crate::resp::{Connection, Frame};
use tokio::io::{AsyncRead, AsyncWrite};

// Process a connection sequentially.
// 1. Transfer connection frame into command
// 2. Send command to DB engine
// 3. Transfer result of command into frame and write to connection.
pub struct Handler<T> {
    db: DB,
    conn: Connection<T>,
}

impl<T: AsyncRead + AsyncWrite + Unpin> Handler<T> {
    // Create a new handler with given db engine and connection
    pub fn new(db: DB, conn: Connection<T>) -> Self {
        Handler { db, conn }
    }

    // Run this Handler
    pub async fn run(&mut self) -> Result<()> {
        loop {
            // Find a new frame
            let frame = match self.conn.read_frame().await? {
                Some(Frame::Array(f)) => f,
                Some(_) => return Err(crate::resp::Error::Invalid.into()),
                None => return Ok(()),
            };

            // Parse command and apply command
            let cmd = Command::from_frame(frame.into_iter())?;

            match apply(cmd, self.db.clone()).await {
                Ok(response) => {
                    self.conn.write_frame(&response).await?;
                    self.conn.flush().await?;
                }
                Err(err) => {
                    // Send error if error happened in apply function
                    self.conn
                        .write_frame(&Frame::Error(err.to_string()))
                        .await?;
                }
            }
        }
    }
}

// Apply command to db engine, and write result of command into connection
async fn apply(cmd: Command, db: DB) -> Result<Frame> {
    Ok(match cmd {
        Command::Get(get) => {
            let val = db.get(get.raw_key()).await?;
            get.response(val)
        }
        Command::Ping(ping) => ping.response(),
        Command::Set(set) => {
            db.set(set.key(), set.val())?;
            set.response()
        }
        Command::Del(del) => {
            db.del(del.key())?;
            del.response()
        }
    })
}
