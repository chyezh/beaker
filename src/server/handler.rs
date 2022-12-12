use super::{connection::Connection, Result};

use crate::cmd::Command;
use crate::engine::Engine;
use crate::resp::Frame;

// Process a connection sequentially.
// 1. Transfer connection frame into command
// 2. Send command to DB engine
// 3. Transfer result of command into frame and write to connection.
pub struct Handler {
    db: Engine,
    conn: Connection,
}

impl Handler {
    // Create a new handler with given db engine and connection
    pub fn new(db: Engine, conn: Connection) -> Self {
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
            apply(&cmd, self.db.clone(), &mut self.conn).await?;
        }
    }
}

// Apply command to db engine, and write result of command into connection
async fn apply(cmd: &Command, db: Engine, conn: &mut Connection) -> Result<()> {
    conn.write_frame(&match cmd {
        Command::Get(get) => match db.get(get.key()).await? {
            Some(val) => Frame::Bulk(val),
            None => Frame::Null,
        },
        Command::Ping(_) => Frame::Simple("pong".into()),
        Command::Set(set) => {
            db.set(set.key(), set.val()).await?;
            Frame::Simple("OK".into())
        }
        Command::Del(del) => {
            db.del(del.key()).await?;
            Frame::Integer(1)
        }
    })
    .await?;
    Ok(())
}
