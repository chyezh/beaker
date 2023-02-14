use super::argument::Args;
use super::{command::parse, Result};
use crate::cmd::Command;
use crate::{resp::Connector, Client};
use bytes::Bytes;
use clap::Parser;
use is_terminal::IsTerminal;
use std::io::{self, Write};
use termion::color;
use tokio::io::{AsyncRead, AsyncWrite};

pub struct Cli {
    args: Args,
    is_terminal: bool,
}

impl Default for Cli {
    fn default() -> Self {
        let args = Args::parse();

        Cli {
            args,
            is_terminal: std::io::stdout().is_terminal(),
        }
    }
}

impl Cli {
    /// Run this cli tool
    pub async fn run(&self) -> Result<()> {
        if self.is_terminal {
            self.run_in_terminal().await
        } else {
            self.run_not_in_terminal().await
        }
    }

    /// Run in terminal
    async fn run_in_terminal(&self) -> Result<()> {
        let client = Client::connect(self.args.addr()).await?;
        let mut input = String::new();

        loop {
            print!("{}> ", color::Fg(color::White));
            io::stdout().flush().unwrap();
            input.clear();
            if io::stdin().read_line(&mut input)? == 0 {
                break;
            }
            match parse(&input) {
                Ok(cmd) => match self.apply_cmd(&client, cmd).await {
                    Ok(msg) => println!("{}{}", color::Fg(color::White), msg),
                    Err(e) => println!("{}{:?}", color::Fg(color::Red), e),
                },
                Err(e) => {
                    println!("{}{:?}", color::Fg(color::Red), e);
                }
            }
            io::stdout().flush().unwrap();
        }

        Ok(())
    }

    /// Run not in terminal
    async fn run_not_in_terminal(&self) -> Result<()> {
        let client = Client::connect(self.args.addr()).await?;
        let mut input = String::new();

        loop {
            input.clear();
            if io::stdin().read_line(&mut input)? == 0 {
                break;
            }
            match parse(&input) {
                Ok(cmd) => match self.apply_cmd(&client, cmd).await {
                    Ok(msg) => println!("{}", msg),
                    Err(e) => println!("{:?}", e),
                },
                Err(e) => {
                    println!("{:?}", e);
                }
            }
        }
        io::stdout().flush().unwrap();

        Ok(())
    }

    async fn apply_cmd<S: AsyncRead + AsyncWrite + Unpin, T: Connector<Stream = S>>(
        &self,
        client: &Client<T>,
        cmd: Command,
    ) -> Result<String> {
        Ok(String::from_utf8(
            match cmd {
                Command::Get(get) => client
                    .get(get.key())
                    .await?
                    .unwrap_or_else(|| Bytes::from("(nil)")),
                Command::Ping(ping) => client.ping(ping.msg()).await?,
                Command::Set(set) => {
                    client.set(set.key(), set.val()).await?;
                    Bytes::from("1")
                }
                Command::Del(del) => {
                    client.del(del.key()).await?;
                    Bytes::from("1")
                }
            }
            .to_vec(),
        )?)
    }
}
