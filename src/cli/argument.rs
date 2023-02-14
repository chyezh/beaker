use crate::util::clap_validator::*;
use clap::Parser;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
pub struct Args {
    /// Remote server host or ip address.
    #[arg(short = 'H', long, value_parser = valid_hostname, default_value = "127.0.0.1")]
    host: String,

    /// Remote server port.
    #[arg(short, long, value_parser = port_in_range, default_value = "6379")]
    port: u16,
}

impl Args {
    /// Get remote address from command-line arguments
    pub fn addr(&self) -> String {
        format!("{}:{}", self.host, self.port)
    }
}
