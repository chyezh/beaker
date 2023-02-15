use std::path::PathBuf;

use crate::{engine::Config, util::clap_validator::*};
use clap::{ArgGroup, Parser};

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
#[command(group(
    ArgGroup::new("database").required(true).args(["root_path", "config"])
))]
pub struct Args {
    /// Listening host or ip address.
    #[arg(short = 'H', long, value_parser = valid_hostname, default_value = "127.0.0.1")]
    host: String,

    /// Listening server port.
    #[arg(short, long, value_parser = port_in_range, default_value = "6379")]
    port: u16,

    /// Database root path.
    #[arg(long, value_parser = valid_path)]
    root_path: Option<PathBuf>,

    #[arg(short = 'C', long, value_parser = Config::parse_config_from_path)]
    config: Option<Config>,
}

impl Args {
    /// Get address from command-line arguments
    pub fn addr(&self) -> String {
        format!("{}:{}", self.host, self.port)
    }

    /// Get database root path,
    pub fn root_path(&self) -> PathBuf {
        if let Some(p) = self.root_path.as_ref() {
            p.clone()
        } else {
            self.config.as_ref().unwrap().root_path.clone()
        }
    }

    /// Get configuration of database
    pub fn config(&self) -> Config {
        if let Some(p) = self.root_path.as_ref() {
            Config::default_config_with_path(p)
        } else {
            self.config.as_ref().unwrap().clone()
        }
    }
}
