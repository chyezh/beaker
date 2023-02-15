use super::event::{EventLoopBuilder, EventNotifier};
use super::sstable::SSTableManager;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use std::str::FromStr;
use std::time::Duration;

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct Config {
    // Serializable type
    pub root_path: PathBuf,

    // Timer for event in database
    #[serde(default)]
    pub timer: Timer,
}

impl Config {
    pub fn parse_config_from_path(s: &str) -> std::result::Result<Config, String> {
        let path = PathBuf::from_str(s)
            .map_err(|e| format!("`{}` isn't a valid path for config: {}", s, e))?;

        let content = std::fs::read_to_string(path)
            .map_err(|e| format!("read config at `{}` failed: {}", s, e))?;

        let config = toml::from_str(&content)
            .map_err(|e| format!("parse config at `{}` failed: {}", s, e))?;

        Ok(config)
    }

    pub fn default_config_with_path(root_path: impl Into<PathBuf>) -> Self {
        Config {
            root_path: root_path.into(),
            timer: Timer::default(),
        }
    }

    #[inline]
    pub fn manifest_path(&self) -> PathBuf {
        self.root_path.join("manifest")
    }

    #[inline]
    pub fn sstable_path(&self) -> PathBuf {
        self.root_path.join("tables")
    }

    #[inline]
    pub fn log_path(&self) -> PathBuf {
        self.root_path.join("log")
    }
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct Timer {
    #[serde(default = "default_enable_timer")]
    pub enable_timer: bool,
    #[serde(default = "default_inactive_reader_clean_period")]
    pub inactive_reader_clean_period: Duration,
    #[serde(default = "default_inactive_log_clean_period")]
    pub inactive_log_clean_period: Duration,
    #[serde(default = "default_inactive_sstable_clean_period")]
    pub inactive_sstable_clean_period: Duration,
    #[serde(default = "default_compact_period")]
    pub compact_period: Duration,
}

impl Default for Timer {
    fn default() -> Self {
        Timer {
            enable_timer: true,
            inactive_reader_clean_period: default_inactive_reader_clean_period(),
            inactive_log_clean_period: default_inactive_log_clean_period(),
            inactive_sstable_clean_period: default_inactive_sstable_clean_period(),
            compact_period: default_compact_period(),
        }
    }
}

#[derive(Clone)]
pub struct Initial {
    pub event_sender: EventNotifier,
    pub sstable_manager: SSTableManager<tokio::fs::File>,
}

impl Initial {
    pub fn initial() -> (Self, EventLoopBuilder) {
        let manager = SSTableManager::new();
        let (event_sender, event_builder) = EventLoopBuilder::new();
        (
            Initial {
                event_sender,
                sstable_manager: manager,
            },
            event_builder,
        )
    }
}

#[inline]
fn default_enable_timer() -> bool {
    true
}

#[inline]
fn default_inactive_reader_clean_period() -> Duration {
    Duration::from_secs(60)
}

#[inline]
fn default_inactive_log_clean_period() -> Duration {
    Duration::from_secs(60)
}

#[inline]
fn default_inactive_sstable_clean_period() -> Duration {
    Duration::from_secs(60)
}

#[inline]
fn default_compact_period() -> Duration {
    Duration::from_secs(60)
}
