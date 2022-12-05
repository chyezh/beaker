use std::{
    cell::RefCell,
    collections::BTreeMap,
    path::PathBuf,
    sync::{Arc, Mutex},
};

use bytes::Bytes;

use crate::cmd::Command;

use super::{Result, SplittedFileLogReader, SplittedFileLogWriter};

#[derive(Clone)]
pub struct Engine {
    // in-memory database
    entries: Arc<Mutex<RefCell<BTreeMap<String, Bytes>>>>,
    // log structured storage
    log_writer: Arc<Mutex<RefCell<SplittedFileLogWriter>>>,
}

impl Engine {
    pub fn open(path: impl Into<PathBuf>) -> Result<Self> {
        let path = path.into();

        // build in-memory db from structured log
        let reader = SplittedFileLogReader::open(&path)?;

        let mut entries = BTreeMap::new();
        for cmd in reader {
            match cmd? {
                Command::Set(set) => {
                    entries.insert(set.key(), set.val());
                }
                Command::Del(del) => {
                    entries.remove(del.key());
                }
                _ => (),
            }
        }

        Ok(Engine {
            entries: Arc::new(Mutex::new(RefCell::new(entries))),
            log_writer: Arc::new(Mutex::new(RefCell::new(
                SplittedFileLogWriter::open_unchecked(&path)?,
            ))),
        })
    }

    pub async fn get(&self, key: &str) -> Result<Option<Bytes>> {
        Ok(self.entries.lock().unwrap().borrow().get(key).cloned())
    }

    pub async fn set(&self, key: String, val: Bytes) -> Result<()> {
        // write operation into log
        self.write_cmd(&Command::set(
            key.clone(),
            String::from_utf8((&val[..]).to_vec()).unwrap(),
        ))?;

        // write operation into in-memory db
        self.entries.lock().unwrap().borrow_mut().insert(key, val);
        Ok(())
    }

    pub async fn del(&self, key: &str) -> Result<()> {
        // write operation into log
        self.write_cmd(&Command::del(String::from(key)))?;

        // write operation into in-memory db
        self.entries.lock().unwrap().borrow_mut().remove(key);
        Ok(())
    }

    pub fn write_cmd(&self, cmd: &Command) -> Result<()> {
        self.log_writer
            .lock()
            .unwrap()
            .borrow_mut()
            .write_cmd(cmd)?;
        Ok(())
    }
}
