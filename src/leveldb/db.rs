use crate::util::shutdown::{Listener, Notifier};

use super::{
    compact::Compactor,
    manifest::Manifest,
    memtable::{DumpRequest, MemTable},
    sstable::{SSTable, SSTableBuilder},
    util::Value,
    Result,
};
use bytes::Bytes;
use std::path::PathBuf;
use tokio::sync::mpsc::UnboundedReceiver;
use tracing::{info, warn};

pub struct DB {
    manifest: Manifest,
    memtable: MemTable,
    shutdown: Option<Notifier>,
    rt: tokio::runtime::Runtime,
}

impl DB {
    // Open levelDB on given path
    pub fn open(path: impl Into<PathBuf>) -> Result<Self> {
        let path = path.into();
        let shutdown = Notifier::new();
        let rt = tokio::runtime::Runtime::new().unwrap();
        let _guard = rt.enter();

        let (manifest, compact_rx) = Manifest::open(path.clone())?;
        let (memtable, dump_rx) = MemTable::open(path)?;

        // Start a background task
        // Transform memtable into level 0 sstable
        listen_dump(
            manifest.clone(),
            memtable.clone(),
            dump_rx,
            shutdown.listen().unwrap(),
        );
        listen_compact(manifest.clone(), compact_rx, shutdown.listen().unwrap());
        Ok(DB {
            manifest,
            memtable,
            shutdown: Some(shutdown),
            rt,
        })
    }

    // Get value from db by key
    pub fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        // Get memtable first
        if let Some(value) = self.memtable.get(key) {
            match value {
                Value::Living(v) => return Ok(Some(v)),
                Value::Tombstone => return Ok(None),
            }
        }

        // Get possible sstables from manifest
        // Read sstable to search value
        for sstable in self.manifest.search(key) {
            let sstable = SSTable::open(sstable.open_reader()?)?;
            if let Some(value) = sstable.search(key)? {
                match value {
                    Value::Living(v) => return Ok(Some(v)),
                    Value::Tombstone => return Ok(None),
                }
            }
        }

        // Not found
        Ok(None)
    }

    // Set key value pair into db
    pub fn set(&self, key: Bytes, value: Bytes) -> Result<()> {
        self.memtable.set(key, Value::Living(value))
    }

    // Delete value by key
    pub fn del(&self, key: Bytes) -> Result<()> {
        self.memtable.set(key, Value::Tombstone)
    }
}

impl Drop for DB {
    fn drop(&mut self) {
        let mut shutdown = self.shutdown.take().unwrap();
        self.rt.block_on(async move {
            shutdown.notify().await;
        });
    }
}

// Listen the compact channel
fn listen_compact(
    manifest: Manifest,
    mut listener: UnboundedReceiver<()>,
    mut shutdown_listener: Listener,
) {
    tokio::spawn(async move {
        info!("compact task listening...");
        loop {
            tokio::select! {
                _ = shutdown_listener.listen() => {
                    // Stop creating new compact task if shutdown signal was notified
                    break;
                }
                _ = listener.recv() => {
                    info!("start find new compact task...");
                    for task in manifest.find_new_compact_tasks() {
                        let manifest = manifest.clone();
                        info!("compact task found, {:?}", task);

                        // Hold a listener copy, to block shutdown util compact task finish
                        let new_shutdown_listener = shutdown_listener.clone();
                        tokio::spawn(async move {
                            new_shutdown_listener.hold();
                            if let Err(err) = Compactor::new(task, manifest).compact() {
                                warn!(error = err.to_string(), "compact task failed")
                            }
                        });
                    }
                    info!("finish find new compact task");
                }
            }
        }
        info!("stop creating new compact task");
    });
}

// Listen the dump channel, dump memtable into sstable concurrently
fn listen_dump(
    manifest: Manifest,
    memtable: MemTable,
    mut listener: UnboundedReceiver<DumpRequest>,
    mut shutdown_listener: Listener,
) {
    tokio::spawn(async move {
        info!("dump task listening...");
        loop {
            tokio::select! {
                _ = shutdown_listener.listen() => {
                    // Stop dump operation if shutdown signal was notified
                    break;
                }
                Some(request) = listener.recv() => {
                    info!("start a new dump operation");
                    if let Err(err) = dump(&manifest, &memtable, request) {
                        warn!(error = err.to_string(), "dump memtable into sstable failed");
                    }
                }
            }
        }
        info!("stop dump task")
    });
}

// Dump a memtable into level 0 sstable
fn dump(manifest: &Manifest, memtable: &MemTable, request: DumpRequest) -> Result<()> {
    // Build a new sstable from memtable if memtable is not empty
    if request.len() != 0 {
        let mut entry = manifest.alloc_new_sstable_entry(0);
        let mut builder = SSTableBuilder::new(entry.open_writer()?);
        for (key, value) in request.iter() {
            builder.add(key.clone(), value.clone())?;
        }

        let range = builder.finish()?;
        entry.set_range(range);

        // Register the new sstable into manifest
        manifest.add_new_sstable(entry)?;
    }

    // Ask for clean memtable that is already dumped
    memtable.clean_immutable(request.path());

    Ok(())
}

#[cfg(test)]
mod tests {

    use super::*;
    use crate::util::generate_random_bytes;
    use rand::{self, Rng};
    use test_log::test;

    #[test]
    fn test_db_with_random_case() {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .expect("Failed building the tokio runtime");
        let _guard = runtime.enter();

        let test_count = 1000;
        let test_case_key = generate_random_bytes(test_count, 10000);
        let test_case_value = generate_random_bytes(test_count, 10 * 32 * 1024);

        let db = DB::open("./data").unwrap();
        for (key, value) in test_case_key.iter().zip(test_case_value.iter()) {
            db.set(key.clone(), value.clone()).unwrap();
        }

        for (key, value) in test_case_key.iter().zip(test_case_value.iter()) {
            assert_eq!(db.get(key).unwrap().unwrap(), value.clone());
        }
    }

    #[test]
    fn test_get_db_with_sequence_number() {
        let test_count = 1000000;
        let db = DB::open("./data").unwrap();
        // Get db
        for (k, v) in sequence_number_iter(test_count).zip(reverse_sequence_number_iter(test_count))
        {
            assert_eq!(db.get(&k).unwrap(), Some(v.clone()));
        }

        // Reopen db and test get
        drop(db);
        let db = DB::open("./data").unwrap();
        for (k, v) in sequence_number_iter(test_count).zip(reverse_sequence_number_iter(test_count))
        {
            assert_eq!(db.get(&k).unwrap(), Some(v.clone()));
        }
    }

    #[test]
    fn test_db_with_sequence_number() {
        tracing::warn!("start testing");

        let mut rng = rand::thread_rng();
        let test_count = 1000000;
        let db = DB::open("./data").unwrap();

        // Set db
        for i in sequence_number_iter(test_count) {
            db.set(i.clone(), i.clone()).unwrap();
        }

        // Get db
        for i in sequence_number_iter(test_count) {
            if 0 == rng.gen_range(0..100) {
                assert_eq!(db.get(&i).unwrap(), Some(i.clone()));
            }
        }

        // Reopen db and test get
        drop(db);
        let db = DB::open("./data").unwrap();
        for i in sequence_number_iter(test_count) {
            if 0 == rng.gen_range(0..100) {
                assert_eq!(db.get(&i).unwrap(), Some(i.clone()));
            }
        }

        // Set db
        for (k, v) in sequence_number_iter(test_count).zip(reverse_sequence_number_iter(test_count))
        {
            db.set(k, v).unwrap();
        }

        // Get db
        for (k, v) in sequence_number_iter(test_count).zip(reverse_sequence_number_iter(test_count))
        {
            if 0 == rng.gen_range(0..100) {
                assert_eq!(db.get(&k).unwrap(), Some(v.clone()));
            }
        }

        // Reopen db and test get
        drop(db);
        let db = DB::open("./data").unwrap();
        for (k, v) in sequence_number_iter(test_count).zip(reverse_sequence_number_iter(test_count))
        {
            if 0 == rng.gen_range(0..100) {
                assert_eq!(db.get(&k).unwrap(), Some(v.clone()));
            }
        }
    }

    fn sequence_number_iter(max: usize) -> impl Iterator<Item = Bytes> {
        (0..max).map(|i| Bytes::from(i.to_string()))
    }

    fn reverse_sequence_number_iter(max: usize) -> impl Iterator<Item = Bytes> {
        (0..max).map(|i| Bytes::from(i.to_string())).rev()
    }
}
