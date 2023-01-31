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
}

impl DB {
    // Open levelDB on given path
    pub fn open(path: impl Into<PathBuf>) -> Result<Self> {
        let path = path.into();
        let (manifest, rx) = Manifest::open(path.clone())?;
        let mut memtable = MemTable::open(path)?;

        // Start a background task
        // Transform memtable into level 0 sstable
        listen_dump(manifest.clone(), memtable.take_dump_listener().unwrap());
        listen_compact(manifest.clone(), rx);
        Ok(DB { manifest, memtable })
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

// Listen the compact channel
fn listen_compact(manifest: Manifest, mut listener: UnboundedReceiver<()>) {
    tokio::spawn(async move {
        info!("compact task listening...");
        while let Some(()) = listener.recv().await {
            info!("start find new compact task...");
            for task in manifest.find_new_compact_tasks() {
                let manifest = manifest.clone();
                info!("compact task found, {:?}", task);
                tokio::spawn(async move {
                    if let Err(err) = Compactor::new(task, manifest).compact() {
                        warn!(error = err.to_string(), "compact task failed")
                    }
                });
            }
            info!("finish find new compact task");
        }
    });
}

// Listen the dump channel, dump memtable into sstable concurrently
fn listen_dump(manifest: Manifest, mut listener: UnboundedReceiver<DumpRequest>) {
    tokio::spawn(async move {
        info!("dump task listening...");
        while let Some(request) = listener.recv().await {
            let manifest = manifest.clone();
            info!("dump request found");
            tokio::spawn(async move {
                if let Err(err) = dump(&manifest, request) {
                    warn!(error = err.to_string(), "dump memtable into sstable failed",)
                }
            });
        }
    });
}

// Dump a memtable into level 0 sstable
fn dump(manifest: &Manifest, request: DumpRequest) -> Result<()> {
    if request.len() != 0 {
        // Build a new sstable from memtable
        let mut entry = manifest.alloc_new_sstable_entry();
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
    request.ok();

    Ok(())
}

#[cfg(test)]
mod tests {

    use super::*;
    use crate::util::generate_random_bytes;
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
    fn test_db_with_step_case() {
        tracing::warn!("start testing");
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .expect("Failed building the tokio runtime");
        let _guard = runtime.enter();

        let test_count = 100000;
        let db = DB::open("./data").unwrap();

        for i in 0..test_count {
            let b = Bytes::from(i.to_string());
            assert_eq!(db.get(&b).unwrap(), Some(b));
            // db.set(b.clone(), b.clone()).unwrap();
        }
    }
}
