use std::path::PathBuf;

use bytes::Bytes;

use super::{
    compact::Compactor,
    manifest::{CompactTask, Manifest},
    memtable::{DumpRequest, MemTable},
    sstable::{SSTable, SSTableBuilder},
    util::Value,
    Result,
};
use tokio::sync::mpsc::UnboundedReceiver;

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
        while let Some(()) = listener.recv().await {
            for task in manifest.find_new_compact_tasks() {
                Compactor::new(task, manifest.clone()).compact().ok();
            }
        }
    });
}

// Listen the dump channel, dump memtable into sstable concurrently
fn listen_dump(manifest: Manifest, mut listener: UnboundedReceiver<DumpRequest>) {
    tokio::spawn(async move {
        while let Some(request) = listener.recv().await {
            let manifest = manifest.clone();
            tokio::spawn(async move {
                dump(&manifest, request).ok();
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
    use crate::util::{generate_random_bytes, generate_step_by_bytes};

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

        println!("start get");
        for (key, value) in test_case_key.iter().zip(test_case_value.iter()) {
            assert_eq!(db.get(key).unwrap().unwrap(), value.clone());
        }
    }

    #[test]
    fn test_db_with_step_case() {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .expect("Failed building the tokio runtime");
        let _guard = runtime.enter();

        let test_count = 100000;
        let test_case_key = generate_step_by_bytes(test_count);
        let test_case_value = generate_step_by_bytes(test_count);

        let db = DB::open("./data").unwrap();
        // for (key, value) in test_case_key.iter().zip(test_case_value.iter()) {
        //     db.set(key.clone(), value.clone()).unwrap();
        // }

        println!("start get");
        for (key, value) in test_case_key.iter().zip(test_case_value.iter()) {
            assert_eq!(db.get(key).unwrap().unwrap(), value.clone());
        }
    }
}
