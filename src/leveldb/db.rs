use std::path::PathBuf;

use bytes::Bytes;

use super::{
    manifest::Manifest,
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
    fn open(path: impl Into<PathBuf>) -> Result<Self> {
        let path = path.into();
        let manifest = Manifest::open(path.clone())?;
        let mut memtable = MemTable::open(path)?;

        // Start a background task
        // Transform memtable into level 0 sstable
        listen_dump(manifest.clone(), memtable.take_dump_listener().unwrap());
        Ok(DB { manifest, memtable })
    }

    // Get value from db by key
    pub async fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
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
    pub async fn set(&self, key: Bytes, value: Bytes) -> Result<()> {
        self.memtable.set(key, Value::Living(value))
    }

    // Delete value by key
    pub async fn del(&self, key: Bytes) -> Result<()> {
        self.memtable.set(key, Value::Tombstone)
    }
}

fn listen_dump(manifest: Manifest, mut listener: UnboundedReceiver<DumpRequest>) {
    tokio::spawn(async move {
        while let Some(request) = listener.recv().await {
            dump(&manifest, request).ok();
        }
    });
}

fn dump(manifest: &Manifest, request: DumpRequest) -> Result<()> {
    // Build a new sstable from memtable
    let entry = manifest.alloc_new_sstable_entry();
    let mut builder = SSTableBuilder::new(entry.open_writer()?);
    for (key, value) in request.iter() {
        builder.add(key, value.clone())?;
    }
    builder.finish()?;

    // Register the new sstable into manifest
    manifest.add_new_sstable(entry)?;

    // Ask for clean memtable that is already dumped
    request.ok();

    Ok(())
}
