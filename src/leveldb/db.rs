use crate::util::shutdown::{Listener, Notifier};

use super::{
    compact::Compactor,
    manifest::Manifest,
    memtable::{DumpRequest, MemTable},
    sstable,
    util::Value,
    Result,
};
use bytes::Bytes;
use parking_lot::Mutex;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::mpsc::UnboundedReceiver;
use tracing::{info, warn};

#[derive(Clone)]
pub struct DB {
    manifest: Manifest,
    memtable: MemTable,
    shutdown: Arc<Mutex<Option<Notifier>>>,
}

impl DB {
    // Open levelDB on given path
    pub fn open(path: impl Into<PathBuf>) -> Result<Self> {
        let path = path.into();
        let shutdown = Notifier::new();

        let (manifest, compact_rx, clean_sstable_rx) = Manifest::open(path.clone())?;
        let (memtable, dump_rx) = MemTable::open(path)?;

        // Start a background task
        // TODO: refactor into event-based
        // Transform memtable into level 0 sstable
        listen_dump(
            manifest.clone(),
            memtable.clone(),
            dump_rx,
            shutdown.listen().unwrap(),
        );
        listen_compact(manifest.clone(), compact_rx, shutdown.listen().unwrap());
        listen_inactive_sstable_clean(
            manifest.clone(),
            clean_sstable_rx,
            shutdown.listen().unwrap(),
        );
        listen_clear_inactive_readers(shutdown.listen().unwrap());
        Ok(DB {
            manifest,
            memtable,
            shutdown: Arc::new(Mutex::new(Some(shutdown))),
        })
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
        for entry in self.manifest.search(key) {
            let table = sstable::open(entry).await?;
            if let Some(value) = table.search(key).await? {
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

    // Shutdown the database, and drop the database
    pub async fn shutdown(self) {
        let mut shutdown = self.shutdown.lock().take().unwrap();
        shutdown.notify().await;
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
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(60));
        loop {
            tokio::select! {
                _ = shutdown_listener.listen() => {
                    // Stop creating new compact task if shutdown signal was notified
                    break;
                }
                _ = listener.recv() => {
                    info!("start find new compact task...");
                    compact(&manifest, &shutdown_listener);
                    info!("finish find new compact task");
                }
                _ = interval.tick() => {
                    info!("start find new compact task from timer...");
                    compact(&manifest, &shutdown_listener);
                    info!("finish find new compact task from timer");
                }
            }
        }
        info!("stop creating new compact task");
    });
}

// Do a compaction
fn compact(manifest: &Manifest, shutdown_listener: &Listener) {
    for task in manifest.find_new_compact_tasks() {
        let manifest = manifest.clone();
        info!("compact task found, {:?}", task);

        // Hold a listener copy, to block shutdown util compact task finish
        let new_shutdown_listener = shutdown_listener.clone();
        tokio::spawn(async move {
            new_shutdown_listener.hold();
            if let Err(err) = Compactor::new(task, manifest).compact().await {
                warn!(error = err.to_string(), "compact task failed")
            }
        });
    }
}

// Listen sstable clean
fn listen_inactive_sstable_clean(
    manifest: Manifest,
    mut listener: UnboundedReceiver<()>,
    mut shutdown_listener: Listener,
) {
    tokio::spawn(async move {
        info!("inactive sstable cleaner listening...");
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(5 * 60));

        loop {
            tokio::select! {
                _ = shutdown_listener.listen() => {
                    // Stop clean inactive sstable on disk
                    break;
                }
                Some(_) = listener.recv() => {
                    info!("start a new inactive sstable clean task from signal");
                    if let Err(err) = manifest.clean_inactive_sstable().await {
                        warn!(error = err.to_string(), "clean inactive sstable failed");
                    }
                }
                _ = interval.tick() => {
                    info!("start a new inactive sstable clean task from timer");
                    if let Err(err) = manifest.clean_inactive_sstable().await {
                        warn!(error = err.to_string(), "clean inactive sstable failed");
                    }
                }
            }
        }
    });
}

fn listen_clear_inactive_readers(mut shutdown_listener: Listener) {
    tokio::spawn(async move {
        info!("clear inactive reader listening...");
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(60));
        loop {
            tokio::select! {
                _ = shutdown_listener.listen() => {
                    break;
                }
                _ = interval.tick() => {
                    info!("start clear inactive readers");
                    sstable::clear_inactive_readers(60);
                }
            }
        }
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
                    if let Err(err) = dump(&manifest, &memtable, request).await {
                        warn!(error = err.to_string(), "dump memtable into sstable failed");
                    }
                }
            }
        }
        info!("stop dump task")
    });
}

// Dump a memtable into level 0 sstable
async fn dump(manifest: &Manifest, memtable: &MemTable, request: DumpRequest) -> Result<()> {
    // Build a new sstable from memtable if memtable is not empty
    if request.len() != 0 {
        let entry = manifest.alloc_new_sstable_entry(0);
        let mut builder = entry.open_builder().await?;
        for (key, value) in request.iter() {
            builder.add(key.clone(), value.clone()).await?;
        }
        let entry = builder.finish().await?;

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
    use crate::util::test_case::{
        generate_random_bytes, reverse_sequence_number_iter, sequence_number_iter,
    };
    use rand::{self, Rng};
    use tokio::test;

    #[test]
    async fn test_db_with_random_case() {
        let _ = env_logger::builder().is_test(true).try_init();

        let test_count = 1000;
        let test_case_key = generate_random_bytes(test_count, 10000);
        let test_case_value = generate_random_bytes(test_count, 10 * 32 * 1024);

        let db = DB::open("./data").unwrap();
        for (key, value) in test_case_key.iter().zip(test_case_value.iter()) {
            db.set(key.clone(), value.clone()).unwrap();
        }

        for (key, value) in test_case_key.iter().zip(test_case_value.iter()) {
            assert_eq!(db.get(key).await.unwrap().unwrap(), value.clone());
        }
        db.shutdown().await;
    }

    #[test]
    async fn test_get_db_with_sequence_number() {
        let _ = env_logger::builder().is_test(true).try_init();

        let test_count = 1000000;
        let db = DB::open("./data").unwrap();
        // Get db
        for (k, v) in sequence_number_iter(test_count).zip(reverse_sequence_number_iter(test_count))
        {
            assert_eq!(db.get(&k).await.unwrap(), Some(v.clone()));
        }

        // Reopen db and test get
        db.shutdown().await;
        let db = DB::open("./data").unwrap();
        for (k, v) in sequence_number_iter(test_count).zip(reverse_sequence_number_iter(test_count))
        {
            assert_eq!(db.get(&k).await.unwrap(), Some(v.clone()));
        }
    }

    #[test(flavor = "multi_thread", worker_threads = 3)]
    async fn test_db_with_sequence_number2() {
        // Reopen db and test get
        let db = DB::open("./data").unwrap();
        let test_count = 10000000;

        //for (k, v) in sequence_number_iter(test_count).zip(reverse_sequence_number_iter(test_count))
        //{
        //    assert_eq!(db.get(&k).await.unwrap(), Some(v.clone()));
        //}
    }

    #[test(flavor = "multi_thread", worker_threads = 3)]
    async fn test_db_with_sequence_number() {
        let _ = env_logger::builder().is_test(true).try_init();
        tracing::info!("start testing");

        let mut rng = rand::thread_rng();
        let test_count = 10000000;
        let db = DB::open("./data").unwrap();

        // Set db
        for i in sequence_number_iter(test_count) {
            db.set(i.clone(), i.clone()).unwrap();
        }

        // Get db
        for i in sequence_number_iter(test_count) {
            if 0 == rng.gen_range(0..100) {
                assert_eq!(db.get(&i).await.unwrap(), Some(i.clone()));
            }
        }
        db.shutdown().await;

        // Reopen db and test get
        let db = DB::open("./data").unwrap();
        for i in sequence_number_iter(test_count) {
            if 0 == rng.gen_range(0..100) {
                assert_eq!(db.get(&i).await.unwrap(), Some(i.clone()));
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
                assert_eq!(db.get(&k).await.unwrap(), Some(v.clone()));
            }
        }

        // Reopen db and test get
        db.shutdown().await;
        let db = DB::open("./data").unwrap();
        for (k, v) in sequence_number_iter(test_count).zip(reverse_sequence_number_iter(test_count))
        {
            if 0 == rng.gen_range(0..100) {
                assert_eq!(db.get(&k).await.unwrap(), Some(v.clone()));
            }
        }
        tokio::time::sleep(std::time::Duration::from_secs(300)).await;
    }
}
