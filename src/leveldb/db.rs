use crate::leveldb::event::Config;
use crate::util::shutdown::Notifier;

use super::{
    event::EventLoopBuilder, manifest::Manifest, memtable::MemTable, sstable, util::Value, Result,
};
use bytes::Bytes;
use parking_lot::Mutex;
use std::path::PathBuf;
use std::sync::Arc;
use tracing::info;

#[derive(Clone)]
pub struct DB {
    manifest: Manifest,
    memtable: MemTable,
    shutdown: Arc<Mutex<Option<Notifier>>>,
}

impl DB {
    // Open levelDB on given path
    pub fn open(path: impl Into<PathBuf>) -> Result<Self> {
        info!("open DB...");
        let path = path.into();
        let shutdown = Notifier::new();
        let (event_sender, mut event_builder) = EventLoopBuilder::new();

        info!("open manifest...");
        let manifest = Manifest::open(path.clone(), event_sender.clone())?;
        info!("open memtable...");
        let memtable = MemTable::open(path, event_sender)?;

        // Start a background task
        event_builder
            .manifest(manifest.clone())
            .memtable(memtable.clone())
            .shutdown(shutdown.listen().unwrap());
        event_builder.run(Config::default());

        info!("open DB complete");
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
        info!("shutdown DB...");
        let mut shutdown = self.shutdown.lock().take().unwrap();
        shutdown.notify().await;
        info!("shutdown DB complete");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::util::{
        shutdown::Notifier,
        test_case::{reverse_sequence_number_kv_iter, sequence_number_kv_iter},
    };
    use crate::{
        leveldb::event::Event,
        util::{
            shutdown,
            test_case::{
                generate_random_bytes, reverse_sequence_number_iter, sequence_number_iter,
            },
        },
    };
    use rand::{self, Rng};
    use tokio::{
        sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender},
        test,
    };
    use tracing::log::info;

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
    async fn test_db_with_dump_and_clean_task() {
        let _ = env_logger::builder().is_test(true).try_init();
        info!("start testing");
        let test_count = 100000;
        let root_path = "./data";

        info!("Test set/get key-value into memtable");
        {
            let (db, _event_sender, _rx) = create_background_full_control_db(root_path);

            // Set DB
            for (k, v) in sequence_number_kv_iter(test_count) {
                db.set(k, v).unwrap();
            }

            // Get DB
            for (k, v) in sequence_number_kv_iter(test_count) {
                assert_eq!(db.get(&k).await.unwrap(), Some(v));
            }

            db.shutdown().await
        }

        info!("Test memtable recovery and overwrite");
        {
            let (db, _event_sender, _rx) = create_background_full_control_db(root_path);

            // Get DB
            for (k, v) in sequence_number_kv_iter(test_count) {
                assert_eq!(db.get(&k).await.unwrap(), Some(v));
            }

            // Set DB
            for (k, v) in reverse_sequence_number_kv_iter(test_count) {
                db.set(k, v).unwrap();
            }

            // Get DB
            for (k, v) in reverse_sequence_number_kv_iter(test_count) {
                assert_eq!(db.get(&k).await.unwrap(), Some(v));
            }

            db.shutdown().await;
        }

        info!("Test memtable recovery again");
        {
            let (db, _event_sender, _rx) = create_background_full_control_db(root_path);

            // Get DB
            for (k, v) in reverse_sequence_number_kv_iter(test_count) {
                assert_eq!(db.get(&k).await.unwrap(), Some(v));
            }

            db.shutdown().await;
        }

        info!("Test get when dump");
        {
            let (db, event_sender, _rx) = create_background_full_control_db(root_path);
            // Trigger a dump operation
            event_sender.send(Event::Dump).unwrap();

            let version = db.manifest.version();
            loop {
                // Test util dump finished
                for (k, v) in reverse_sequence_number_kv_iter(test_count) {
                    assert_eq!(db.get(&k).await.unwrap(), Some(v));
                }

                if db.manifest.version() > version {
                    break;
                }
            }

            // Test after dump finished
            for (k, v) in reverse_sequence_number_kv_iter(test_count) {
                assert_eq!(db.get(&k).await.unwrap(), Some(v));
            }

            db.shutdown().await;
        }

        info!("Test recovery after dump");
        {
            let (db, event_sender, _rx) = create_background_full_control_db(root_path);

            // Test util dump finished
            for (k, v) in reverse_sequence_number_kv_iter(test_count) {
                assert_eq!(db.get(&k).await.unwrap(), Some(v));
            }

            // Clean the memtable log, no effect
            event_sender.send(Event::InactiveLogClean).unwrap();
            tokio::time::sleep(std::time::Duration::from_secs(5)).await;

            db.shutdown().await;
        }

        info!("Test dump again");
        {
            let (db, event_sender, _rx) = create_background_full_control_db(root_path);

            for (k, v) in reverse_sequence_number_kv_iter(test_count) {
                assert_eq!(db.get(&k).await.unwrap(), Some(v));
            }
            // Trigger a dump operation
            event_sender.send(Event::Dump).unwrap();

            let version = db.manifest.version();
            loop {
                // Test util dump finished
                for (k, v) in reverse_sequence_number_kv_iter(test_count) {
                    assert_eq!(db.get(&k).await.unwrap(), Some(v));
                }

                if db.manifest.version() > version {
                    break;
                }
            }

            // Test after dump finished
            for (k, v) in reverse_sequence_number_kv_iter(test_count) {
                assert_eq!(db.get(&k).await.unwrap(), Some(v));
            }

            // Clean the memtable log, no effect
            event_sender.send(Event::InactiveLogClean).unwrap();
            tokio::time::sleep(std::time::Duration::from_secs(5)).await;

            db.shutdown().await;
        }

        info!("Test get after dump and clean log");
        {
            let (db, event_sender, _rx) = create_background_full_control_db(root_path);

            for (k, v) in reverse_sequence_number_kv_iter(test_count) {
                assert_eq!(db.get(&k).await.unwrap(), Some(v));
            }

            db.shutdown().await;
        }
    }

    #[test(flavor = "multi_thread", worker_threads = 3)]
    async fn test_db_with_sequence_number() {
        let _ = env_logger::builder().is_test(true).try_init();
        tracing::info!("start testing");

        let mut rng = rand::thread_rng();
        let test_count = 1000000;
        let get_check_rate = 10;
        let db = DB::open("./data").unwrap();

        // Set db
        for i in sequence_number_iter(test_count) {
            db.set(i.clone(), i.clone()).unwrap();
        }

        // Get db
        for i in sequence_number_iter(test_count) {
            if 0 == rng.gen_range(0..get_check_rate) {
                assert_eq!(db.get(&i).await.unwrap(), Some(i.clone()));
            }
        }
        db.shutdown().await;

        // Reopen db and test get
        let db = DB::open("./data").unwrap();
        for i in sequence_number_iter(test_count) {
            if 0 == rng.gen_range(0..get_check_rate) {
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
            if 0 == rng.gen_range(0..get_check_rate) {
                assert_eq!(db.get(&k).await.unwrap(), Some(v.clone()));
            }
        }

        // Reopen db and test get
        db.shutdown().await;
        let db = DB::open("./data").unwrap();
        for (k, v) in sequence_number_iter(test_count).zip(reverse_sequence_number_iter(test_count))
        {
            if 0 == rng.gen_range(0..get_check_rate) {
                assert_eq!(db.get(&k).await.unwrap(), Some(v.clone()));
            }
        }
        tokio::time::sleep(std::time::Duration::from_secs(600)).await;
    }

    fn create_background_full_control_db(
        path: impl Into<PathBuf> + Copy,
    ) -> (DB, UnboundedSender<Event>, UnboundedReceiver<Event>) {
        let shutdown = Notifier::new();
        let (fake_event_sender, _rx) = unbounded_channel();
        let (event_sender, mut event_builder) = EventLoopBuilder::new();

        let manifest = Manifest::open(path, fake_event_sender.clone()).unwrap();
        let memtable = MemTable::open(path, fake_event_sender).unwrap();

        // Start a background task
        let mut config = Config::default();
        config.enable_timer = false;
        event_builder
            .manifest(manifest.clone())
            .memtable(memtable.clone())
            .shutdown(shutdown.listen().unwrap());
        event_builder.run(config);

        let db = DB {
            manifest,
            memtable,
            shutdown: Arc::new(Mutex::new(Some(shutdown))),
        };

        (db, event_sender, _rx)
    }
}
