use crate::leveldb::event::Config;
use crate::util::shutdown::Notifier;

use super::sstable::SSTableManager;
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
    manager: SSTableManager<tokio::fs::File>,
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
        let manager = SSTableManager::new();
        let (event_sender, mut event_builder) = EventLoopBuilder::new();

        info!("open manifest...");
        let manifest = Manifest::open(path.clone(), event_sender.clone(), manager.clone())?;
        info!("open memtable...");
        let memtable = MemTable::open(path, event_sender)?;

        // Start a background task
        event_builder
            .manifest(manifest.clone())
            .memtable(memtable.clone())
            .manager(manager.clone())
            .shutdown(shutdown.listen().unwrap());
        event_builder.run(Config::default());

        info!("open DB complete");
        Ok(DB {
            manifest,
            memtable,
            shutdown: Arc::new(Mutex::new(Some(shutdown))),
            manager,
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
            let table = self.manager.open(entry).await?;
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
    use super::{super::event::Event, *};
    use crate::{
        leveldb::event::{EventMessage, EventNotifier},
        util::shutdown::Notifier,
    };
    use rand::Rng;
    use tempfile::tempdir;
    use tokio::{
        sync::mpsc::{unbounded_channel, UnboundedReceiver},
        test,
    };
    use tracing::log::info;

    #[test(flavor = "multi_thread", worker_threads = 3)]
    async fn test_db_multi_set_with_sequence_case() {
        let _ = env_logger::builder().is_test(true).try_init();
        let min = 100000000;
        let max = 101000000;
        let temp_dir = tempdir().unwrap();
        let root_path = temp_dir.path();
        let root_path = "./data";
        let case = TestCase::build_sequence_and_reverse_case(min, max);

        info!("Start test of path, {:?}", root_path);
        test_db_multi_set(&case, root_path).await;
    }

    #[test(flavor = "multi_thread", worker_threads = 3)]
    async fn test_db_with_sequence_case() {
        let _ = env_logger::builder().is_test(true).try_init();
        let min = 100000000;
        let max = 101000000;
        let temp_dir = tempdir().unwrap();
        let root_path = temp_dir.path();
        let case = TestCase::build_sequence_and_reverse_case(min, max);

        info!("Start test of path, {:?}", root_path);
        test_db(&case, root_path).await;
    }

    #[test(flavor = "multi_thread", worker_threads = 3)]
    async fn test_dump_and_log_clean_with_sequence_case() {
        let _ = env_logger::builder().is_test(true).try_init();
        let min = 0;
        let max = 100000;
        let temp_dir = tempdir().unwrap();
        let root_path = temp_dir.path();
        let case = TestCase::build_sequence_and_reverse_case(min, max);

        info!("Start test of path, {:?}", root_path);
        test_dump_and_log_clean(&case, root_path).await;
    }

    #[test(flavor = "multi_thread", worker_threads = 3)]
    async fn test_compact_with_sequence_case() {
        let _ = env_logger::builder().is_test(true).try_init();
        let min = 100000000;
        let max = 101000000;
        let temp_dir = tempdir().unwrap();
        let root_path = temp_dir.path();
        let case = TestCase::build_sequence_and_reverse_case(min, max);

        info!("Start test of path, {:?}", root_path);
        test_compact(&case, root_path).await;
    }

    async fn test_db_multi_set(case: &TestCase, root_path: impl Into<PathBuf> + Copy) {
        info!("Test set/get key-value");
        let mut select = 0;
        {
            let db = DB::open(root_path).unwrap();
            case.test_set(&db, select);
            select = 1;
            case.test_set(&db, select);
            select = 0;
            case.test_set(&db, select);
            select = 1;
            case.test_set(&db, select);
            case.test_get(&db, select).await;

            db.shutdown().await;
        }

        info!("Test set/get after recovery");
        {
            let db = DB::open(root_path).unwrap();
            case.test_set(&db, select);
            select = 1;
            case.test_set(&db, select);
            select = 0;
            case.test_set(&db, select);
            select = 1;
            case.test_set(&db, select);
            case.test_get(&db, select).await;

            db.shutdown().await;
        }
    }

    async fn test_db(case: &TestCase, root_path: impl Into<PathBuf> + Copy) {
        info!("Test set/get key-value");
        let mut select = 0;
        {
            let db = DB::open(root_path).unwrap();
            case.test_set(&db, select);
            case.test_get(&db, select).await;
            db.shutdown().await
        }

        info!("Test recover");
        {
            let db = DB::open(root_path).unwrap();
            case.test_get(&db, select).await;
            select = 1;
            case.test_set(&db, select);
            case.test_get(&db, select).await;
            db.shutdown().await
        }

        info!("Test recover");
        {
            let db = DB::open(root_path).unwrap();
            case.test_get(&db, select).await;
            select = 0;
            case.test_set(&db, select);
            case.test_get(&db, select).await;
            select = 1;
            case.test_set(&db, select);
            case.test_get(&db, select).await;
            select = 0;
            case.test_set(&db, select);
            case.test_get(&db, select).await;
            db.shutdown().await
        }
    }

    async fn test_dump_and_log_clean(case: &TestCase, root_path: impl Into<PathBuf> + Copy) {
        info!("Test set/get key-value into memtable");
        let mut select = 0;
        {
            let (db, _event_sender, _rx) = create_background_full_control_db(root_path);
            case.test_set(&db, select);
            case.test_get(&db, select).await;
            db.shutdown().await
        }

        info!("Test memtable recovery and overwrite");
        {
            let (db, _event_sender, _rx) = create_background_full_control_db(root_path);
            case.test_get(&db, select).await;

            select = 1;

            // Overwrite with new value
            case.test_set(&db, select);
            case.test_get(&db, select).await;
            db.shutdown().await;
        }

        info!("Test memtable recovery again");
        {
            let (db, _event_sender, _rx) = create_background_full_control_db(root_path);
            case.test_get(&db, select).await;
            db.shutdown().await;
        }

        info!("Test get when dump");
        {
            let (db, event_sender, _rx) = create_background_full_control_db(root_path);
            // Trigger a dump operation
            let mut done = event_sender.notify(Event::Dump).unwrap();
            // Test util dump finished
            loop {
                case.test_get(&db, select).await;
                if done.is_done() {
                    break;
                }
            }
            // Test after dump finished
            case.test_get(&db, select).await;
            db.shutdown().await;
        }

        info!("Test recovery after dump");
        {
            let (db, event_sender, _rx) = create_background_full_control_db(root_path);
            case.test_get(&db, select).await;
            // Clean the memtable log, no effect
            event_sender
                .notify(Event::InactiveLogClean)
                .unwrap()
                .done()
                .await;
            db.shutdown().await;
        }

        info!("Test dump again");
        {
            let (db, event_sender, _rx) = create_background_full_control_db(root_path);
            case.test_get(&db, select).await;
            // Trigger a dump operation and clean log operation
            let mut done = event_sender.notify(Event::Dump).unwrap();
            event_sender.notify(Event::InactiveLogClean).unwrap();
            loop {
                // Test util dump finished
                case.test_get(&db, select).await;

                if done.is_done() {
                    break;
                }
            }
            case.test_get(&db, select).await;
            // Clean the memtable log
            event_sender
                .notify(Event::InactiveLogClean)
                .unwrap()
                .done()
                .await;
            case.test_get(&db, select).await;

            db.shutdown().await;
        }

        info!("Test recovery get/set after dump and clean log");
        {
            let (db, event_sender, _rx) = create_background_full_control_db(root_path);
            case.test_get(&db, select).await;

            select = 0;

            case.test_set(&db, select);
            case.test_get(&db, select).await;
            let mut done = event_sender.notify(Event::Dump).unwrap();
            event_sender.notify(Event::InactiveLogClean).unwrap();
            loop {
                // Test util dump finished
                case.test_get(&db, select).await;

                if done.is_done() {
                    break;
                }
            }
            case.test_get(&db, select).await;
            // Clean the memtable log
            event_sender
                .notify(Event::InactiveLogClean)
                .unwrap()
                .done()
                .await;
            case.test_get(&db, select).await;

            db.shutdown().await;
        }

        info!("Test recover again");
        {
            let (db, _event_sender, _rx) = create_background_full_control_db(root_path);
            case.test_get(&db, select).await;
        }
    }

    async fn test_compact(case: &TestCase, root_path: impl Into<PathBuf> + Copy) {
        let test_ratio = 0.001;
        let mut select = 0;

        info!("Test recovery get/set after dump and clean log");
        {
            // Create test status before compact
            let (db, event_sender, _rx) = create_background_full_control_db(root_path);
            case.test_set(&db, select);
            event_sender.notify(Event::Dump).unwrap().done().await;
            event_sender
                .notify(Event::InactiveLogClean)
                .unwrap()
                .done()
                .await;
            case.test_get(&db, select).await;

            // Trigger multiple compact operation
            for _ in 1..3 {
                let mut done = event_sender.notify(Event::Compact).unwrap();
                loop {
                    // Test util compact finished
                    case.test_get_random(&db, select, test_ratio).await;

                    if done.is_done() {
                        break;
                    }
                }
            }
            case.test_get_random(&db, select, test_ratio).await;
            db.shutdown().await;
        }

        info!("Test recover and overwrite");
        {
            let (db, event_sender, _rx) = create_background_full_control_db(root_path);
            case.test_get_random(&db, select, test_ratio).await;
            select = 1;

            case.test_set(&db, select);
            event_sender.notify(Event::Dump).unwrap().done().await;
            event_sender
                .notify(Event::InactiveLogClean)
                .unwrap()
                .done()
                .await;
            case.test_get(&db, select).await;

            // Trigger multiple compact operation
            for _ in 1..3 {
                let mut done = event_sender.notify(Event::Compact).unwrap();
                loop {
                    // Test util compact finished
                    case.test_get_random(&db, select, test_ratio).await;

                    if done.is_done() {
                        break;
                    }
                }
            }
            case.test_get_random(&db, select, test_ratio).await;
            db.shutdown().await;
        }
    }

    // Create DB with a manually controllable background task
    fn create_background_full_control_db(
        path: impl Into<PathBuf> + Copy,
    ) -> (DB, EventNotifier, UnboundedReceiver<EventMessage>) {
        let shutdown = Notifier::new();
        let (fake_event_sender, _rx) = unbounded_channel();
        let (event_sender, mut event_builder) = EventLoopBuilder::new();
        let fake_event_sender = EventNotifier::new(fake_event_sender);

        let manifest =
            Manifest::open(path, fake_event_sender.clone(), SSTableManager::new()).unwrap();
        let memtable = MemTable::open(path, fake_event_sender).unwrap();

        // Start a background task
        let config = Config {
            enable_timer: false,
            ..Default::default()
        };
        event_builder
            .manifest(manifest.clone())
            .memtable(memtable.clone())
            .shutdown(shutdown.listen().unwrap());
        event_builder.run(config);

        let db = DB {
            manifest,
            memtable,
            shutdown: Arc::new(Mutex::new(Some(shutdown))),
            manager: SSTableManager::new(),
        };

        (db, event_sender, _rx)
    }

    struct TestCase {
        case: Vec<(Bytes, Option<Bytes>, Option<Bytes>)>,
    }

    impl TestCase {
        fn build_sequence_and_reverse_case(min: usize, max: usize) -> Self {
            assert!(min < max);

            let mut case = Vec::with_capacity(max - min + 1);

            for k in min..=max {
                let key = Bytes::from(k.to_string());
                let val2 = Bytes::from((max - (k - min)).to_string());
                case.push((key.clone(), Some(key), Some(val2)));
            }

            TestCase { case }
        }

        async fn test_get_part(&self, db: &DB, select: usize, skip: usize, limit: usize) {
            assert!(select <= 1);
            for (key, val1, val2) in self.case.iter().skip(skip).take(limit) {
                let mut val = val1;
                if select != 0 {
                    val = val2;
                }
                assert_eq!(db.get(key).await.unwrap().as_ref(), val.as_ref());
            }
        }

        async fn test_get_random_part(
            &self,
            db: &DB,
            select: usize,
            skip: usize,
            limit: usize,
            ratio: f64,
        ) {
            assert!(select <= 1);
            let mut rng = rand::thread_rng();

            for (key, val1, val2) in self.case.iter().skip(skip).take(limit) {
                if rng.gen_bool(ratio) {
                    let mut val = val1;
                    if select != 0 {
                        val = val2;
                    }
                    assert_eq!(db.get(key).await.unwrap().as_ref(), val.as_ref());
                }
            }
        }

        fn test_set_part(&self, db: &DB, select: usize, skip: usize, limit: usize) {
            assert!(select <= 1);
            for (key, val1, val2) in self.case.iter().skip(skip).take(limit) {
                let mut val = val1;
                if select != 0 {
                    val = val2;
                }
                if val.is_some() {
                    db.set(key.clone(), val.as_ref().unwrap().clone()).unwrap();
                } else {
                    db.del(key.clone()).unwrap();
                }
            }
        }

        async fn test_get(&self, db: &DB, select: usize) {
            self.test_get_part(db, select, 0, self.case.len()).await;
        }

        async fn test_get_random(&self, db: &DB, select: usize, ratio: f64) {
            self.test_get_random_part(db, select, 0, self.case.len(), ratio)
                .await;
        }

        fn test_set(&self, db: &DB, select: usize) {
            self.test_set_part(db, select, 0, self.case.len());
        }
    }
}
