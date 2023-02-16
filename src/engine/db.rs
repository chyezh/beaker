use super::{config::Config, manifest::Manifest, memtable::MemTable, util::Value, Result};
use crate::{engine::config::Initial, util::shutdown::Notifier};
use bytes::Bytes;
use parking_lot::Mutex;
use std::sync::Arc;
use tracing::info;

#[derive(Clone)]
pub struct DB {
    manifest: Manifest,
    memtable: MemTable,
    initial: Initial,
    shutdown: Arc<Mutex<Option<Notifier>>>,
}

impl DB {
    /// Open levelDB on given path
    pub fn open(config: Config) -> Result<Self> {
        info!("open DB...");
        let shutdown = Notifier::new();
        let (initial, event_builder) = Initial::new();

        info!("open manifest...");
        let manifest = Manifest::open(config.clone(), initial.clone())?;
        info!("open memtable...");
        let memtable = MemTable::open(config.clone(), initial.clone())?;

        // Start a background task
        event_builder
            .manifest(manifest.clone())
            .memtable(memtable.clone())
            .manager(initial.sstable_manager.clone())
            .shutdown(shutdown.listen().unwrap())
            .run(config.timer);

        info!("open DB complete");
        Ok(DB {
            manifest,
            memtable,
            initial,
            shutdown: Arc::new(Mutex::new(Some(shutdown))),
        })
    }

    /// Get value from db by key
    pub async fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        // Get memtable first
        if let Some(value) = self.memtable.get(key) {
            match value {
                Value::Living(v) | Value::LivingMeta(v, _) => return Ok(Some(v)),
                Value::Tombstone | Value::TombstoneMeta(_) => return Ok(None),
            }
        }

        // Get possible sstables from manifest
        // Read sstable to search value
        for entry in self.manifest.search(key) {
            let table = self.initial.sstable_manager.open(entry).await?;
            if let Some(value) = table.search(key).await? {
                match value {
                    Value::Living(v) | Value::LivingMeta(v, _) => return Ok(Some(v)),
                    Value::Tombstone | Value::TombstoneMeta(_) => return Ok(None),
                }
            }
        }

        // Not found
        Ok(None)
    }

    /// Set key value pair into db and return log_id of the write operation
    #[inline]
    pub fn set(&self, key: Bytes, value: Bytes) -> Result<u64> {
        self.memtable.set(key, Value::Living(value))
    }

    /// Delete value by key and return log_id of the write operation
    #[inline]
    pub fn del(&self, key: Bytes) -> Result<u64> {
        self.memtable.set(key, Value::Tombstone)
    }

    /// Set key value pair into db and return log_id of the write operation
    #[inline]
    pub fn set_with_meta(&self, key: Bytes, value: Bytes, meta: Bytes) -> Result<u64> {
        self.memtable.set(key, Value::LivingMeta(value, meta))
    }

    /// Delete value by key and return log_id of the write operation
    #[inline]
    pub fn del_with_meta(&self, key: Bytes, meta: Bytes) -> Result<u64> {
        self.memtable.set(key, Value::TombstoneMeta(meta))
    }

    /// Permit: dump operation can be applied to log util given new_log_id.
    /// Dump log id always grows monotonic, and never be set as Some when it's initialized as None.
    #[inline]
    pub fn permit_dump_util(&self, new_log_id: u64) -> Result<()> {
        self.memtable.permit_dump_util(new_log_id)
    }

    // Shutdown the database, and drop the database
    pub async fn shutdown(self) {
        info!("shutdown DB...");
        let mut shutdown = self.shutdown.lock().take().unwrap();
        shutdown.notify().await;
        info!("DB shutdown");
    }
}

#[cfg(test)]
mod tests {
    use super::{super::event::Event, *};
    use crate::{
        engine::event::{EventMessage, EventNotifier},
        util::shutdown::Notifier,
    };
    use rand::Rng;
    use std::path::PathBuf;
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
            let db = DB::open(Config::default_config_with_path(root_path)).unwrap();
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
            let db = DB::open(Config::default_config_with_path(root_path)).unwrap();
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
            let db = DB::open(Config::default_config_with_path(root_path)).unwrap();
            case.test_set(&db, select);
            case.test_get(&db, select).await;
            db.shutdown().await
        }

        info!("Test recover");
        {
            let db = DB::open(Config::default_config_with_path(root_path)).unwrap();
            case.test_get(&db, select).await;
            select = 1;
            case.test_set(&db, select);
            case.test_get(&db, select).await;
            db.shutdown().await
        }

        info!("Test recover");
        {
            let db = DB::open(Config::default_config_with_path(root_path)).unwrap();
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
        // Disable timer
        let config = Config::default_config_with_path(path.into());
        let shutdown = Notifier::new();

        // Create a manually control event
        let (mut initial, event_builder) = Initial::new();
        let (fake_event_sender, _rx) = unbounded_channel();
        let mut fake_event_sender = EventNotifier::new(fake_event_sender);
        std::mem::swap(&mut initial.event_sender, &mut fake_event_sender);
        // Now, fake_event_sender is the true event sender.

        let manifest = Manifest::open(config.clone(), initial.clone()).unwrap();
        let memtable = MemTable::open(config.clone(), initial.clone()).unwrap();
        // Start a background task
        event_builder
            .manifest(manifest.clone())
            .memtable(memtable.clone())
            .manager(initial.sstable_manager.clone())
            .shutdown(shutdown.listen().unwrap())
            .run(config.timer);

        let db = DB {
            manifest,
            memtable,
            initial,
            shutdown: Arc::new(Mutex::new(Some(shutdown))),
        };

        (db, fake_event_sender, _rx)
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
