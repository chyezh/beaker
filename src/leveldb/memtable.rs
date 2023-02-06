use super::{
    event::Event,
    record::{RecordReader, RecordWriter},
    util::{async_scan_file_at_path, scan_sorted_file_at_path, Value},
    Error, Result,
};
use crate::util::from_le_bytes_32;
use bytes::Bytes;
use futures_core::Future;
use parking_lot::{Mutex, RwLock};
use std::{
    collections::BTreeMap,
    fs::{self, File, OpenOptions},
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
};
use tokio::sync::mpsc::UnboundedSender;
use tracing::info;

const KV_HEADER: usize = 8; // key_len + value_len 4+4
const SPLIT_LOG_SIZE_THRESHOLD: usize = 4 * 1024 * 1024; // 4M
const LOG_FILE_EXTENSION: &str = "log";

#[derive(Clone)]
// Implement a memtable with persist write-ahead-log file
pub struct MemTable {
    mutable: Arc<Mutex<KVTable>>,
    immutable: Arc<RwLock<Vec<Arc<KVTable>>>>,
    root_path: PathBuf,
    next_log_seq: Arc<AtomicU64>,
    event_sender: UnboundedSender<Event>,
    dump_mutex: Arc<tokio::sync::Mutex<()>>,
}

impl MemTable {
    pub fn open(
        root_path: impl Into<PathBuf>,
        event_sender: UnboundedSender<Event>,
    ) -> Result<Self> {
        // Try to create log root directory at this path.
        let root_path: PathBuf = root_path.into();
        fs::create_dir_all(root_path.as_path())?;

        // Scan the root_path directory, find all log sorted by sequence number
        let log_files = scan_sorted_file_at_path(&root_path, LOG_FILE_EXTENSION)?;

        // Recover immutables
        let (immutable, mut next_log_seq) = if !log_files.is_empty() {
            let mut tables = Vec::with_capacity(log_files.len());

            // Recover immutable memtable
            for (path, seq) in log_files.iter() {
                let reader = RecordReader::new(File::open(path.as_path())?);
                let mut table = KVTable::new(*seq);
                for data in reader {
                    let (key, value) = decode_kv(data?)?;
                    table.entries.insert(key, value);
                }

                // Old tables never write new entry
                let immutable_table = Arc::new(table);
                tables.push(Arc::clone(&immutable_table));
            }
            (tables, log_files.last().unwrap().1 + 1)
        } else {
            (Vec::new(), 0)
        };

        // Create new mutable to write
        let new_log_seq = next_log_seq;
        let (new_file, _) = open_new_log(&root_path, new_log_seq)?;
        let mut mutable = KVTable::new(new_log_seq);
        mutable.log = Some(RecordWriter::new(new_file));
        next_log_seq += 1;

        // Try to dump old immutable memtable
        if !immutable.is_empty() {
            let send_result = event_sender.send(Event::Dump);
            debug_assert!(send_result.is_ok());
        }

        let table = MemTable {
            mutable: Arc::new(Mutex::new(mutable)),
            immutable: Arc::new(RwLock::new(immutable)),
            root_path,
            next_log_seq: Arc::new(AtomicU64::new(next_log_seq)),
            event_sender,
            dump_mutex: Arc::new(tokio::sync::Mutex::new(())),
        };

        Ok(table)
    }

    // Get the value by key
    pub fn get(&self, key: &[u8]) -> Option<Value> {
        // Read mutable first
        if let Some(value) = self.mutable.lock().entries.get(key) {
            return Some(value.clone());
        }

        // Read from immutable list
        let immutable = self.immutable.read();
        for table in immutable.iter().rev() {
            if let Some(value) = table.entries.get(key) {
                return Some(value.clone());
            }
        }
        None
    }

    // Set the key-value pair into mutable
    pub fn set(&self, key: Bytes, value: Value) -> Result<()> {
        let mut mutable = self.mutable.lock();
        // Mutable log must exists
        debug_assert!(mutable.log.is_some());

        // Write kv pair into log ahead
        let total_written = mutable
            .log
            .as_mut()
            .unwrap()
            .append(encode_kv(&key, &value))?;

        // Set value into in-memory table
        mutable.entries.insert(key, value);

        // Switch new log if needed
        // Low-rate route
        if total_written > SPLIT_LOG_SIZE_THRESHOLD {
            let new_log_seq = self.next_log_seq.fetch_add(1, Ordering::Relaxed);

            // Create a new writer
            let (new_file, _) = open_new_log(&self.root_path, new_log_seq)?;
            let mut new_mutable = KVTable::new(new_log_seq);
            new_mutable.log = Some(RecordWriter::new(new_file));

            // Swap mutable, close writer, and convert old mutable into immutable
            std::mem::swap(&mut *mutable, &mut new_mutable);
            // Close the file writer, ignore the synchronizing fail.
            new_mutable.log.take();

            let immutable = Arc::new(new_mutable);
            // Release immutable first, read requests see new empty mutable after new immutable.
            // Previous write would not be lost on this read requests.
            self.immutable.write().push(Arc::clone(&immutable));

            // Send a dump request
            // Send operation would never fail
            let result = self.event_sender.send(Event::Dump);
            debug_assert!(result.is_ok());
        }

        Ok(())
    }

    // Dump oldest immutable memtable and remove it
    pub async fn dump_oldest_immutable<
        C: FnOnce(Vec<Arc<KVTable>>) -> E,
        E: Future<Output = Result<()>> + Send + 'static,
    >(
        &self,
        f: C,
    ) -> Result<()> {
        // Acquire lock first
        let _guard = self.dump_mutex.try_lock();
        if _guard.is_err() {
            return Ok(());
        }
        let immutable = self.immutable.read().clone();
        let immutable_len = immutable.len();
        if immutable_len > 0 {
            // Try to Dump all immutable
            f(immutable).await?;
            // Remove all dumped immutable if dump finish
            let mut tables = self.immutable.write();
            let mut new_tables = Vec::with_capacity((2 * (tables.len() - immutable_len)).max(5));
            for table in &tables[immutable_len..] {
                new_tables.push(Arc::clone(table));
            }
            *tables = new_tables;

            // Try to clear log
            if let Err(err) = self.event_sender.send(Event::InactiveLogClean) {
                info!(
                    error = err.to_string(),
                    "receiver for inactive log clean trigger has been released"
                );
            }
        }

        Ok(())
    }

    // Clean the inactive memtable log
    pub async fn clean_inactive_log(&self) -> Result<()> {
        let filenames = async_scan_file_at_path(&self.root_path, LOG_FILE_EXTENSION).await?;

        // at least one mutable memtable
        if filenames.len() <= 1 {
            return Ok(());
        }

        // Get minimal immutable seq id
        // It's safe to get mutable seq first, seq is monotonic.
        let mut minimal_seq = self.mutable.lock().seq;
        {
            // It's safe to clean all log except mutable if immutable is empty
            if let Some(table) = self.immutable.read().first() {
                minimal_seq = table.seq;
            }
        }

        // Clear log which sequence is less than minimal seq
        for (filename, seq) in filenames {
            if seq < minimal_seq {
                tokio::fs::remove_file(filename).await?;
            }
        }

        Ok(())
    }
}

// Encode the KV pair into binary format. While no checksum is applied on here, record writer downside has promised. Layout:
// 1. 0-4 key_len
// 2. 4-8 value_len
// 3. key
// 4. Value::write_to
fn encode_kv(key: &[u8], value: &Value) -> Bytes {
    // Key length + key + value
    let mut buffer = Vec::with_capacity(4 + key.len() + value.encode_bytes_len());

    buffer.extend_from_slice(&key.len().to_le_bytes()[0..4]);
    buffer.extend_from_slice(&value.encode_bytes_len().to_le_bytes()[0..4]);
    buffer.extend(key);
    // IO always success on Vec, ignore error
    value.encode_to(&mut buffer).ok();

    buffer.into()
}

// Decode the KV Pair from binary format
fn decode_kv(data: Bytes) -> Result<(Bytes, Value)> {
    debug_assert!(data.len() >= KV_HEADER);
    if data.len() < KV_HEADER {
        return Err(Error::IllegalLog);
    }

    // Parse header
    let key_length = from_le_bytes_32(&data[0..4]);
    let value_length = from_le_bytes_32(&data[4..8]);
    debug_assert_eq!(data.len(), key_length + value_length + KV_HEADER);
    if data.len() != key_length + value_length + KV_HEADER {
        return Err(Error::IllegalLog);
    }

    let key_offset = 8;
    let value_offset = key_offset + key_length;

    // Parse key part
    let key = data.slice(key_offset..value_offset);
    // Parse value part
    let value = Value::decode_from_bytes(data.slice(value_offset..))?;
    Ok((key, value))
}

// Open a new log with given log sequence number
fn open_new_log(dir: &Path, log_seq: u64) -> Result<(File, PathBuf)> {
    let new_log_path = dir.join(format!("{}.{}", log_seq, LOG_FILE_EXTENSION));

    let new_file = OpenOptions::new()
        .truncate(true)
        .create(true)
        .write(true)
        .open(&new_log_path)?;

    Ok((new_file, new_log_path))
}

// Implement kv table
// TODO: replace by skip list in future
#[derive(Debug)]
pub struct KVTable {
    entries: BTreeMap<Bytes, Value>,
    log: Option<RecordWriter<File>>,
    seq: u64,
}

impl KVTable {
    // Create a new memtable
    fn new(seq: u64) -> KVTable {
        KVTable {
            entries: BTreeMap::default(),
            log: None,
            seq,
        }
    }

    // Get iterator of the table
    #[inline]
    pub fn iter(&self) -> impl Iterator<Item = (&Bytes, &Value)> {
        self.entries.iter()
    }
}

#[cfg(test)]
mod tests {
    use core::panic;

    use tokio::sync::mpsc::unbounded_channel;

    use super::*;
    use crate::util::test_case::generate_random_bytes;

    #[test]
    fn test_log_with_random_case() {
        let test_count = 100;

        let test_case_key = generate_random_bytes(test_count, 10000);
        let test_case_value = generate_random_bytes(test_count, 10 * 32 * 1024);
        let (tx, _rx) = unbounded_channel();

        // Test normal value
        let memtable = MemTable::open("./data", tx.clone()).unwrap();
        for (key, value) in test_case_key.iter().zip(test_case_value.iter()) {
            memtable
                .set(key.clone(), Value::Living(value.clone()))
                .unwrap();
        }

        drop(memtable);
        let memtable = MemTable::open("./data", tx.clone()).unwrap();
        for (key, value) in test_case_key.iter().zip(test_case_value.iter()) {
            if let Some(Value::Living(v)) = memtable.get(key) {
                assert_eq!(value, &v);
            } else {
                panic!("value not found or not match");
            }
        }

        // Test deleted value
        let test_case_deleted = generate_random_bytes(test_count, 2);
        for (key, is_deleted) in test_case_key.iter().zip(test_case_deleted.iter()) {
            if is_deleted.first().unwrap() % 2 == 0 {
                memtable.set(key.clone(), Value::Tombstone).unwrap();
            }
        }

        drop(memtable);
        let memtable = MemTable::open("./data", tx.clone()).unwrap();
        for ((key, value), is_deleted) in test_case_key
            .iter()
            .zip(test_case_value.iter())
            .zip(test_case_deleted.iter())
        {
            if is_deleted.first().unwrap() % 2 == 0 {
                assert!(matches!(memtable.get(key).unwrap(), Value::Tombstone));
            } else if let Some(Value::Living(v)) = memtable.get(key) {
                assert_eq!(value, &v);
            } else {
                panic!("value not found or not match");
            }
        }

        // Set back again
        for ((key, value), is_deleted) in test_case_key
            .iter()
            .zip(test_case_value.iter())
            .zip(test_case_deleted.iter())
        {
            if is_deleted.first().unwrap() % 2 == 0 {
                memtable
                    .set(key.clone(), Value::Living(value.clone()))
                    .unwrap();
            }
        }

        drop(memtable);
        let memtable = MemTable::open("./data", tx).unwrap();
        for (key, value) in test_case_key.iter().zip(test_case_value.iter()) {
            if let Some(Value::Living(v)) = memtable.get(key) {
                assert_eq!(value, &v);
            } else {
                panic!("value not found or not match");
            }
        }
    }
}
