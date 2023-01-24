use bytes::Bytes;

use crate::util::from_le_bytes_32;

use super::{
    record::{RecordReader, RecordWriter},
    util::{scan_sorted_file_at_path, Value},
    Error, Result,
};
use parking_lot::{Mutex, RwLock};
use std::fs::{self, File, OpenOptions};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::{
    collections::BTreeMap,
    sync::atomic::{AtomicU64, Ordering},
};

use tokio::sync::mpsc::{self, UnboundedReceiver, UnboundedSender};

const KV_HEADER: usize = 8; // key_len + value_len 4+4
const SPLIT_LOG_SIZE_THRESHOLD: usize = 4 * 1024 * 1024; // 4M
const LOG_FILE_EXTENSION: &str = "log";

// Implement a memtable with persist write-ahead-log file
pub struct MemTable {
    mutable: Arc<Mutex<KVTable>>,
    immutable: Arc<RwLock<Vec<Arc<KVTable>>>>,
    root_path: PathBuf,
    next_log_seq: AtomicU64,

    dump_tx: UnboundedSender<DumpRequest>,
    dump_rx: Option<UnboundedReceiver<DumpRequest>>,
    dump_finish_tx: UnboundedSender<PathBuf>,
}

impl MemTable {
    pub fn open(root_path: impl Into<PathBuf>) -> Result<Self> {
        // Try to create log root directory at this path.
        let root_path: PathBuf = root_path.into();
        fs::create_dir_all(root_path.as_path())?;

        // Scan the root_path directory, find all log sorted by sequence number
        let log_files = scan_sorted_file_at_path(&root_path, LOG_FILE_EXTENSION)?;

        // Create dump task channel
        let (tx, rx) = mpsc::unbounded_channel::<DumpRequest>();
        let (finish_tx, finish_rx) = mpsc::unbounded_channel::<PathBuf>();

        // Recover immutables
        let (immutable, mut next_log_seq) = if !log_files.is_empty() {
            let mut tables = Vec::with_capacity(log_files.len());

            // Recover immutable memtable
            for (path, _) in log_files.iter() {
                let reader = RecordReader::new(File::open(path.as_path())?);
                let mut table = KVTable::new(path.clone());
                for data in reader {
                    let (key, value) = decode_kv(data?)?;
                    table.entries.insert(key, value);
                }

                // Old tables never write new entry
                let immutable_table = Arc::new(table);
                tables.push(Arc::clone(&immutable_table));
                // Ask to dump the immutable memtable
                let send_result = tx.send(DumpRequest::new(immutable_table, finish_tx.clone()));
                debug_assert!(send_result.is_ok());
            }
            (tables, log_files.last().unwrap().1 + 1)
        } else {
            (Vec::new(), 0)
        };

        // Create new mutable to write
        let new_log_seq = next_log_seq;
        let (new_file, new_log_path) = open_new_log(&root_path, new_log_seq)?;
        let mut mutable = KVTable::new(new_log_path);
        mutable.log = Some(RecordWriter::new(new_file));
        next_log_seq += 1;

        let table = MemTable {
            mutable: Arc::new(Mutex::new(mutable)),
            immutable: Arc::new(RwLock::new(immutable)),
            root_path,
            next_log_seq: AtomicU64::new(next_log_seq),
            dump_tx: tx,
            dump_rx: Some(rx),
            dump_finish_tx: finish_tx,
        };

        // Start the memtable cleaner
        table.start_cleaner(finish_rx);
        Ok(table)
    }

    // Take the dump request receiver
    pub fn take_dump_listener(&mut self) -> Option<UnboundedReceiver<DumpRequest>> {
        self.dump_rx.take()
    }

    // Get the value by key
    pub fn get(&self, key: &Bytes) -> Option<Value> {
        // Read mutable first
        if let Some(value) = self.mutable.lock().entries.get(key) {
            return Some(value.clone());
        }

        // Read from immutable list
        let immutable = self.immutable.read();
        for table in &*immutable {
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
            let (new_file, new_log_path) = open_new_log(&self.root_path, new_log_seq)?;
            let mut new_mutable = KVTable::new(new_log_path);
            new_mutable.log = Some(RecordWriter::new(new_file));

            // Swap mutable, close writer, and convert old mutable into immutable
            std::mem::swap(&mut *mutable, &mut new_mutable);
            // Close the file writer, ignore the synchronizing fail.
            new_mutable.log.take();

            let immutable = Arc::new(new_mutable);
            // Release immutable first, read requests see new empty mutable after new immutable first.
            // Previous write would not be lost on this read requests.
            self.immutable.write().push(Arc::clone(&immutable));

            // Send a dump request
            // Send operation would never fail
            let result = self
                .dump_tx
                .send(DumpRequest::new(immutable, self.dump_finish_tx.clone()));
            debug_assert!(result.is_ok());
        }

        Ok(())
    }

    fn start_cleaner(&self, mut receiver: UnboundedReceiver<PathBuf>) {
        let immutable = Arc::clone(&self.immutable);
        tokio::spawn(async move {
            // Clear log file and remove memtable if dump is finished
            while let Some(path) = receiver.recv().await {
                let mut tables = immutable.write();
                if let Some(first_table) = tables.first() {
                    if path == first_table.path {
                        // TODO: remove should be log
                        fs::remove_file(path).unwrap();
                        tables.remove(0);
                    }
                }
            }
        });
    }
}

// Ask to do a dump operation for a slice of memtable
pub struct DumpRequest {
    table: Arc<KVTable>,
    tx: UnboundedSender<PathBuf>,
}

impl DumpRequest {
    fn new(table: Arc<KVTable>, tx: UnboundedSender<PathBuf>) -> Self {
        DumpRequest { table, tx }
    }

    // Consume the request
    fn ok(self) {
        self.tx.send(self.table.path.clone());
    }

    // Get iterator of the table
    fn iter(&self) -> impl Iterator<Item = (&Bytes, &Value)> {
        self.table.entries.iter()
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
    value.encode_to(&mut buffer);

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
struct KVTable {
    entries: BTreeMap<Bytes, Value>,
    log: Option<RecordWriter<File>>,
    path: PathBuf,
}

impl KVTable {
    // Create a new memtable
    fn new(path: PathBuf) -> KVTable {
        KVTable {
            entries: BTreeMap::default(),
            log: None,
            path,
        }
    }
}

#[cfg(test)]
mod tests {
    use core::panic;

    use super::*;
    use crate::util::generate_random_bytes;

    #[test]
    fn test_log_with_random_case() {
        let test_count = 100;

        let test_case_key = generate_random_bytes(test_count, 10000);
        let test_case_value = generate_random_bytes(test_count, 10 * 32 * 1024);

        // Test normal value
        let memtable = MemTable::open("./data").unwrap();
        for (key, value) in test_case_key.iter().zip(test_case_value.iter()) {
            memtable
                .set(key.clone(), Value::living(value.clone()))
                .unwrap();
        }

        drop(memtable);
        let memtable = MemTable::open("./data").unwrap();
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
        let mut memtable = MemTable::open("./data").unwrap();
        for ((key, value), is_deleted) in test_case_key
            .iter()
            .zip(test_case_value.iter())
            .zip(test_case_deleted.iter())
        {
            if is_deleted.first().unwrap() % 2 == 0 {
                assert!(matches!(memtable.get(key).unwrap(), Value::Tombstone));
            } else {
                if let Some(Value::Living(v)) = memtable.get(key) {
                    assert_eq!(value, &v);
                } else {
                    panic!("value not found or not match");
                }
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
                    .set(key.clone(), Value::living(value.clone()))
                    .unwrap();
            }
        }

        drop(memtable);
        let memtable = MemTable::open("./data").unwrap();
        for (key, value) in test_case_key.iter().zip(test_case_value.iter()) {
            if let Some(Value::Living(v)) = memtable.get(key) {
                assert_eq!(value, &v);
            } else {
                panic!("value not found or not match");
            }
        }
    }
}
