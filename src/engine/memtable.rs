use super::{
    config::{Config, Initial},
    event::Event,
    kvtable::KVTable,
    record::{RecordReader, RecordWriter},
    util::{async_scan_file_at_path, scan_sorted_file_at_path, Value},
    Error, Result,
};
use crate::util::{from_le_bytes_32, from_le_bytes_64};
use bytes::Bytes;
use futures_core::Future;
use parking_lot::{Mutex, RwLock};
use std::{
    fs::{self, File, OpenOptions},
    io::{BufWriter, Seek, SeekFrom},
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
};
use tracing::info;

const KV_HEADER: usize = 16; // log_id + key_len + 8+4
const SPLIT_LOG_SIZE_THRESHOLD: usize = 4 * 1024 * 1024; // 4M
const LOG_FILE_EXTENSION: &str = "log";

#[derive(Clone)]
// Implement a memtable with persist write-ahead-log file
pub struct MemTable {
    mutable: Arc<Mutex<(KVTable, RecordWriter<File>)>>,
    immutable: Arc<RwLock<Vec<Arc<KVTable>>>>,
    next_log_seq: Arc<AtomicU64>,
    dump_mutex: Arc<tokio::sync::Mutex<()>>,
    config: Config,
    initial: Initial,
}

impl MemTable {
    /// Recover memtable from log until max_log_id.
    pub fn open(config: Config, initial: Initial) -> Result<Self> {
        // Try to create log root directory at this path.
        fs::create_dir_all(config.log_path())?;

        // Scan the root_path directory, find all log sorted by sequence number
        let log_files = scan_sorted_file_at_path(&config.log_path(), LOG_FILE_EXTENSION)?;

        let mut last_log_id = 0;

        // Recover immutables
        let (immutable, mut last_log_seq) = if !log_files.is_empty() {
            let mut tables = Vec::with_capacity(log_files.len());

            // Recover immutable memtable
            for (path, seq_id) in log_files.iter() {
                match recover_log(path, *seq_id, last_log_id, initial.max_log_id)? {
                    (Some(table), None) => {
                        // Old tables never write new entry
                        last_log_id = table.log_id_range().1;
                        let immutable_table = Arc::new(table);
                        tables.push(Arc::clone(&immutable_table));
                    }
                    (Some(table), Some(writer)) => {
                        // Recover part of log, and continue write log after the part.
                        return Ok(MemTable {
                            mutable: Arc::new(Mutex::new((table, writer))),
                            immutable: Arc::new(RwLock::new(tables)),
                            next_log_seq: Arc::new(AtomicU64::new(*seq_id + 1)),
                            dump_mutex: Arc::new(tokio::sync::Mutex::new(())),
                            config,
                            initial,
                        });
                    }
                    _ => continue,
                }
            }
            (tables, log_files.last().unwrap().1)
        } else {
            (Vec::new(), 0)
        };

        // Create new mutable to write
        last_log_seq += 1;
        last_log_id += 1;
        let mutable = open_new_log(&config.log_path(), last_log_seq, last_log_id)?;

        // Try to dump old immutable memtable
        if !immutable.is_empty() {
            let send_result = initial.event_sender.notify(Event::Dump);
            debug_assert!(send_result.is_ok());
        }

        Ok(MemTable {
            mutable: Arc::new(Mutex::new(mutable)),
            immutable: Arc::new(RwLock::new(immutable)),
            next_log_seq: Arc::new(AtomicU64::new(last_log_seq + 1)),
            dump_mutex: Arc::new(tokio::sync::Mutex::new(())),
            config,
            initial,
        })
    }

    // Get the value by key
    pub fn get(&self, key: &[u8]) -> Option<Value> {
        // Read mutable first
        if let Some(value) = self.mutable.lock().0.get(key) {
            return Some(value);
        }

        // Read from immutable list
        let immutable = self.immutable.read();
        for table in immutable.iter().rev() {
            if let Some(value) = table.get(key) {
                return Some(value);
            }
        }
        None
    }

    // Set the key-value pair into mutable
    pub fn set(&self, key: Bytes, value: Value) -> Result<u64> {
        let mut mutable = self.mutable.lock();

        let log_id = mutable.0.next_log_id();

        // Write kv pair into log ahead
        let total_written = mutable.1.append(encode_kv(log_id, &key, &value))?;

        // Set value into in-memory table
        let log_id = mutable.0.set(key, value);

        // Switch new log if needed
        // Low-rate route
        if total_written > SPLIT_LOG_SIZE_THRESHOLD {
            let new_log_seq = self.next_log_seq.fetch_add(1, Ordering::Relaxed);

            // Create a new writer
            let mut new_mutable = open_new_log(
                &self.config.log_path(),
                new_log_seq,
                mutable.0.next_log_id(),
            )?;

            // Swap mutable, close writer, and convert old mutable into immutable
            std::mem::swap(&mut *mutable, &mut new_mutable);

            // Close the file writer, ignore the synchronizing fail.
            let immutable = Arc::new(new_mutable.0);
            // Release immutable first, read requests see new empty mutable after new immutable.
            // Previous write would not be lost on this read requests.
            self.immutable.write().push(Arc::clone(&immutable));

            // Send a dump request
            // Send operation would never fail
            let result = self.initial.event_sender.notify(Event::Dump);
            debug_assert!(result.is_ok());
        }

        Ok(log_id)
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
            if let Err(err) = self.initial.event_sender.notify(Event::InactiveLogClean) {
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
        let filenames =
            async_scan_file_at_path(&self.config.log_path(), LOG_FILE_EXTENSION).await?;

        // at least one mutable memtable
        if filenames.len() <= 1 {
            return Ok(());
        }

        // Get minimal immutable seq id
        // It's safe to get mutable seq first, seq is monotonic.
        let mut minimal_seq = self.mutable.lock().0.seq_id();
        {
            // It's safe to clean all log except mutable if immutable is empty
            if let Some(table) = self.immutable.read().first() {
                minimal_seq = table.seq_id();
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

/// Recover a table from given log sequence id
///
/// Return KVTable if table is not empty.
///
/// Return RecordWriter if recover operation reaches the target_log_id,
/// RecordWriter will continue append new log after the log of target_log_id,
///
/// Return Err(TargetLogIdConsumed) if given target log id was unrecoverable,
/// because of sstable dump operation.
fn recover_log(
    path: &Path,
    seq_id: u64,
    mut last_log_id: u64,
    target_log_id: Option<u64>,
) -> Result<(Option<KVTable>, Option<RecordWriter<File>>)> {
    let mut reader = RecordReader::new(File::open(path)?);

    if let Some(data) = reader.next() {
        let (log_id, key, value) = decode_kv(data?)?;

        // Update and check last log id, log_id should be keep increasing step by one
        last_log_id = update_last_log_id(last_log_id, log_id)?;

        // Check if target_log_id was consumed
        if matches!(target_log_id, Some(id) if id < log_id) {
            return Err(Error::TargetLogIdConsumed);
        }

        // Initialize kvtable at first iteration
        let mut table = KVTable::new(seq_id, last_log_id);
        table.set(key, value);

        // Reach the target log id, return sstable and log writer at this record point
        if Some(log_id) == target_log_id {
            let (block_offset, total_offset) = reader.offset()?;
            let writer = open_exist_log_at_offset(path, block_offset, total_offset)?;
            return Ok((Some(table), Some(writer)));
        }

        // Build new kvtable from reader
        while let Some(data) = reader.next() {
            let (log_id, key, value) = decode_kv(data?)?;
            last_log_id = update_last_log_id(last_log_id, log_id)?;

            table.set(key, value);

            debug_assert_eq!(last_log_id, table.log_id_range().1);
            debug_assert!(
                target_log_id.is_none() || matches!(target_log_id, Some(id) if log_id <= id)
            );

            // Reach the target log id, return sstable and log writer at this record point
            if Some(log_id) == target_log_id {
                let (block_offset, total_offset) = reader.offset()?;
                let writer = open_exist_log_at_offset(path, block_offset, total_offset)?;
                return Ok((Some(table), Some(writer)));
            }
        }

        return Ok((Some(table), None));
    }

    Ok((None, None))
}

/// Check last log id, log_id should be keep increasing step by one
fn update_last_log_id(mut last_log_id: u64, log_id: u64) -> Result<u64> {
    if last_log_id == 0 {
        // Initialize last log id if last log id is not set up
        last_log_id = log_id;
    } else {
        last_log_id += 1;
    }

    if last_log_id != log_id {
        return Err(Error::InconsistentLogId);
    }

    Ok(last_log_id)
}

// Encode the KV pair into binary format. While no checksum is applied on here, record writer downside has promised. Layout:
// 8 log_id
// 4 key_len
// key
// Value::write_to
fn encode_kv(log_id: u64, key: &[u8], value: &Value) -> Bytes {
    // Key length + key + value
    let mut buffer = Vec::with_capacity(KV_HEADER + key.len() + value.encode_bytes_len());

    buffer.extend_from_slice(&log_id.to_le_bytes()[0..8]);
    buffer.extend_from_slice(&key.len().to_le_bytes()[0..4]);
    buffer.extend_from_slice(&value.encode_bytes_len().to_le_bytes()[0..4]);
    buffer.extend(key);
    // IO always success on Vec, ignore error
    value.encode_to(&mut buffer).ok();

    buffer.into()
}

// Decode the KV Pair from binary format
fn decode_kv(data: Bytes) -> Result<(u64, Bytes, Value)> {
    debug_assert!(data.len() >= KV_HEADER);
    if data.len() < KV_HEADER {
        return Err(Error::IllegalLog);
    }

    // Parse header
    let log_id = from_le_bytes_64(&data[0..8]);
    let key_length = from_le_bytes_32(&data[8..12]);
    let value_offset = KV_HEADER + key_length;

    // Parse key part
    let key = data.slice(KV_HEADER..value_offset);
    // Parse value part
    let value = Value::decode_from_bytes(data.slice(value_offset..))?;
    Ok((log_id, key, value))
}

/// Open a new log with given log sequence number
fn open_new_log(dir: &Path, log_seq: u64, log_id: u64) -> Result<(KVTable, RecordWriter<File>)> {
    let new_log_path = log_path(dir, log_seq);

    let new_file = OpenOptions::new()
        .truncate(true)
        .create(true)
        .write(true)
        .open(new_log_path)?;

    let table = KVTable::new(log_seq, log_id);
    Ok((table, RecordWriter::new(new_file)))
}

/// Open exist log at given seek offset from beginning.
fn open_exist_log_at_offset(
    log_path: &Path,
    block_offset: usize,
    total_offset: usize,
) -> Result<RecordWriter<File>> {
    let mut new_file = OpenOptions::new().write(true).open(log_path)?;
    // Truncate the file and seek to offset
    new_file.seek(SeekFrom::Start(total_offset as u64))?;
    new_file.set_len(total_offset as u64)?;

    RecordWriter::recover(new_file, block_offset, total_offset)
}

#[inline]
fn log_path(dir: &Path, log_seq: u64) -> PathBuf {
    dir.join(format!("{}.{}", log_seq, LOG_FILE_EXTENSION))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::util::test_case::{generate_random_bytes, generate_step_by_bytes};
    use core::panic;
    use tempfile::tempdir;

    #[test]
    fn test_log_part_log_recover() {
        let temp_dir = tempdir().unwrap();
        let root_path = temp_dir.path();

        let test_count = 10000;
        let test_case_key = generate_step_by_bytes(test_count);
        let test_case_value = generate_random_bytes(test_count, 32 * 10 * 1024);

        let config = Config::default_config_with_path(root_path.to_path_buf());
        let (mut initial, _event_builder) = Initial::new();

        // Create case
        let memtable = MemTable::open(config.clone(), initial.clone()).unwrap();
        for (key, value) in test_case_key.iter().zip(test_case_value.iter()) {
            memtable
                .set(key.clone(), Value::Living(value.clone()))
                .unwrap();
        }
        drop(memtable);

        // Recover part twice
        for recover_count in [1, test_count / 3, test_count / 2, test_count] {
            // Recover half of test
            initial.max_log_id = Some(recover_count as u64);
            let memtable = MemTable::open(config.clone(), initial.clone()).unwrap();
            for (offset, (key, value)) in
                test_case_key.iter().zip(test_case_value.iter()).enumerate()
            {
                let v = memtable.get(key);
                if offset < recover_count {
                    assert_eq!(v, Some(Value::Living(value.clone())));
                } else {
                    assert_eq!(v, None);
                }
            }

            // Continue to set rest kv pair
            for (key, value) in test_case_key
                .iter()
                .zip(test_case_value.iter())
                .skip(recover_count)
            {
                memtable
                    .set(key.clone(), Value::Living(value.clone()))
                    .unwrap();
            }
            for (key, value) in test_case_key.iter().zip(test_case_value.iter()) {
                let v = memtable.get(key);
                assert_eq!(v, Some(Value::Living(value.clone())));
            }
        }
    }

    #[test]
    fn test_log_with_random_case() {
        let temp_dir = tempdir().unwrap();
        let root_path = temp_dir.path();
        let test_count = 1000;

        let test_case_key = generate_step_by_bytes(test_count);
        let test_case_value = generate_random_bytes(test_count, 32 * 10 * 1024);

        let config = Config::default_config_with_path(root_path.to_path_buf());
        let (initial, _event_builder) = Initial::new();

        // Test normal value
        let memtable = MemTable::open(config.clone(), initial.clone()).unwrap();
        for (key, value) in test_case_key.iter().zip(test_case_value.iter()) {
            memtable
                .set(key.clone(), Value::Living(value.clone()))
                .unwrap();
        }

        drop(memtable);
        let memtable = MemTable::open(config.clone(), initial.clone()).unwrap();
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
        let memtable = MemTable::open(config.clone(), initial.clone()).unwrap();
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
        let memtable = MemTable::open(config, initial).unwrap();
        for (key, value) in test_case_key.iter().zip(test_case_value.iter()) {
            if let Some(Value::Living(v)) = memtable.get(key) {
                assert_eq!(value, &v);
            } else {
                panic!("value not found or not match");
            }
        }
    }
}
