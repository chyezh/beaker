use crate::util::from_le_bytes_32;

use super::{
    log::{RecordReader, RecordWriter},
    Error, Key, Result, Value,
};
use std::collections::BTreeMap;
use std::fs::{self, File, OpenOptions};
use std::path::{Path, PathBuf};

const KV_LIVING_VALUE_TYPE: u8 = 1;
const KV_TOMBSTONE_VALUE_TYPE: u8 = 0;
const KV_LIVING_VALUE_HEADER: usize = 1 + 4 + 4; // value_type + key_length + value_length
const KV_TOMBSTONE_VALUE_HEADER: usize = 1 + 4; // value_type + key_length
const SPLIT_LOG_SIZE_THRESHOLD: usize = 4 * 1024 * 1024; // 4M
const LOG_FILE_EXTENSION: &str = "log";

// Implement a memtable with persist write-ahead-log file
pub struct PersistentMemTable {
    // Sequence of memtable and corresponding record writer
    tables: Vec<(MemTable, Option<RecordWriter<File>>)>,
    // persistent directory root path
    root_path: PathBuf,
    // next new log to write
    next_log_seq: u64,
}

impl PersistentMemTable {
    // Create or recover memtable with given persistent directory root path
    pub fn create_or_recover(root_path: impl Into<PathBuf>) -> Result<Self> {
        // Try to create log root directory at this path.
        let root_path: PathBuf = root_path.into();
        fs::create_dir_all(root_path.clone())?;

        // Scan the root_path directory, find all log sorted by sequence number
        let log_files = scan_sorted_file_at_path(&root_path)?;

        let (tables, next_log_seq) = if !log_files.is_empty() {
            // Read the log, recover the memtable
            let mut tables: Vec<(MemTable, Option<RecordWriter<File>>)> =
                Vec::with_capacity(1 + log_files.len());
            for (path, _) in log_files.iter() {
                let reader = RecordReader::new(File::open(path)?);
                let mut table = MemTable::new();
                for data in reader {
                    let (key, value) = decode_kv(data?)?;
                    table.set(key, value);
                }
                // Old tables never write new entry
                tables.push((table, None));
            }

            (tables, log_files.last().unwrap().1 + 1)
        } else {
            // Empty log, set the initial params
            let tables: Vec<(MemTable, Option<RecordWriter<File>>)> = Vec::with_capacity(2);
            (tables, 0)
        };
        let mut table = PersistentMemTable {
            tables,
            root_path,
            next_log_seq,
        };

        // Create new log to write
        table.open_new_log()?;

        Ok(table)
    }

    // Find a living value by key from tables sequentially.
    // Return None immediately if any value is a tombstone
    pub fn get(&self, key: &Key) -> Option<Vec<u8>> {
        debug_assert!(!self.tables.is_empty() && !key.is_empty());
        for (table, _) in self.tables.iter().rev() {
            match table.get(key) {
                Some(Value::Living(v)) => return Some(v),
                Some(Value::Tombstone) => return None,
                None => {}
            }
        }
        None
    }

    // Write record to last persistent log, set a value to the last table
    pub fn set(&mut self, key: Key, value: Value) -> Result<()> {
        debug_assert!(!self.tables.is_empty() && !key.is_empty());
        // Get the last log and corresponding table
        let (table, log_writer) = self.tables.last_mut().unwrap();
        debug_assert!(log_writer.is_some());
        let log_writer = log_writer.as_mut().unwrap();

        // Write kv pair into log
        log_writer.append(encode_kv(&key, &value))?;
        // Then set it to memory table
        table.set(key, value);

        // split log, creator a new log to write
        if log_writer.written() > SPLIT_LOG_SIZE_THRESHOLD {
            self.open_new_log()?;
        }

        Ok(())
    }

    // Open new log and increment log sequence number
    fn open_new_log(&mut self) -> Result<()> {
        let new_log_path = log_path(&self.root_path, self.next_log_seq);
        let new_file = OpenOptions::new()
            .create(true)
            .write(true)
            .open(new_log_path)?;

        self.next_log_seq += 1;
        self.tables
            .push((MemTable::new(), Some(RecordWriter::new(new_file))));

        Ok(())
    }
}

// Generate log path
fn log_path(dir: &Path, log_seq: u64) -> PathBuf {
    dir.join(format!("{}.{}", log_seq, LOG_FILE_EXTENSION))
}

// Scan given log root directory and get all sorted log file
fn scan_sorted_file_at_path(path: &Path) -> Result<Vec<(PathBuf, u64)>> {
    // example of log file name format: 1.log 2.log
    let mut filenames: Vec<_> = fs::read_dir(path)?
        .flat_map(|elem| -> Result<PathBuf> { Ok(elem?.path()) }) // filter read_dir failed paths
        .filter_map(|elem| {
            if !elem.is_file() || elem.extension() != Some(LOG_FILE_EXTENSION.as_ref()) {
                None
            } else if let Some(Ok(log_num)) = elem
                .file_stem()
                .and_then(std::ffi::OsStr::to_str)
                .map(str::parse::<u64>)
            {
                Some((elem, log_num))
            } else {
                None
            }
        }) // filter illegal filename and get log seq no
        .collect();

    // sort filenames with log seq no
    filenames.sort_by_key(|elem| elem.1);
    Ok(filenames)
}

// Encode the KV pair into binary format. While no checksum is applied on here, record writer downside has promised. Layout:
// Value::Living
// 1. 0-1 value_type
// 2. 1-5 key_len
// 3. 5-9 value_len
// 4. key
// 5. value
// Value::Tombstone
// 1. 0-1 value_type
// 2. 1-5 key_len
// 3. key
fn encode_kv(key: &Key, value: &Value) -> Vec<u8> {
    match value {
        Value::Living(value) => {
            let mut v = Vec::with_capacity(key.len() + value.len() + KV_LIVING_VALUE_HEADER);
            // Write value_type
            v.push(KV_LIVING_VALUE_TYPE);
            // Write length
            v.extend_from_slice(&key.len().to_le_bytes()[0..4]);
            v.extend_from_slice(&value.len().to_le_bytes()[0..4]);
            // Write data
            v.extend_from_slice(key);
            v.extend_from_slice(value);
            v
        }
        Value::Tombstone => {
            let mut v = Vec::with_capacity(key.len() + KV_TOMBSTONE_VALUE_HEADER);
            v.push(KV_TOMBSTONE_VALUE_TYPE);
            v.extend_from_slice(&key.len().to_le_bytes()[0..4]);
            v.extend_from_slice(key);
            v
        }
    }
}

// Decode the KV pair from binary format.
fn decode_kv(v: Vec<u8>) -> Result<(Key, Value)> {
    debug_assert!(v.len() > KV_LIVING_VALUE_HEADER || v.len() > KV_TOMBSTONE_VALUE_HEADER);
    if v.is_empty() {
        return Err(Error::IllegalLog);
    }

    match v[0] {
        KV_LIVING_VALUE_TYPE => {
            if v.len() < KV_LIVING_VALUE_HEADER {
                return Err(Error::IllegalLog);
            }
            let key_length = from_le_bytes_32(&v[1..5]);
            let value_length = from_le_bytes_32(&v[5..9]);
            if v.len() != key_length + value_length + KV_LIVING_VALUE_HEADER {
                return Err(Error::IllegalLog);
            }
            Ok((
                (&v[KV_LIVING_VALUE_HEADER..KV_LIVING_VALUE_HEADER + key_length]).to_vec(),
                Value::Living(Vec::from(
                    &v[KV_LIVING_VALUE_HEADER + key_length
                        ..KV_LIVING_VALUE_HEADER + key_length + value_length],
                )),
            ))
        }
        KV_TOMBSTONE_VALUE_TYPE => {
            if v.len() < KV_TOMBSTONE_VALUE_HEADER {
                return Err(Error::IllegalLog);
            }
            let key_length = from_le_bytes_32(&v[1..5]);
            if v.len() != key_length + KV_TOMBSTONE_VALUE_HEADER {
                return Err(Error::IllegalLog);
            }
            Ok((
                (&v[KV_TOMBSTONE_VALUE_HEADER..KV_TOMBSTONE_VALUE_HEADER + key_length]).to_vec(),
                Value::Tombstone,
            ))
        }
        _ => Err(Error::IllegalLog),
    }
}

// Implement memtable
// TODO: replace by skip list in future
pub struct MemTable {
    entries: BTreeMap<Key, Value>,
}

impl MemTable {
    // Create a new memtable
    fn new() -> MemTable {
        MemTable {
            entries: BTreeMap::default(),
        }
    }

    // Find a value by key from memtable
    fn get(&self, key: &Key) -> Option<Value> {
        debug_assert!(!key.is_empty());
        self.entries.get(key).cloned()
    }

    // Set a value by key
    fn set(&mut self, key: Key, value: Value) {
        debug_assert!(!key.is_empty());
        self.entries.insert(key, value);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::util::generate_random_bytes_vec;

    #[test]
    fn test_log_with_random_case() {
        let test_count = 100;

        let test_case_key = generate_random_bytes_vec(test_count, 10000);
        let test_case_value = generate_random_bytes_vec(test_count, 10 * 32 * 1024);

        // Test normal value
        let mut memtable = PersistentMemTable::create_or_recover("./data").unwrap();
        for (key, value) in test_case_key.iter().zip(test_case_value.iter()) {
            memtable
                .set(key.to_vec(), Value::Living(value.to_vec()))
                .unwrap();
        }

        drop(memtable);
        let mut memtable = PersistentMemTable::create_or_recover("./data").unwrap();
        for (key, value) in test_case_key.iter().zip(test_case_value.iter()) {
            assert_eq!(memtable.get(key).unwrap(), value[..]);
        }

        // Test deleted value
        let test_case_deleted = generate_random_bytes_vec(test_count, 2);
        for (key, is_deleted) in test_case_key.iter().zip(test_case_deleted.iter()) {
            if is_deleted.first().unwrap() % 2 == 0 {
                memtable.set(key.to_vec(), Value::Tombstone).unwrap();
            }
        }

        drop(memtable);
        let mut memtable = PersistentMemTable::create_or_recover("./data").unwrap();
        for ((key, value), is_deleted) in test_case_key
            .iter()
            .zip(test_case_value.iter())
            .zip(test_case_deleted.iter())
        {
            if is_deleted.first().unwrap() % 2 == 0 {
                assert!(memtable.get(key).is_none())
            } else {
                assert_eq!(memtable.get(key).unwrap(), value[..]);
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
                    .set(key.to_vec(), Value::Living(value.to_vec()))
                    .unwrap();
            }
        }

        drop(memtable);
        let memtable = PersistentMemTable::create_or_recover("./data").unwrap();
        for (key, value) in test_case_key.iter().zip(test_case_value.iter()) {
            assert_eq!(memtable.get(key).unwrap(), value[..]);
        }
    }
}
