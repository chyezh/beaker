use super::{
    block::{Block, BlockBuilder, BlockIntoIterator},
    util::Value,
    Error, Result,
};
use crate::util::{async_util::seek_and_read_buf, checksum, from_le_bytes_32, from_le_bytes_64};
use bytes::Bytes;
use lazy_static::lazy_static;
use parking_lot::RwLock;
use std::{
    collections::{HashMap, HashSet},
    io::{SeekFrom, Write},
    path::PathBuf,
    sync::{
        atomic::{AtomicU64, AtomicU8, Ordering},
        Arc,
    },
    time::Instant,
};
use tokio::{
    fs::File,
    io::{AsyncRead, AsyncSeek, AsyncWrite, AsyncWriteExt},
    sync::Mutex,
};
use tracing::info;
use uuid::Uuid;

const FOOTER_SIZE: usize = 12; // 12bytes
const BLOCK_DEFAULT_SIZE: usize = 4 * 1024; // 4kb
pub const SSTABLE_FILE_EXTENSION: &str = "tbl";
const MINIMUM_SSTABLE_ENTRY_SIZE: usize = 28;

lazy_static! {
    // Global sstable manager
    pub static ref MANAGER: SSTableManager<File> = SSTableManager::new();

    static ref ORIGINAL_INSTANT: Instant = Instant::now();
}

pub async fn open(entry: Arc<SSTableEntry>) -> Result<Arc<SSTable<File>>> {
    MANAGER.open(entry).await
}

pub fn clear_inactive_readers(secs: u64) {
    MANAGER.clear_inactive_readers(secs);
}

pub fn is_active_uuid(uid: &Uuid) -> bool {
    MANAGER.is_active_uuid(uid)
}

#[derive(Debug, Default, Clone)]
pub struct SSTableRange {
    pub left: Bytes,
    pub right: Bytes,
}

// Describe range info of sstable
impl SSTableRange {
    // Get minimum contain two range
    pub fn get_minimum_contain(&mut self, other: &SSTableRange) {
        if self.is_overlap(other) {
            if self.left > other.left {
                self.left = other.left.clone()
            }
            if self.right < other.right {
                self.right = other.right.clone()
            }
        }
    }

    // Check if two range is overlapping
    pub fn is_overlap(&self, other: &SSTableRange) -> bool {
        !(self.right < other.left || self.left > other.right)
    }

    // Check if a key in this range
    pub fn in_range(&self, key: &[u8]) -> bool {
        key >= self.left && key <= self.right
    }

    // Order with given key
    pub fn order(&self, key: &[u8]) -> std::cmp::Ordering {
        if key < self.left {
            std::cmp::Ordering::Greater
        } else if key > self.right {
            std::cmp::Ordering::Less
        } else {
            std::cmp::Ordering::Equal
        }
    }
}

// SSTableEntry is the unique entry of single sstable file, hold by manifest, SSTable, SSTableStream by using reference count
// SSTableEntry would never modified if sstable file's construction was finished
// When SSTableEntry is dropped except termination of system, no more read operation would do on sstable file and ssTable file can be removed safely
#[derive(Debug)]
pub struct SSTableEntry {
    pub lv: usize,
    pub uid: Uuid,
    pub range: SSTableRange,
    pub root_path: PathBuf,
    compact_lock: AtomicU8,
}

impl SSTableEntry {
    // Create a new SSTableEntry
    pub fn new(lv: usize, root_path: PathBuf) -> Self {
        let uid = Uuid::new_v4();
        // Register uid into global manager
        MANAGER.register_entry(uid);

        SSTableEntry {
            lv,
            uid,
            root_path,
            range: SSTableRange::default(),
            compact_lock: AtomicU8::new(0),
        }
    }

    // Parse sstable entry from bytes
    pub fn decode_from_bytes(root_path: PathBuf, data: Bytes) -> Result<Self> {
        debug_assert!(data.len() >= MINIMUM_SSTABLE_ENTRY_SIZE);
        if data.len() < MINIMUM_SSTABLE_ENTRY_SIZE {
            return Err(Error::IllegalSSTableEntry);
        }

        let lv = from_le_bytes_32(&data[0..4]);
        let uid = Uuid::from_bytes_le((&data[4..20]).try_into().unwrap());

        // Parsing range
        let left_boundary_len = from_le_bytes_32(&data[20..24]);
        debug_assert!(data.len() >= MINIMUM_SSTABLE_ENTRY_SIZE + left_boundary_len);
        if data.len() < MINIMUM_SSTABLE_ENTRY_SIZE + left_boundary_len {
            return Err(Error::IllegalSSTableEntry);
        }
        let right_boundary_len_start = 24 + left_boundary_len;
        let left_boundary = data.slice(24..right_boundary_len_start);

        let right_boundary_len =
            from_le_bytes_32(&data[right_boundary_len_start..right_boundary_len_start + 4]);
        debug_assert_eq!(
            data.len(),
            MINIMUM_SSTABLE_ENTRY_SIZE + left_boundary_len + right_boundary_len
        );
        if data.len() != MINIMUM_SSTABLE_ENTRY_SIZE + left_boundary_len + right_boundary_len {
            return Err(Error::IllegalSSTableEntry);
        }
        let right_boundary = data
            .slice(right_boundary_len_start + 4..right_boundary_len_start + 4 + right_boundary_len);

        // Check if sstable file exist
        if !Self::file_path(root_path.clone(), &uid).is_file() {
            return Err(Error::IllegalSSTableEntry);
        }

        // Register uid into global manager
        MANAGER.register_entry(uid);
        Ok(SSTableEntry {
            lv,
            uid,
            root_path,
            compact_lock: AtomicU8::new(0),
            range: SSTableRange {
                left: left_boundary,
                right: right_boundary,
            },
        })
    }

    // Consume the entry and open a builder of this entry
    pub async fn open_builder(self) -> Result<SSTableBuilder<File>> {
        let path = Self::file_path(self.root_path.clone(), &self.uid);
        let new_file = tokio::fs::OpenOptions::new()
            .truncate(true)
            .create(true)
            .write(true)
            .open(path)
            .await?;

        Ok(SSTableBuilder {
            writer: new_file,
            data_block: BlockBuilder::new(),
            index_block: BlockBuilder::new(),
            last_key: Vec::with_capacity(128),
            offset: 0,
            complete_data_block_size: 0,
            range: (None, None),
            entry: self,
        })
    }

    // Get encode bytes length for pre-allocation
    #[inline]
    pub fn encode_bytes_len(&self) -> usize {
        MINIMUM_SSTABLE_ENTRY_SIZE + self.range.left.len() + self.range.right.len()
    }

    // Encode into bytes and write to given Write
    // 1. 0-4 lv
    // 2. 4-20 uuid
    // 3. length of range's left boundary
    // 4. left boundary
    // 5. length of range's right boundary
    // 6. right boundary
    pub fn encode_to<W: Write>(&self, mut w: W) -> Result<()> {
        w.write_all(&self.lv.to_le_bytes()[0..4])?;
        w.write_all(&self.uid.to_bytes_le())?;
        w.write_all(&self.range.left.len().to_le_bytes()[0..4])?;
        w.write_all(&self.range.left)?;
        w.write_all(&self.range.right.len().to_le_bytes()[0..4])?;
        w.write_all(&self.range.right)?;
        Ok(())
    }

    // Try to lock for compact process
    pub fn try_compact_lock(&self) -> Result<()> {
        // Just keep atomic, no memory barrier needed
        self.compact_lock
            .compare_exchange(0, 1, Ordering::AcqRel, Ordering::Acquire)
            .map_err(|_| Error::SSTableEntryLockFailed)?;
        Ok(())
    }

    // release compact lock
    pub fn release_lock(&self) {
        // release the lock
        self.compact_lock.store(0, Ordering::Release);
    }

    pub fn sort_key(&self) -> Bytes {
        self.range.left.clone()
    }

    pub fn file_path(root_path: PathBuf, uid: &Uuid) -> PathBuf {
        let file_name = format!("{}.{}", uid, SSTABLE_FILE_EXTENSION);
        root_path.join(file_name)
    }

    // Open a sstable reader for this file
    #[inline]
    async fn open_reader(&self) -> Result<File> {
        let path = Self::file_path(self.root_path.clone(), &self.uid);
        let new_file = File::open(path).await?;
        Ok(new_file)
    }

    // Set range into entry
    #[inline]
    fn set_range(&mut self, range: SSTableRange) {
        debug_assert!(range.left <= range.right);
        self.range = range;
    }
}

impl Drop for SSTableEntry {
    fn drop(&mut self) {
        println!("drop entry");
        MANAGER.drop_entry(&self.uid);
    }
}

pub struct SSTable<R: AsyncRead + AsyncSeek + Unpin> {
    index: Arc<Index>,
    reader: Arc<Mutex<R>>,
    entry: Arc<SSTableEntry>,
    create_time: Instant,
    last_access: AtomicU64,
}

impl SSTable<File> {
    // Open a new SSTable with given entry and Parsing index into memory
    // Hold the entry and unique reader util SSTable is dropped
    async fn open(entry: Arc<SSTableEntry>) -> Result<Self> {
        let mut reader = entry.open_reader().await?;

        // Recover footer from file
        let footer_buffer = seek_and_read_buf(
            &mut reader,
            SeekFrom::End(-(FOOTER_SIZE as i64)),
            FOOTER_SIZE,
        )
        .await?;
        let footer = Footer::from_bytes(&footer_buffer)?;

        // Read index
        let mut index_buffer = seek_and_read_buf(
            &mut reader,
            SeekFrom::Start(footer.index_offset),
            footer.index_size,
        )
        .await?;
        debug_assert_eq!(index_buffer.len(), footer.index_size);
        if index_buffer.len() != footer.index_size {
            return Err(Error::BadSSTable);
        }

        // Recover index
        debug_assert!(index_buffer.len() >= 4);
        let index_len = index_buffer.len();
        let check_sum_result = from_le_bytes_32(&index_buffer[index_len - 4..index_len]) as u32;
        if checksum(&index_buffer[0..index_len - 4]) != check_sum_result {
            return Err(Error::BadSSTable);
        }
        index_buffer.resize(index_len - 4, b'\x00');
        let index = Index::from_block(Block::from_bytes(index_buffer.into())?)?;

        // Parse key range of this table
        // Get first key
        debug_assert!(!index.index.is_empty());
        if index.index.is_empty() {
            return Err(Error::BadSSTable);
        }

        let first_block = index.index.first().ok_or(Error::BadSSTable)?;
        let block_buffer = seek_and_read_buf(
            &mut reader,
            SeekFrom::Start(first_block.offset),
            first_block.size,
        )
        .await?;
        debug_assert_eq!(block_buffer.len(), first_block.size);
        if block_buffer.len() != first_block.size {
            return Err(Error::BadSSTable);
        }

        Ok(SSTable {
            index: Arc::new(index),
            reader: Arc::new(Mutex::new(reader)),
            entry,
            create_time: Instant::now(),
            last_access: AtomicU64::new(0),
        })
    }

    // Open a new stream of this SSTable, any operation of stream is independent with this SSTable
    pub async fn open_stream(&self) -> Result<SSTableStream<File>> {
        // Open a independent reader for stream
        let reader = self.entry.open_reader().await?;
        Ok(SSTableStream {
            block: None,
            index: Arc::clone(&self.index),
            index_offset: 0,
            reader,
            stop: false,
            _entry: Arc::clone(&self.entry),
        })
    }
}

impl<R: AsyncRead + AsyncSeek + Unpin> SSTable<R> {
    // Get value in this sstable with given key
    pub async fn search(&self, key: &[u8]) -> Result<Option<Value>> {
        if !self.entry.range.in_range(key) {
            return Ok(None);
        }

        let search_index = self
            .index
            .index
            .binary_search_by_key(&key, |elem| &elem.key[..])
            .unwrap_or_else(|x| x);

        debug_assert!(search_index < self.index.index.len());

        let block_info = &self.index.index[search_index];

        // Acquire file reader lock
        let block_buffer = {
            let mut reader = self.reader.lock().await;
            seek_and_read_buf(
                &mut *reader,
                SeekFrom::Start(block_info.offset),
                block_info.size,
            )
            .await?
        };
        debug_assert_eq!(block_buffer.len(), block_info.size);
        if block_buffer.len() != block_info.size {
            return Err(Error::BadSSTable);
        }

        if let Some(value) = Block::from_bytes(block_buffer.into())?.search_key(key)? {
            return Ok(Some(Value::decode_from_bytes(value)?));
        }
        Ok(None)
    }

    // Refresh last access time
    #[inline]
    fn refresh_last_access(&self) {
        self.last_access.store(
            Instant::now().duration_since(self.create_time).as_secs(),
            Ordering::Release,
        );
    }

    // SSTable is active if someone access it one minute ago
    #[inline]
    fn is_active(&self, secs: u64) -> bool {
        let last_access = self.last_access.load(Ordering::Acquire);
        (Instant::now().duration_since(self.create_time).as_secs() - last_access) < secs
    }
}

// A async iterator iterating whole sstable key-value pair for compaction
pub struct SSTableStream<R: AsyncRead + AsyncSeek + Unpin> {
    block: Option<BlockIntoIterator>,
    index: Arc<Index>,
    index_offset: usize,
    reader: R,
    stop: bool,
    _entry: Arc<SSTableEntry>,
}

// Implement a Iterator to iterating the sstable
impl<R: AsyncRead + AsyncSeek + Unpin> SSTableStream<R> {
    pub async fn next(&mut self) -> Option<Result<(Bytes, Value)>> {
        if self.stop {
            return None;
        }

        loop {
            match &mut self.block {
                Some(block) => {
                    // Read the next kv
                    match block.next() {
                        Some(Ok((key, value))) => {
                            match Value::decode_from_bytes(value) {
                                Ok(v) => return Some(Ok((key, v))),
                                Err(e) => {
                                    // Unexpected error occurred
                                    // Stop iteration and return last error
                                    self.stop = true;
                                    return Some(Err(e));
                                }
                            }
                        }
                        Some(Err(e)) => {
                            // Unexpected error occurred
                            // Stop iteration and return last error
                            self.stop = true;
                            return Some(Err(e));
                        }
                        None => {
                            // Reach the end of block iteration
                            // Restart by index iteration
                            self.block = None;
                        }
                    }
                }
                None => {
                    // Read a new block
                    // Search index if block is none
                    if let Some(block_item) = self.index.index.get(self.index_offset) {
                        self.index_offset += 1;
                        // Read a new block
                        let block_buffer = seek_and_read_buf(
                            &mut self.reader,
                            SeekFrom::Start(block_item.offset),
                            block_item.size,
                        )
                        .await;
                        if let Err(e) = block_buffer {
                            // Unexpected error occurred
                            // Stop iteration
                            self.stop = true;
                            return Some(Err(e.into()));
                        }
                        let block_buffer = block_buffer.unwrap();

                        debug_assert_eq!(block_buffer.len(), block_item.size);
                        if block_buffer.len() != block_item.size {
                            self.stop = true;
                            return Some(Err(Error::BadSSTable));
                        }

                        let block = Block::from_bytes(block_buffer.into());
                        if let Err(e) = block {
                            // Unexpected error occurred
                            // Stop iteration
                            self.stop = true;
                            return Some(Err(e));
                        }

                        self.block = Some(block.unwrap().into_iter());
                    } else {
                        return None;
                    }
                }
            }
        }
    }
}

struct Footer {
    index_offset: u64, // Index block offset
    index_size: usize, // Index block size
}

impl Footer {
    // Create a new Footer
    fn new(index_offset: u64, index_size: usize) -> Self {
        Footer {
            index_offset,
            index_size,
        }
    }

    // Recover footer from bytes
    fn from_bytes(data: &[u8]) -> Result<Self> {
        debug_assert_eq!(data.len(), FOOTER_SIZE);
        if data.len() != FOOTER_SIZE {
            return Err(Error::BadSSTableBlockFooter);
        }
        let index_offset = from_le_bytes_64(&data[0..8]);
        let index_size = from_le_bytes_32(&data[8..12]);

        Ok(Footer {
            index_offset,
            index_size,
        })
    }

    // Convert Footer into bytes
    fn to_bytes(&self) -> [u8; FOOTER_SIZE] {
        let mut new_index_value: [u8; FOOTER_SIZE] = [0; FOOTER_SIZE];
        new_index_value[0..8].copy_from_slice(&self.index_offset.to_le_bytes()[0..8]);
        new_index_value[8..12].copy_from_slice(&self.index_size.to_le_bytes()[0..4]);

        new_index_value
    }
}

struct BlockItem {
    key: Bytes,
    offset: u64, // Data block offset in sstable file
    size: usize, // Data block size in sstable file
}

struct Index {
    index: Vec<BlockItem>, // In memory index
}

impl Index {
    fn from_block(b: Block) -> Result<Self> {
        // TODO: Optimize by saving block count into index file
        let mut index = Vec::with_capacity(128);

        for pair in b.iter() {
            let (key, value) = pair?;
            debug_assert_eq!(value.len(), 12);
            if value.len() != 12 {
                return Err(Error::BadSSTableBlock);
            }

            let data_offset = from_le_bytes_64(&value[0..8]);
            let data_size = from_le_bytes_32(&value[8..12]);

            index.push(BlockItem {
                key,
                offset: data_offset,
                size: data_size,
            });
        }

        Ok(Index { index })
    }
}

pub struct SSTableManager<R: AsyncRead + AsyncSeek + Unpin> {
    readers: Arc<RwLock<HashMap<Uuid, Arc<SSTable<R>>>>>,
    active_entry_uid: Arc<parking_lot::Mutex<HashSet<Uuid>>>,
    instant_reference: Instant,
}

impl SSTableManager<File> {
    // Open a sstable with a cache layer
    async fn open(&self, entry: Arc<SSTableEntry>) -> Result<Arc<SSTable<File>>> {
        // Check whether sstable is already open in cache
        {
            let tables = self.readers.read();
            if let Some(table) = tables.get(&entry.uid) {
                // Update last access time
                table.refresh_last_access();
                return Ok(Arc::clone(table));
            }
        }
        // If sstable is not found in cache, open sstable and add to cache
        let new_table = SSTable::open(Arc::clone(&entry)).await?;

        let mut tables = self.readers.write();
        let table = tables
            .entry(entry.uid)
            .or_insert_with(|| Arc::new(new_table));

        // Update last access time
        table.refresh_last_access();

        info!("new sstable reader was opened, uuid: {}", entry.uid);
        Ok(Arc::clone(table))
    }

    // Clear all inactive readers
    pub fn clear_inactive_readers(&self, secs: u64) {
        // Pre-find all inactive reader, avoid acquire write lock
        let mut uid_list = Vec::new();
        {
            let reader = self.readers.read();
            for (uid, sstable) in reader.iter() {
                if !sstable.is_active(secs) {
                    uid_list.push(*uid);
                }
            }
        }

        if uid_list.is_empty() {
            return;
        }

        // Remove all inactive reader
        // Inconsistency is allowed, path not hit cache in open function was executed in worst case
        let mut reader = self.readers.write();
        for uid in uid_list {
            info!("inactive sstable reader was remove, uuid: {}", uid);
            reader.remove(&uid);
        }
    }

    // Create a manager
    fn new() -> Self {
        SSTableManager {
            readers: Arc::new(RwLock::new(HashMap::new())),
            active_entry_uid: Arc::new(parking_lot::Mutex::new(HashSet::new())),
            instant_reference: Instant::now(),
        }
    }

    // Check if a uid is still active
    #[inline]
    fn is_active_uuid(&self, uid: &Uuid) -> bool {
        self.active_entry_uid.lock().contains(uid)
    }

    // Add entry uid if created
    #[inline]
    fn register_entry(&self, uid: Uuid) {
        self.active_entry_uid.lock().insert(uid);
    }

    // Remove entry uid if drop
    #[inline]
    fn drop_entry(&self, uid: &Uuid) {
        self.active_entry_uid.lock().remove(uid);
    }
}

// Build a single sstable, use leveldb definition without filter block.
pub struct SSTableBuilder<W> {
    writer: W,
    data_block: BlockBuilder,
    index_block: BlockBuilder,
    last_key: Vec<u8>,
    offset: u64,
    complete_data_block_size: usize,
    range: (Option<Bytes>, Option<Bytes>),
    entry: SSTableEntry,
}

impl<W: AsyncWrite + Unpin> SSTableBuilder<W> {
    // Add a new key to sstable and return size estimate
    pub async fn add(&mut self, key: Bytes, value: Value) -> Result<usize> {
        self.data_block.add(&key, &value.to_bytes());
        self.last_key.clear();
        self.last_key.extend_from_slice(&key);

        let block_size = self.data_block.size_estimate();
        let complete_data_block_size = self.complete_data_block_size;

        // Flush if block size is bigger enough, add a index
        if block_size > BLOCK_DEFAULT_SIZE {
            self.complete_data_block_size += block_size;
            let (offset, data_size) = self.flush_new_data_block().await?;
            self.add_new_index(offset, data_size);
        }

        // Update range
        if self.range.0.is_none() {
            self.range.0 = Some(key.clone());
        }
        self.range.1 = Some(key);

        // Return size estimate
        Ok(complete_data_block_size + block_size + self.index_block.size_estimate())
    }

    // Finish a table construction, and get the sstable entry
    pub async fn finish(mut self) -> Result<SSTableEntry> {
        debug_assert!(self.range.0.is_some());
        debug_assert!(self.range.1.is_some());

        // Append new data block if data_block is not empty
        if !self.data_block.is_empty() {
            let (offset, data_size) = self.flush_new_data_block().await?;
            self.add_new_index(offset, data_size)
        }

        // Append index block
        let data = self.index_block.finish();
        let index_offset = self.offset;
        let index_size = data.len() + 4; // add checksum extra

        self.writer.write_all(data).await?;
        self.writer
            .write_all(&checksum(data).to_le_bytes()[0..4])
            .await?;

        self.index_block.reset();
        self.offset += index_size as u64 + 4;

        // Append footer
        self.writer
            .write_all(&Footer::new(index_offset, index_size).to_bytes())
            .await?;

        // Set range into entry
        self.entry.set_range(SSTableRange {
            left: self.range.0.as_ref().unwrap().clone(),
            right: self.range.1.as_ref().unwrap().clone(),
        });

        Ok(self.entry)
    }

    // flush a new data block into underlying writer
    pub async fn flush_new_data_block(&mut self) -> Result<(u64, usize)> {
        let data = self.data_block.finish();
        let data_size = data.len();
        let offset = self.offset;

        // Write data and checksum, clear data block builder
        self.writer.write_all(data).await?;
        self.writer
            .write_all(&checksum(data).to_le_bytes()[0..4])
            .await?;
        self.data_block.reset();
        self.offset += data_size as u64 + 4; // advance offset(data_size + checksum_size)

        Ok((offset, data_size))
    }

    // Add new index into builder
    fn add_new_index(&mut self, offset: u64, data_size: usize) {
        let mut new_index_value: [u8; 12] = [0; 12];
        new_index_value[0..8].copy_from_slice(&offset.to_le_bytes()[0..8]);
        new_index_value[8..12].copy_from_slice(&data_size.to_le_bytes()[0..4]);

        self.index_block.add(&self.last_key, &new_index_value);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::util::test_case::generate_random_bytes;
    use tokio::test;

    #[test]
    async fn test_sstable_builder_and_stream() {
        let _ = env_logger::builder().is_test(true).try_init();

        let test_count = 10000;
        let mut test_case_key = generate_random_bytes(test_count, 1000);
        let test_case_value = generate_random_bytes(test_count, 10 * 32 * 1024);
        test_case_key.sort();
        std::fs::create_dir_all("./data").unwrap();

        let mut uid: Option<Uuid> = None;
        {
            let entry = SSTableEntry::new(0, "./data".into());
            uid = Some(entry.uid);
            // Check MANAGER
            assert!(is_active_uuid(&uid.unwrap()));
            // Test build
            let mut builder = entry.open_builder().await.unwrap();
            for (key, value) in test_case_key.iter().zip(test_case_value.iter()) {
                builder
                    .add(key.clone(), Value::Living(value.clone()))
                    .await
                    .unwrap();
            }
            let entry = Arc::new(builder.finish().await.unwrap());

            // Open sstable
            let sstable = open(entry).await.unwrap();

            // Test search
            for (key, value) in test_case_key.iter().zip(test_case_value.iter()) {
                assert_eq!(
                    sstable.search(key).await.unwrap().unwrap(),
                    Value::Living(value.clone()),
                );
            }

            // Test stream
            let mut s = sstable.open_stream().await.unwrap();
            let mut offset = 0;
            while let Some(item) = s.next().await {
                let (key, value) = item.unwrap();
                assert_eq!(key, test_case_key[offset]);
                assert_eq!(value, Value::living(test_case_value[offset].clone()));
                offset += 1;
            }

            // Drop all access entry
            clear_inactive_readers(0);
        }

        // Check MANAGER
        assert!(!is_active_uuid(&uid.unwrap()));
    }
}
