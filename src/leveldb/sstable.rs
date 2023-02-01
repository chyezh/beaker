use bytes::Bytes;

use crate::util::{checksum, from_le_bytes_32, from_le_bytes_64, seek_and_read_buf};

use super::{
    block::{Block, BlockBuilder, BlockIntoIterator},
    util::Value,
    Error, Result,
};
use std::{
    io::{Read, Seek, SeekFrom, Write},
    sync::{Arc, Mutex},
};

const FOOTER_SIZE: usize = 12; // 12bytes
const BLOCK_DEFAULT_SIZE: usize = 4 * 1024; // 4kb

#[derive(Debug, Default, Clone)]
pub struct SSTableRange {
    pub left: Bytes,
    pub right: Bytes,
}

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

pub struct SSTable<R: Seek + Read> {
    index: Index,
    reader: Arc<Mutex<Option<R>>>,
    range: SSTableRange, // Key range of this table [xxx, xxx]
}

impl<R: Seek + Read> SSTable<R> {
    pub fn open(mut reader: R) -> Result<Self> {
        // Recover footer from file
        let footer_buffer = seek_and_read_buf(
            &mut reader,
            SeekFrom::End(-(FOOTER_SIZE as i64)),
            FOOTER_SIZE,
        )?;
        let footer = Footer::from_bytes(&footer_buffer)?;

        // Read index
        let mut index_buffer = seek_and_read_buf(
            &mut reader,
            SeekFrom::Start(footer.index_offset),
            footer.index_size,
        )?;
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
        )?;
        debug_assert_eq!(block_buffer.len(), first_block.size);
        if block_buffer.len() != first_block.size {
            return Err(Error::BadSSTable);
        }

        let first_block = Block::from_bytes(block_buffer.into())?;
        let (first_key, _) = first_block.iter().next().ok_or(Error::BadSSTable)??;
        // Get last key
        let last_block = if index.index.len() == 1 {
            // Single block in index
            first_block
        } else {
            let last_block = index.index.last().ok_or(Error::BadSSTable)?;
            let block_buffer = seek_and_read_buf(
                &mut reader,
                SeekFrom::Start(last_block.offset),
                last_block.size,
            )?;
            debug_assert_eq!(block_buffer.len(), last_block.size);
            if block_buffer.len() != last_block.size {
                return Err(Error::BadSSTable);
            }
            Block::from_bytes(block_buffer.into())?
        };

        let mut last_key = Bytes::new();
        for pair in last_block.iter() {
            last_key = pair?.0;
        }

        Ok(SSTable {
            index,
            reader: Arc::new(Mutex::new(Some(reader))),
            range: SSTableRange {
                left: first_key,
                right: last_key,
            },
        })
    }

    pub fn search(&self, key: &[u8]) -> Result<Option<Value>> {
        debug_assert!(self.reader.lock().unwrap().is_some());
        if !self.range.in_range(key) {
            return Ok(None);
        }

        let search_index = self
            .index
            .index
            .binary_search_by_key(&key, |elem| &elem.key[..])
            .unwrap_or_else(|x| x);
        debug_assert!(search_index < self.index.index.len());

        let block_info = &self.index.index[search_index];
        let block_buffer = seek_and_read_buf(
            (*self.reader.lock().unwrap()).as_mut().unwrap(),
            SeekFrom::Start(block_info.offset),
            block_info.size,
        )?;
        debug_assert_eq!(block_buffer.len(), block_info.size);
        if block_buffer.len() != block_info.size {
            return Err(Error::BadSSTable);
        }

        if let Some(value) = Block::from_bytes(block_buffer.into())?.search_key(key)? {
            return Ok(Some(Value::decode_from_bytes(value)?));
        }
        Ok(None)
    }
}

impl<R: Read + Seek> IntoIterator for SSTable<R> {
    type Item = Result<(Bytes, Value)>;
    type IntoIter = SSTableIntoIterator<R>;

    fn into_iter(self) -> Self::IntoIter {
        // Reader is always exists
        debug_assert!(self.reader.lock().unwrap().is_some());
        SSTableIntoIterator {
            block: None,
            index: self.index.index.into_iter(),
            reader: (*self.reader.lock().unwrap()).take().unwrap(),
            stop: false,
        }
    }
}

pub struct SSTableIntoIterator<R: Seek + Read> {
    block: Option<BlockIntoIterator>,
    index: std::vec::IntoIter<BlockItem>,
    reader: R,
    stop: bool,
}

// Implement a Iterator to iterating the sstable
impl<R: Seek + Read> Iterator for SSTableIntoIterator<R> {
    type Item = Result<(Bytes, Value)>;

    fn next(&mut self) -> Option<Self::Item> {
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
                    if let Some(block_item) = self.index.next() {
                        // Read a new block
                        let block_buffer = seek_and_read_buf(
                            &mut self.reader,
                            SeekFrom::Start(block_item.offset),
                            block_item.size,
                        );
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
                            return Some(Err(e.into()));
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

// Build a single sstable, use leveldb definition without filter block.
pub struct SSTableBuilder<W: Write> {
    writer: W,
    data_block: BlockBuilder,
    index_block: BlockBuilder,
    last_key: Vec<u8>,
    offset: u64,
    complete_data_block_size: usize,
    range: (Option<Bytes>, Option<Bytes>),
}

impl<W: Write> SSTableBuilder<W> {
    pub fn new(writer: W) -> Self {
        SSTableBuilder {
            writer,
            data_block: BlockBuilder::new(),
            index_block: BlockBuilder::new(),
            last_key: Vec::with_capacity(128),
            offset: 0,
            complete_data_block_size: 0,
            range: (None, None),
        }
    }

    // Add a new key to sstable and return size estimate
    pub fn add(&mut self, key: Bytes, value: Value) -> Result<usize> {
        self.data_block.add(&key, &value.to_bytes());
        self.last_key.clear();
        self.last_key.extend_from_slice(&key);

        let block_size = self.data_block.size_estimate();
        let complete_data_block_size = self.complete_data_block_size;

        // Flush if block size is bigger enough, add a index
        if block_size > BLOCK_DEFAULT_SIZE {
            self.complete_data_block_size += block_size;
            let (offset, data_size) = self.flush_new_data_block()?;
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

    // Finish a table construction, and get the sstable range
    pub fn finish(&mut self) -> Result<SSTableRange> {
        debug_assert!(self.range.0.is_some());
        debug_assert!(self.range.1.is_some());

        // Append new data block if data_block is not empty
        if !self.data_block.is_empty() {
            let (offset, data_size) = self.flush_new_data_block()?;
            self.add_new_index(offset, data_size)
        }

        // Append index block
        let data = self.index_block.finish();
        let index_offset = self.offset;
        let index_size = data.len() + 4; // add checksum extra

        self.writer.write_all(data)?;
        self.writer.write_all(&checksum(data).to_le_bytes()[0..4])?;

        self.index_block.reset();
        self.offset += index_size as u64 + 4;

        // Append footer
        self.writer
            .write_all(&Footer::new(index_offset, index_size).to_bytes())?;

        Ok(SSTableRange {
            left: self.range.0.as_ref().unwrap().clone(),
            right: self.range.1.as_ref().unwrap().clone(),
        })
    }

    #[inline]
    pub fn size_estimate(&self) -> usize {
        self.complete_data_block_size
            + self.data_block.size_estimate()
            + self.index_block.size_estimate()
    }

    fn add_new_index(&mut self, offset: u64, data_size: usize) {
        let mut new_index_value: [u8; 12] = [0; 12];
        new_index_value[0..8].copy_from_slice(&offset.to_le_bytes()[0..8]);
        new_index_value[8..12].copy_from_slice(&data_size.to_le_bytes()[0..4]);

        self.index_block.add(&self.last_key, &new_index_value);
    }

    fn flush_new_data_block(&mut self) -> Result<(u64, usize)> {
        let data = self.data_block.finish();
        let data_size = data.len();
        let offset = self.offset;

        // Write data and checksum, clear data block builder
        self.writer.write_all(data)?;
        self.writer.write_all(&checksum(data).to_le_bytes()[0..4])?;
        self.data_block.reset();

        self.offset += data_size as u64 + 4; // advance offset(data_size + checksum_size)

        Ok((offset, data_size))
    }
}

#[cfg(test)]
mod tests {
    use crate::util::generate_random_bytes;

    use super::*;
    use bytes::Bytes;
    use std::io::Cursor;

    #[test]
    fn test_table_build_and_search_with_random_case() {
        let (test_case_key, test_case_value, mut table) = create_new_table_with_random_case(1000);
        for (key, value) in test_case_key.iter().zip(test_case_value.iter()) {
            assert_eq!(
                table.search(key).unwrap().unwrap(),
                Value::living(value.clone())
            );
        }
    }

    #[test]
    fn test_compact_table_with_random_case() {
        let (test_case_key_1, test_case_value_1, mut table_1) =
            create_new_table_with_random_case(1500);
        for (key, value) in test_case_key_1.iter().zip(test_case_value_1.iter()) {
            assert_eq!(
                table_1.search(key).unwrap().unwrap(),
                Value::living(value.clone())
            );
        }

        let (test_case_key_2, test_case_value_2, mut table_2) =
            create_new_table_with_random_case(1000);
        for (key, value) in test_case_key_2.iter().zip(test_case_value_2.iter()) {
            assert_eq!(
                table_2.search(key).unwrap().unwrap(),
                Value::living(value.clone())
            );
        }

        // Build up table
        // let mut builder = SSTableBuilder::new(Vec::new());
        // compact_sstable(table_1, table_2, &mut builder).unwrap();

        // // test search operation
        // let writer = builder.writer;
        // let buffer = Cursor::new(writer);
        // let mut table = SSTable::open(buffer).unwrap();

        // let kv_1: HashMap<_, _> = test_case_key_1
        //     .iter()
        //     .zip(test_case_value_1.iter())
        //     .collect();

        // let kv_2: HashMap<_, _> = test_case_key_2
        //     .iter()
        //     .zip(test_case_value_2.iter())
        //     .collect();

        // for key in test_case_key_1.iter().chain(test_case_key_2.iter()) {
        //     let value = table.search_key(key).unwrap().unwrap();
        //     // Checkout table 2 first
        //     if let Some(value2) = kv_2.get(key) {
        //         assert_eq!(value, **value2);
        //         continue;
        //     }
        //     if let Some(value2) = kv_1.get(key) {
        //         assert_eq!(value, **value2);
        //         continue;
        //     }
        //     panic!("key lost after compaction");
        // }
    }

    fn create_new_table_with_random_case(
        test_count: usize,
    ) -> (Vec<Bytes>, Vec<Bytes>, SSTable<Cursor<Bytes>>) {
        // Generate random test case
        let mut test_case_key = generate_random_bytes(test_count, 10000);
        let test_case_value = generate_random_bytes(test_count, 10 * 32 * 1024);
        test_case_key.sort();

        // Build up table
        let mut builder = SSTableBuilder::new(Vec::new());
        for (key, value) in test_case_key.iter().zip(test_case_value.iter()) {
            builder
                .add(key.clone(), Value::living(value.clone()))
                .unwrap();
        }
        builder.finish().unwrap();

        // test search operation
        let buffer = Cursor::new(Bytes::from(builder.writer));
        let table = SSTable::open(buffer).unwrap();

        (test_case_key, test_case_value, table)
    }

    #[test]
    fn test_table_build_and_search() {
        use std::io::Cursor;

        let v = Vec::new();
        let mut builder = SSTableBuilder::new(v);
        builder
            .add(
                Bytes::from_static(b"123456789"),
                Value::living_static(b"1234567689"),
            )
            .unwrap();
        builder
            .add(
                Bytes::from_static(b"1234567891"),
                Value::living_static(b"1234567689"),
            )
            .unwrap();
        builder
            .add(
                Bytes::from_static(b"12345678912"),
                Value::living_static(b"1234567689"),
            )
            .unwrap();
        builder
            .add(
                Bytes::from_static(b"12345678923"),
                Value::living_static(b"1234567689"),
            )
            .unwrap();
        builder
            .add(Bytes::from_static(b"2"), Value::living_static(b"2"))
            .unwrap();
        builder
            .add(Bytes::from_static(b"3"), Value::living_static(b"3"))
            .unwrap();
        builder
            .add(Bytes::from_static(b"4"), Value::living_static(b""))
            .unwrap();
        builder.finish().unwrap();

        let v = Cursor::new(builder.writer);
        let table = SSTable::open(v).unwrap();

        let result = table.search(b"123456789").unwrap().unwrap();
        assert_eq!(result, Value::living_static(b"1234567689"));
        let result = table.search(b"1234567891").unwrap().unwrap();
        assert_eq!(result, Value::living_static(b"1234567689"));
        let result = table.search(b"12345678912").unwrap().unwrap();
        assert_eq!(result, Value::living_static(b"1234567689"));
        let result = table.search(b"12345678923").unwrap().unwrap();
        assert_eq!(result, Value::living_static(b"1234567689"));
        let result = table.search(b"2").unwrap().unwrap();
        assert_eq!(result, Value::living_static(b"2"));
        let result = table.search(b"3").unwrap().unwrap();
        assert_eq!(result, Value::living_static(b"3"));
        let result = table.search(b"4").unwrap().unwrap();
        assert_eq!(result, Value::living_static(b""));
        assert!(table.search(b"1000").unwrap().is_none());
        assert!(table.search(b"1").unwrap().is_none());
        assert!(table.search(b"666").unwrap().is_none());
        assert!(table.search(b"5").unwrap().is_none());
    }
}
