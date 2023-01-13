use crate::util::{checksum, from_le_bytes_32, from_le_bytes_64, seek_and_read_buf};

use super::{
    block::{Block, BlockBuilder, BlockIntoIterator},
    Error, Result,
};
use std::{
    cmp::Ordering,
    io::{Read, Seek, SeekFrom, Write},
};

const FOOTER_SIZE: usize = 12; // 12bytes
const BLOCK_DEFAULT_SIZE: usize = 4 * 1024; // 4kb

pub struct Table<R: Seek + Read> {
    index: Index,
    reader: R,
    range: (Vec<u8>, Vec<u8>), // Key range of this table [xxx, xxx]
}

impl<R: Seek + Read> Table<R> {
    fn open(mut reader: R) -> Result<Self> {
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
        let index = Index::from_block(Block::from_bytes(index_buffer)?)?;

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

        let first_block = Block::from_bytes(block_buffer)?;
        let (first_key, _) = first_block.iter().next().ok_or(Error::BadSSTable)?;
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
            Block::from_bytes(block_buffer)?
        };

        let mut last_key = Vec::new();
        for (k, _) in last_block.iter() {
            last_key = k;
        }

        Ok(Table {
            index,
            reader,
            range: (first_key, last_key),
        })
    }

    pub fn search_key(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        if !self.in_range(key) {
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
            &mut self.reader,
            SeekFrom::Start(block_info.offset),
            block_info.size,
        )?;
        debug_assert_eq!(block_buffer.len(), block_info.size);
        if block_buffer.len() != block_info.size {
            return Err(Error::BadSSTable);
        }

        if let Some(value) = Block::from_bytes(block_buffer)?.search_key(key) {
            return Ok(Some(value.to_vec()));
        }
        Ok(None)
    }

    fn in_range(&self, key: &[u8]) -> bool {
        key >= &self.range.0[..] && key <= &self.range.1[..]
    }
}

impl<R: Read + Seek> IntoIterator for Table<R> {
    type Item = (Vec<u8>, Vec<u8>);
    type IntoIter = TableIntoIterator<R>;

    fn into_iter(self) -> Self::IntoIter {
        TableIntoIterator {
            block: None,
            index: self.index.index.into_iter(),
            reader: self.reader,
            err: None,
        }
    }
}

pub struct TableIntoIterator<R: Seek + Read> {
    block: Option<BlockIntoIterator>,
    index: std::vec::IntoIter<BlockItem>,
    reader: R,
    err: Option<Error>,
}

impl<R: Seek + Read> TableIntoIterator<R> {
    fn error(self) -> Result<()> {
        if let Some(err) = self.err {
            Err(err)
        } else {
            Ok(())
        }
    }
}

impl<R: Seek + Read> Iterator for TableIntoIterator<R> {
    type Item = (Vec<u8>, Vec<u8>);

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            match &mut self.block {
                Some(block) => {
                    // Read the next kv
                    if let Some((key, value)) = block.next() {
                        return Some((key, value.to_vec()));
                    }
                    // Reach the end of block iteration
                    self.block = None;
                }
                // Read a new block
                None => {
                    // Search index if block is none
                    if let Some(block_info) = self.index.next() {
                        // Read a new block
                        let block_buffer = seek_and_read_buf(
                            &mut self.reader,
                            SeekFrom::Start(block_info.offset),
                            block_info.size,
                        );
                        if block_buffer.is_err() {
                            self.err = block_buffer.err().map(Error::from);
                            return None;
                        }
                        let block_buffer = block_buffer.unwrap();

                        debug_assert_eq!(block_buffer.len(), block_info.size);
                        if block_buffer.len() != block_info.size {
                            self.err = Some(Error::BadSSTable);
                            return None;
                        }
                        let block = Block::from_bytes(block_buffer);
                        if block.is_err() {
                            self.err = block.err();
                            return None;
                        }
                        let block = block.unwrap();
                        self.block = Some(block.into_iter());
                    } else {
                        // Read the end of index iteration
                        return None;
                    }
                }
            }
        }
    }
}

// Compact two Table and build new table
pub fn compact<R: Read + Seek, W: Write>(
    prev: Table<R>,
    next: Table<R>,
    builder: &mut TableBuilder<W>,
) -> Result<()> {
    let mut prev_iter = prev.into_iter();
    let mut next_iter = next.into_iter();

    let mut prev_new_kv: Option<(Vec<u8>, Vec<u8>)> = None;
    let mut next_new_kv: Option<(Vec<u8>, Vec<u8>)> = None;

    // Tables are sorted by key, do merging by using role of merge sort.
    loop {
        if prev_new_kv.is_none() {
            prev_new_kv = prev_iter.next();
        }
        if next_new_kv.is_none() {
            next_new_kv = next_iter.next();
        }

        // Finish tow iteration, stop merging
        if next_new_kv.is_none() && prev_new_kv.is_none() {
            break;
        } else if next_new_kv.is_some() && prev_new_kv.is_none() {
            let (key, value) = next_new_kv.unwrap();
            builder.add(&key, &value)?;
            next_new_kv = None;
        } else if next_new_kv.is_none() && prev_new_kv.is_some() {
            let (key, value) = prev_new_kv.unwrap();
            builder.add(&key, &value)?;
            prev_new_kv = None;
        } else {
            match next_new_kv.as_ref().unwrap().0[..].cmp(&prev_new_kv.as_ref().unwrap().0[..]) {
                Ordering::Equal => {
                    // Always use next table if key is same
                    let (key, value) = next_new_kv.unwrap();
                    builder.add(&key, &value)?;
                    next_new_kv = None;
                    prev_new_kv = None;
                }
                Ordering::Greater => {
                    let (key, value) = prev_new_kv.unwrap();
                    builder.add(&key, &value)?;
                    prev_new_kv = None;
                }
                Ordering::Less => {
                    let (key, value) = next_new_kv.unwrap();
                    builder.add(&key, &value)?;
                    next_new_kv = None;
                }
            }
        }
    }

    // Checkout iteration success
    prev_iter.error()?;
    next_iter.error()?;

    // Finish the build
    builder.finish()?;

    Ok(())
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
        (&mut new_index_value[0..8]).copy_from_slice(&self.index_offset.to_le_bytes()[0..8]);
        (&mut new_index_value[8..12]).copy_from_slice(&self.index_size.to_le_bytes()[0..4]);

        new_index_value
    }
}

struct BlockItem {
    key: Vec<u8>,
    block: Option<Block>, // In memory block
    offset: u64,          // Data block offset in sstable file
    size: usize,          // Data block size in sstable file
}

struct Index {
    index: Vec<BlockItem>, // In memory index
}

impl Index {
    fn from_block(b: Block) -> Result<Self> {
        // TODO: Optimize by saving block count into index file
        let mut index = Vec::with_capacity(128);

        for (key, value) in b.iter() {
            debug_assert_eq!(value.len(), 12);
            if value.len() != 12 {
                return Err(Error::BadSSTableBlock);
            }

            let data_offset = from_le_bytes_64(&value[0..8]);
            let data_size = from_le_bytes_32(&value[8..12]);

            index.push(BlockItem {
                key,
                block: None,
                offset: data_offset,
                size: data_size,
            });
        }

        Ok(Index { index })
    }
}

// Build a single sstable, use leveldb definition without filter block.
pub struct TableBuilder<W: Write> {
    writer: W,
    data_block: BlockBuilder,
    index_block: BlockBuilder,
    last_key: Vec<u8>,
    offset: u64,
}

impl<W: Write> TableBuilder<W> {
    fn new(writer: W) -> Self {
        TableBuilder {
            writer,
            data_block: BlockBuilder::new(),
            index_block: BlockBuilder::new(),
            last_key: Vec::with_capacity(128),
            offset: 0,
        }
    }

    // Add a new key to sstable
    fn add(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        self.data_block.add(key, value);
        self.last_key.clear();
        self.last_key.extend_from_slice(key);

        // Flush if block size is bigger enough, add a index
        if self.data_block.size_estimate() > BLOCK_DEFAULT_SIZE {
            let (offset, data_size) = self.flush_new_data_block()?;
            self.add_new_index(offset, data_size);
        }

        Ok(())
    }

    // Finish a table construction
    fn finish(&mut self) -> Result<()> {
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

        Ok(())
    }

    fn add_new_index(&mut self, offset: u64, data_size: usize) {
        let mut new_index_value: [u8; 12] = [0; 12];
        new_index_value[0..8].copy_from_slice(&offset.to_le_bytes()[0..8]);
        new_index_value[8..12].copy_from_slice(&data_size.to_le_bytes()[0..4]);

        self.index_block.add(&self.last_key, &new_index_value)
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
    use crate::util::generate_random_bytes_vec;

    use super::*;
    use std::{collections::HashMap, io::Cursor};

    #[test]
    fn test_table_build_and_search_with_random_case() {
        let (test_case_key, test_case_value, mut table) = create_new_table_with_random_case(1000);
        for (key, value) in test_case_key.iter().zip(test_case_value.iter()) {
            assert_eq!(&table.search_key(key).unwrap().unwrap(), value);
        }
    }

    #[test]
    fn test_compact_table_with_random_case() {
        let (test_case_key_1, test_case_value_1, mut table_1) =
            create_new_table_with_random_case(1500);
        for (key, value) in test_case_key_1.iter().zip(test_case_value_1.iter()) {
            assert_eq!(&table_1.search_key(key).unwrap().unwrap(), value);
        }

        let (test_case_key_2, test_case_value_2, mut table_2) =
            create_new_table_with_random_case(1000);
        for (key, value) in test_case_key_2.iter().zip(test_case_value_2.iter()) {
            assert_eq!(&table_2.search_key(key).unwrap().unwrap(), value);
        }

        // Build up table
        let mut builder = TableBuilder::new(Vec::new());
        compact(table_1, table_2, &mut builder).unwrap();

        // test search operation
        let writer = builder.writer;
        let buffer = Cursor::new(writer);
        let mut table = Table::open(buffer).unwrap();

        let kv_1: HashMap<_, _> = test_case_key_1
            .iter()
            .zip(test_case_value_1.iter())
            .collect();

        let kv_2: HashMap<_, _> = test_case_key_2
            .iter()
            .zip(test_case_value_2.iter())
            .collect();

        for key in test_case_key_1.iter().chain(test_case_key_2.iter()) {
            let value = table.search_key(key).unwrap().unwrap();
            // Checkout table 2 first
            if let Some(value2) = kv_2.get(key) {
                assert_eq!(value, **value2);
                continue;
            }
            if let Some(value2) = kv_1.get(key) {
                assert_eq!(value, **value2);
                continue;
            }
            panic!("key lost after compaction");
        }
    }

    fn create_new_table_with_random_case(
        test_count: usize,
    ) -> (Vec<Vec<u8>>, Vec<Vec<u8>>, Table<Cursor<Vec<u8>>>) {
        // Generate random test case
        let mut test_case_key = generate_random_bytes_vec(test_count, 10000);
        let test_case_value = generate_random_bytes_vec(test_count, 10 * 32 * 1024);
        test_case_key.sort();

        // Build up table
        let mut builder = TableBuilder::new(Vec::new());
        for (key, value) in test_case_key.iter().zip(test_case_value.iter()) {
            builder.add(key, value).unwrap();
        }
        builder.finish().unwrap();

        // test search operation
        let buffer = Cursor::new(builder.writer);
        let table = Table::open(buffer).unwrap();

        (test_case_key, test_case_value, table)
    }

    #[test]
    fn test_table_build_and_search() {
        use std::io::Cursor;

        let v = Vec::new();
        let mut builder = TableBuilder::new(v);
        builder.add(b"123456789", b"1234567689").unwrap();
        builder.add(b"1234567891", b"1234567689").unwrap();
        builder.add(b"12345678912", b"1234567689").unwrap();
        builder.add(b"12345678923", b"1234567689").unwrap();
        builder.add(b"2", b"2").unwrap();
        builder.add(b"3", b"3").unwrap();
        builder.add(b"4", b"").unwrap();
        builder.finish().unwrap();

        let v = Cursor::new(builder.writer);
        let mut table = Table::open(v).unwrap();

        let result = table.search_key(b"123456789").unwrap().unwrap();
        assert_eq!(result, b"1234567689");
        let result = table.search_key(b"1234567891").unwrap().unwrap();
        assert_eq!(result, b"1234567689");
        let result = table.search_key(b"12345678912").unwrap().unwrap();
        assert_eq!(result, b"1234567689");
        let result = table.search_key(b"12345678923").unwrap().unwrap();
        assert_eq!(result, b"1234567689");
        let result = table.search_key(b"2").unwrap().unwrap();
        assert_eq!(result, b"2");
        let result = table.search_key(b"3").unwrap().unwrap();
        assert_eq!(result, b"3");
        let result = table.search_key(b"4").unwrap().unwrap();
        assert_eq!(result, b"");
        assert!(table.search_key(b"1000").unwrap().is_none());
        assert!(table.search_key(b"1").unwrap().is_none());
        assert!(table.search_key(b"666").unwrap().is_none());
        assert!(table.search_key(b"5").unwrap().is_none());
    }
}