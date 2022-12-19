use super::Result;
use crc::{Crc, CRC_32_ISCSI};
use std::{
    collections::BTreeMap,
    io::{Read, Seek, SeekFrom, Write},
    mem::size_of,
    path::PathBuf,
};

const KEY_PREFIX_COMPRESS_RESTART_THRESHOLD: usize = 15;
const BLOCK_BUFFER_DEFAULT_SIZE: usize = 10 * 1024; // 10kb
const BLOCK_DEFAULT_SIZE: usize = 4 * 1024; // 4kb
const FOOTER_SIZE: usize = 12; // 12bytes

struct SSTable<R: Seek + Read> {
    footer: Footer,
    index: BTreeMap<Vec<u8>, (usize, usize)>,
    reader: R,
}

impl<R: Seek + Read> SSTable<R> {
    fn open(mut r: R) -> Result<Self> {
        // set offset to footer
        r.seek(SeekFrom::End(FOOTER_SIZE as i64))?;

        // read footer
        let mut footer_buffer = Vec::with_capacity(FOOTER_SIZE);
        r.read_to_end(&mut footer_buffer)?;
        let footer = Footer::from_bytes(&footer_buffer);
        debug_assert!(footer.index.0 > 0 && footer.index.1 > 0);

        r.seek(SeekFrom::Start(footer.index.0 as u64))?;

        todo!()
    }
}

struct Block {
    data: Vec<u8>, // Original block binary data
    compress_restart_offset: Vec<usize>,
}

impl Block {
    // Construct a new block from bytes builded by BlockBuilder
    fn from_bytes(mut data: Vec<u8>) -> Self {
        debug_assert!(data.len() > 4);
        let data_size = data.len();

        // get restart array size
        let restart_offset_size =
            u32::from_le_bytes((&data[data_size - 4..data_size]).try_into().unwrap()) as usize;
        debug_assert!(data.len() > ((restart_offset_size + 1) * 4));

        let mut compress_restart_offset = Vec::with_capacity(restart_offset_size);
        for idx in (1..=restart_offset_size).rev() {
            debug_assert!(data_size >= 4 * (idx + 1));

            let end = data_size - (4 * idx);
            compress_restart_offset
                .push(u32::from_le_bytes((&data[end - 4..end]).try_into().unwrap()) as usize);
        }

        // resize to data block
        data.resize(data_size - 4 * (1 + restart_offset_size), b'\x00');

        Block {
            data,
            compress_restart_offset,
        }
    }

    // Search value by key in data
    fn search_key(&self, key: &[u8]) -> Option<&[u8]> {
        let mut last_key = Vec::with_capacity(128);
        let mut restart_offset_idx = 0;
        let mut counter = 0;

        let mut data = &self.data[..];
        loop {
            if data.is_empty() {
                break;
            }

            // Check restart offset and clear the last key
            if self.compress_restart_offset.len() > restart_offset_idx
                && self.compress_restart_offset[restart_offset_idx] == counter
            {
                last_key.clear();
                restart_offset_idx += 1;
            }

            // parse single kv pair
            let (shared_prefix_count, non_shared_key, value, new_data) = Self::parse_new_kv(&data);
            // compare key
            let mut is_greater = false;

            let match_counts = key
                .iter()
                .zip(
                    (last_key[0..shared_prefix_count])
                        .iter()
                        .chain(non_shared_key.iter()),
                )
                .take_while(|x| {
                    if x.0 == x.1 {
                        true
                    } else {
                        if x.0 < x.1 {
                            is_greater = true
                        }
                        false
                    }
                })
                .count();
            let current_key_count = shared_prefix_count + non_shared_key.len();

            // Full prefix matched
            if match_counts == key.len() {
                // Key Found
                if key.len() == current_key_count {
                    return Some(value);
                }
                // Target key prefix matched, but current key is longer than target key, current key is lexicographic greater than target key
                // e.g.
                // current key: 123456789
                // target key: 123456
                is_greater = true
            }

            // KV Items are sorted in asc order, return None immediately if input key is lexicographic greater than current key
            if is_greater {
                return None;
            }

            last_key.resize(shared_prefix_count, b'\x00');
            last_key.extend_from_slice(non_shared_key);
            counter += 1;
            data = new_data;
        }

        None
    }

    // Parse new KV, return the shared_prefix_count, non_shared_key, value and rest data
    fn parse_new_kv(data: &[u8]) -> (usize, &[u8], &[u8], &[u8]) {
        debug_assert!(data.len() > 12); // shared_prefix_count, non_shared_prefix_count, value_length

        let shared_prefix_count = u32::from_le_bytes((&data[0..4]).try_into().unwrap()) as usize;
        let non_shared_prefix_count =
            u32::from_le_bytes((&data[4..8]).try_into().unwrap()) as usize;
        let value_length = u32::from_le_bytes((&data[8..12]).try_into().unwrap()) as usize;

        let key_offset = 12;
        let value_offset = key_offset + non_shared_prefix_count;
        let next_data_offset = value_offset + value_length;
        debug_assert!(next_data_offset <= data.len());

        (
            shared_prefix_count,
            &data[key_offset..value_offset],
            &data[value_offset..next_data_offset],
            &data[next_data_offset..],
        )
    }
}

// Build Key-Value data block, use leveldb definition.
// Use key prefix-shared compress method.
struct BlockBuilder {
    buffer: Vec<u8>, // In-memory buffer storage

    // Used to implement prefix-shared compress
    // Less disk usage and string construction memory usage at search
    last_key: Vec<u8>, // Record the last key, used to implement key prefix shared compress
    compress_restart_offset: Vec<usize>,
    compressed_counter: usize,
}

impl BlockBuilder {
    fn new() -> Self {
        let mut builder = BlockBuilder {
            buffer: Vec::with_capacity(BLOCK_BUFFER_DEFAULT_SIZE),
            last_key: Vec::with_capacity(128),
            compress_restart_offset: Vec::with_capacity(10),
            compressed_counter: 0,
        };
        builder.compress_restart_offset.push(0);
        builder
    }

    // Reset to the initial status, start a new building task
    fn reset(&mut self) {
        self.buffer.clear();
        self.last_key.clear();
        self.compress_restart_offset.clear();
        self.compress_restart_offset.push(0);
        self.compressed_counter = 0;
    }

    fn is_empty(&self) -> bool {
        self.buffer.is_empty()
    }

    fn size_estimate(&self) -> usize {
        self.buffer.len() + (self.compress_restart_offset.len() + 1) * size_of::<u32>()
    }

    // Append tailer and return the underlying block building result
    fn finish(&mut self) -> &[u8] {
        for offset in self.compress_restart_offset.iter() {
            self.buffer.extend_from_slice(&offset.to_le_bytes()[0..4]);
        }
        self.buffer
            .extend_from_slice(&self.compress_restart_offset.len().to_le_bytes()[0..4]);

        &self.buffer
    }

    // Key-value pair must be added by lexicographic order of key
    fn add(&mut self, key: &[u8], value: &[u8]) {
        // Calculate the shared prefix length
        let shared_prefix_count = if self.compressed_counter < KEY_PREFIX_COMPRESS_RESTART_THRESHOLD
        {
            key.iter()
                .zip(self.last_key.iter())
                .take_while(|x| x.0 == x.1)
                .count()
        } else {
            self.compress_restart_offset.push(self.buffer.len());
            0
        };
        let non_shared_prefix_count = key.len() - shared_prefix_count;

        // build length field
        self.buffer
            .extend_from_slice(&shared_prefix_count.to_le_bytes()[0..4]);
        self.buffer
            .extend_from_slice(&non_shared_prefix_count.to_le_bytes()[0..4]);
        self.buffer
            .extend_from_slice(&value.len().to_le_bytes()[0..4]);

        // build kv field
        self.buffer.extend_from_slice(&key[shared_prefix_count..]);
        self.buffer.extend_from_slice(value);

        self.last_key.resize(shared_prefix_count, b'\x00');
        self.last_key.extend_from_slice(&key[shared_prefix_count..]);
        debug_assert_eq!(self.last_key, key);
        self.compressed_counter += 1;
    }
}

struct Footer {
    index: (usize, usize), // index block offset and size
}

impl Footer {
    // Create a new Footer
    fn new(index: (usize, usize)) -> Self {
        Footer { index }
    }

    // Create a new Footer from bytes
    // Panic if data is invalid format
    fn from_bytes(data: &[u8]) -> Self {
        debug_assert_eq!(data.len(), FOOTER_SIZE);
        let index_offset = u64::from_le_bytes((&data[0..8]).try_into().unwrap()) as usize;
        let index_size = u32::from_le_bytes((&data[8..12]).try_into().unwrap()) as usize;

        Footer {
            index: (index_offset, index_size),
        }
    }

    // Convert Footer into bytes
    fn to_bytes(&self) -> [u8; FOOTER_SIZE] {
        // Append footer
        let mut new_index_value: [u8; FOOTER_SIZE] = [0; FOOTER_SIZE];
        (&mut new_index_value[0..8]).copy_from_slice(&self.index.0.to_le_bytes()[0..8]);
        (&mut new_index_value[8..12]).copy_from_slice(&self.index.1.to_le_bytes()[0..4]);

        new_index_value
    }
}

// Build a single sstable, use leveldb definition without filter block.
struct TableBuilder<W: Write> {
    writer: W,
    data_block: BlockBuilder,
    index_block: BlockBuilder,
    last_key: Vec<u8>,
    offset: usize,
    crc: Crc<u32>,
}

impl<W: Write> TableBuilder<W> {
    fn new(writer: W) -> Self {
        TableBuilder {
            writer,
            data_block: BlockBuilder::new(),
            index_block: BlockBuilder::new(),
            last_key: Vec::with_capacity(128),
            offset: 0,
            crc: Crc::<u32>::new(&CRC_32_ISCSI),
        }
    }

    // Add a new key to sstable
    fn add(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        self.data_block.add(key, value);
        // Flush if block size is bigger enough, add a index
        if self.data_block.size_estimate() > BLOCK_DEFAULT_SIZE {
            let (offset, data_size) = self.flush_new_data_block()?;
            self.last_key.clear();
            self.last_key.extend_from_slice(key);
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
        let data_size = data.len();
        let offset = self.offset;

        self.writer.write_all(data)?;
        self.writer
            .write_all(&self.crc.checksum(data).to_le_bytes()[0..4])?;
        self.index_block.reset();
        self.offset += data_size + 4;

        // Append footer
        self.writer
            .write_all(&Footer::new((offset, data_size)).to_bytes())?;

        Ok(())
    }

    fn add_new_index(&mut self, offset: usize, data_size: usize) {
        let mut new_index_value: [u8; 12] = [0; 12];
        (&mut new_index_value[0..8]).copy_from_slice(&offset.to_le_bytes()[0..8]);
        (&mut new_index_value[8..12]).copy_from_slice(&data_size.to_le_bytes()[0..4]);

        self.index_block.add(&self.last_key, &new_index_value)
    }

    fn flush_new_data_block(&mut self) -> Result<(usize, usize)> {
        let data = self.data_block.finish();
        let data_size = data.len();
        let offset = self.offset;

        // Write data and checksum, clear data block builder
        self.writer.write_all(data)?;
        self.writer
            .write_all(&self.crc.checksum(data).to_le_bytes()[0..4])?;
        self.data_block.reset();

        self.offset += data_size + 4; // advance offset(data_size + checksum_size)

        Ok((offset, data_size))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_block_build_and_search() {
        let mut builder = BlockBuilder::new();
        builder.add(b"123456789", b"1234567689");
        builder.add(b"1234567891", b"1234567689");
        builder.add(b"12345678912", b"1234567689");
        builder.add(b"12345678923", b"1234567689");
        builder.add(b"2", b"2");
        builder.add(b"3", b"3");
        builder.add(b"4", b"");
        let data = builder.finish();
        let block = Block::from_bytes(data.to_vec());
        assert_eq!(block.search_key(b"123456789"), Some(b"1234567689".as_ref()));
        assert_eq!(block.search_key(b"2"), Some(b"2".as_ref()));
        assert_eq!(block.search_key(b"3"), Some(b"3".as_ref()));
        assert_eq!(block.search_key(b"4"), Some(b"".as_ref()));
        assert_eq!(block.search_key(b"5"), None);
        assert_eq!(
            block.search_key(b"1234567891"),
            Some(b"1234567689".as_ref())
        );
        assert_eq!(
            block.search_key(b"12345678912"),
            Some(b"1234567689".as_ref())
        );
        assert_eq!(
            block.search_key(b"12345678923"),
            Some(b"1234567689".as_ref())
        );
    }
}
