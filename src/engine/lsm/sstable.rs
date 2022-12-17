use super::Result;
use crc::{Crc, CRC_32_ISCSI};
use std::{io::Write, mem::size_of};

const KEY_PREFIX_COMPRESS_RESTART_THRESHOLD: usize = 15;
const BLOCK_BUFFER_DEFAULT_SIZE: usize = 10 * 1024; // 10kb
const BLOCK_DEFAULT_SIZE: usize = 4 * 1024; // 4kb

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
        builder.last_key.push(0);
        builder
    }

    // Reset to the initial status, start a new building task
    fn reset(&mut self) {
        self.buffer.clear();
        self.last_key.clear();
        self.last_key.push(0);
        self.compress_restart_offset.clear();
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
                .take_while(|x| x.0 != x.1)
                .count()
        } else {
            self.compress_restart_offset.push(self.buffer.len());
            0
        };
        let non_shared_prefix_count = self.last_key.len() - shared_prefix_count;

        // build length field
        self.buffer
            .extend_from_slice(&shared_prefix_count.to_le_bytes()[0..4]);
        self.buffer
            .extend_from_slice(&non_shared_prefix_count.to_le_bytes()[0..4]);
        self.buffer
            .extend_from_slice(&value.len().to_le_bytes()[0..4]);

        // build kv field
        self.buffer.extend_from_slice(&key[shared_prefix_count..]);
        self.buffer.extend_from_slice(&value);

        self.last_key.resize(shared_prefix_count, b'\x00');
        self.last_key.extend_from_slice(&key[shared_prefix_count..]);
        debug_assert_eq!(self.last_key, key);
        self.compressed_counter += 1;
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
        let mut new_index_value: [u8; 12] = [0; 12];
        (&mut new_index_value[0..8]).copy_from_slice(&offset.to_le_bytes()[0..8]);
        (&mut new_index_value[8..12]).copy_from_slice(&data_size.to_le_bytes()[0..4]);
        self.writer.write_all(&new_index_value)?;

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
