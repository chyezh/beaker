use crate::engine::lsm::util::{from_le_bytes_32, from_le_bytes_64};

use super::{
    block::{Block, BlockBuilder},
    util::{checksum, read_exact},
    Error, Result,
};
use std::{
    collections::BTreeMap,
    io::{Read, Seek, SeekFrom, Write},
};

const FOOTER_SIZE: usize = 12; // 12bytes
const BLOCK_DEFAULT_SIZE: usize = 4 * 1024; // 4kb

struct SSTable<R: Seek + Read> {
    footer: Footer,
    index: BTreeMap<Vec<u8>, (usize, usize)>,
    reader: R,
}

impl<R: Seek + Read> SSTable<R> {
    fn open(mut reader: R) -> Result<Self> {
        // Set offset to footer
        reader.seek(SeekFrom::End(FOOTER_SIZE as i64))?;

        // Read footer
        let mut footer_buffer = Vec::with_capacity(FOOTER_SIZE);
        read_exact(&mut reader, &mut footer_buffer)?;
        let footer = Footer::from_bytes(&footer_buffer);
        debug_assert!(footer.index.0 > 0 && footer.index.1 > 0);

        // Read index
        let mut index_buffer = Vec::with_capacity(footer.index.1);
        reader.seek(SeekFrom::Start(footer.index.0 as u64))?;
        read_exact(&mut reader, &mut index_buffer)?;
        debug_assert_eq!(index_buffer.len(), footer.index.1);

        // Recover index
        debug_assert!(index_buffer.len() >= 4);
        let index_len = index_buffer.len();
        // validate checksum
        let check_sum_result = from_le_bytes_32(&index_buffer[index_len - 4..index_len]) as u32;
        if checksum(&index_buffer[0..index_len - 4]) != check_sum_result {
            return Err(Error::IllegalTable);
        }

        index_buffer.resize(index_len - 4, b'\x00');
        let index = Block::from_bytes(index_buffer);

        todo!()
    }
}

struct Footer {
    index: (u64, usize), // index block offset and size
}

impl Footer {
    // Create a new Footer
    fn new(index: (u64, usize)) -> Self {
        Footer { index }
    }

    // Create a new Footer from bytes
    // Panic if data is invalid format
    fn from_bytes(data: &[u8]) -> Self {
        debug_assert_eq!(data.len(), FOOTER_SIZE);
        let index_offset = from_le_bytes_64(&data[0..8]);
        let index_size = from_le_bytes_32(&data[8..12]);

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
        self.writer.write_all(&checksum(data).to_le_bytes()[0..4])?;
        self.index_block.reset();
        self.offset += data_size as u64 + 4;

        // Append footer
        self.writer
            .write_all(&Footer::new((offset, data_size)).to_bytes())?;

        Ok(())
    }

    fn add_new_index(&mut self, offset: u64, data_size: usize) {
        let mut new_index_value: [u8; 12] = [0; 12];
        (&mut new_index_value[0..8]).copy_from_slice(&offset.to_le_bytes()[0..8]);
        (&mut new_index_value[8..12]).copy_from_slice(&data_size.to_le_bytes()[0..4]);

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
