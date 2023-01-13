use super::{Error, Result};
use crate::util::from_le_bytes_32;
use std::{cmp::Ordering, mem::size_of};

const KEY_PREFIX_COMPRESS_RESTART_THRESHOLD: usize = 15;
const BLOCK_BUFFER_DEFAULT_SIZE: usize = 10 * 1024; // 10kb

// Build Key-Value data block, use leveldb definition.
// Use key prefix-shared compress method.
pub struct BlockBuilder {
    buffer: Vec<u8>, // In-memory buffer storage

    // Used to implement prefix-shared compress
    // Less disk usage and string construction memory usage at search
    last_key: Vec<u8>, // Record the last key, used to implement key prefix shared compress
    compress_restart_offset: Vec<usize>,
    compressed_counter: usize,
}

impl BlockBuilder {
    pub fn new() -> Self {
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
    pub fn reset(&mut self) {
        self.buffer.clear();
        self.last_key.clear();
        self.compress_restart_offset.clear();
        self.compress_restart_offset.push(0);
        self.compressed_counter = 0;
    }

    pub fn is_empty(&self) -> bool {
        self.buffer.is_empty()
    }

    pub fn size_estimate(&self) -> usize {
        self.buffer.len() + (self.compress_restart_offset.len() + 1) * size_of::<u32>()
    }

    // Append tailer and return the underlying block building result
    pub fn finish(&mut self) -> &[u8] {
        for offset in self.compress_restart_offset.iter() {
            self.buffer.extend_from_slice(&offset.to_le_bytes()[0..4]);
        }
        self.buffer
            .extend_from_slice(&self.compress_restart_offset.len().to_le_bytes()[0..4]);

        &self.buffer
    }

    // Key-value pair must be added by lexicographic order of key
    pub fn add(&mut self, key: &[u8], value: &[u8]) {
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

pub struct Block {
    data: Vec<u8>, // Original block binary data
    compress_restart_offset: Vec<usize>,
}

impl Block {
    // Construct a new block from bytes builded by BlockBuilder
    pub fn from_bytes(mut data: Vec<u8>) -> Result<Self> {
        let data_size = data.len();

        // Store size of restart offset array at least
        debug_assert!(data_size > 4);
        if data_size <= 4 {
            return Err(Error::BadSSTableBlock);
        }
        let restart_offset_size = from_le_bytes_32(&data[data_size - 4..data_size]);

        // Store restart offset array at least and recover the array of restart offset
        debug_assert!(data.len() > ((restart_offset_size + 1) * 4));
        if data_size <= (restart_offset_size + 1) * 4 {
            return Err(Error::BadSSTableBlock);
        }
        let mut compress_restart_offset = Vec::with_capacity(restart_offset_size);
        let restart_offset_start = data_size - 4 * (1 + restart_offset_size);
        for offset in (restart_offset_start..data_size - 4).step_by(4) {
            debug_assert!(offset + 4 < data_size);
            compress_restart_offset.push(from_le_bytes_32(&data[offset..offset + 4]));
        }

        // resize to data block
        data.resize(restart_offset_start, b'\x00');

        Ok(Block {
            data,
            compress_restart_offset,
        })
    }

    pub fn into_iter<'a>(self) -> BlockIntoIterator {
        BlockIntoIterator {
            data: self.data,
            data_offset_idx: 0,
            compress_restart_offset: self.compress_restart_offset,
            last_key: Vec::new(),
            counter: 0,
            restart_offset_idx: 0,
            err: None,
        }
    }

    // Create a new iterator on this block
    pub fn iter(&self) -> BlockIterator {
        BlockIterator {
            last_key: Vec::with_capacity(128),
            compress_restart_offset: &self.compress_restart_offset,
            data: &self.data,
            counter: 0,
            restart_offset_idx: 0,
            err: None,
        }
    }

    // Search a key in this block, block items is assumed to be sorted in lexicographic order
    pub fn search_key(&self, key: &[u8]) -> Option<&[u8]> {
        for (current_key, value) in self.iter() {
            match (&current_key[..]).cmp(key) {
                Ordering::Equal => return Some(value),
                // KV Items are sorted, return None immediately if current key is lexicographic greater than target key
                Ordering::Greater => return None,
                _ => continue,
            }
        }
        None
    }

    // Parse new KV, return the shared_prefix_count, non_shared_key, value and rest data
    fn parse_new_kv(data: &[u8]) -> Result<(usize, &[u8], &[u8], usize)> {
        // Check single item header,
        // 12 bytes, shared_prefix_count, non_shared_prefix_count, value_length
        debug_assert!(data.len() > 12);
        if data.len() <= 12 {
            return Err(Error::BadSSTableBlock);
        }

        let shared_prefix_count = from_le_bytes_32(&data[0..4]);
        let non_shared_prefix_count = from_le_bytes_32(&data[4..8]);
        let value_length = from_le_bytes_32(&data[8..12]);

        let key_offset = 12;
        let value_offset = key_offset + non_shared_prefix_count;
        let next_data_offset = value_offset + value_length;
        debug_assert!(next_data_offset <= data.len());
        if next_data_offset > data.len() {
            return Err(Error::BadSSTableBlock);
        }

        Ok((
            shared_prefix_count,
            &data[key_offset..value_offset],
            &data[value_offset..next_data_offset],
            next_data_offset,
        ))
    }
}

pub struct BlockIntoIterator {
    data: Vec<u8>,
    data_offset_idx: usize,
    compress_restart_offset: Vec<usize>,
    last_key: Vec<u8>,
    counter: usize,
    restart_offset_idx: usize,
    err: Option<Error>,
}

impl BlockIntoIterator {
    // Get error and drop the target
    fn error(mut self) -> Option<Error> {
        std::mem::replace(&mut self.err, None)
    }
}

impl Iterator for BlockIntoIterator {
    type Item = (Vec<u8>, Vec<u8>);

    fn next(&mut self) -> Option<Self::Item> {
        debug_assert!(matches!(self.err, None));
        if self.data_offset_idx == self.data.len() {
            return None;
        }

        // Check restart offset and clear the last key
        if self.restart_offset_idx < self.compress_restart_offset.len()
            && self.compress_restart_offset[self.restart_offset_idx] == self.counter
        {
            self.last_key.clear();
            self.restart_offset_idx += 1;
        }

        // parse single kv pair
        match Block::parse_new_kv(&self.data[self.data_offset_idx..]) {
            Ok((shared_prefix_count, non_shared_key, value, next_offset)) => {
                // Advance state
                self.last_key.resize(shared_prefix_count, b'\x00');
                self.last_key.extend_from_slice(non_shared_key);
                self.counter += 1;
                self.data_offset_idx += next_offset;

                let key = self.last_key[..shared_prefix_count]
                    .iter()
                    .chain(non_shared_key)
                    .copied()
                    .collect();

                Some((key, value.to_vec()))
            }
            Err(e) => {
                // Error happened, stop iteration
                self.err = Some(e);
                None
            }
        }
    }
}

// Block iteration implement
pub struct BlockIterator<'a> {
    data: &'a [u8],
    compress_restart_offset: &'a [usize],
    last_key: Vec<u8>,
    counter: usize,
    restart_offset_idx: usize,
    err: Option<Error>,
}

impl<'a> BlockIterator<'a> {
    // Get error and drop the target
    fn error(self) -> Option<Error> {
        self.err
    }
}

impl<'a> Iterator for BlockIterator<'a> {
    type Item = (Vec<u8>, &'a [u8]);

    fn next(&mut self) -> Option<Self::Item> {
        debug_assert!(matches!(self.err, None));
        if self.data.is_empty() {
            return None;
        }

        // Check restart offset and clear the last key
        if self.restart_offset_idx < self.compress_restart_offset.len()
            && self.compress_restart_offset[self.restart_offset_idx] == self.counter
        {
            self.last_key.clear();
            self.restart_offset_idx += 1;
        }

        // parse single kv pair
        match Block::parse_new_kv(self.data) {
            Ok((shared_prefix_count, non_shared_key, value, next_offset)) => {
                // Advance state
                self.last_key.resize(shared_prefix_count, b'\x00');
                self.last_key.extend_from_slice(non_shared_key);
                self.counter += 1;
                self.data = &self.data[next_offset..];

                let key = self.last_key[..shared_prefix_count]
                    .iter()
                    .chain(non_shared_key)
                    .copied()
                    .collect();

                Some((key, value))
            }
            Err(e) => {
                // Error happened, stop iteration
                self.err = Some(e);
                None
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::util::generate_random_bytes_vec;

    #[test]
    fn test_block_build_and_search_with_random_case() {
        // Generate random test key
        let test_count = 1000;
        let mut test_case_key = generate_random_bytes_vec(test_count, 10000);
        let test_case_value = generate_random_bytes_vec(test_count, 10 * 32 * 1024);
        test_case_key.sort();

        // build the table
        let mut builder = BlockBuilder::new();
        for (key, value) in test_case_key.iter().zip(test_case_value.iter()) {
            builder.add(key, value);
        }
        let data = builder.finish();

        // test search operation
        let block = Block::from_bytes(data.to_vec()).unwrap();
        for (key, value) in test_case_key.iter().zip(test_case_value.iter()) {
            assert_eq!(&block.search_key(key).unwrap(), value);
        }
    }

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
        let block = Block::from_bytes(data.to_vec()).unwrap();
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
