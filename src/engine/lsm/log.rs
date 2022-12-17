// Write-ahead-log format, reader and writer implementation.
// Reference to leveldb log format implementation.
//  block := record* trailer?
//  record :=
//    checksum: uint32     // crc32c of type and data[] ; little-endian
//    length: uint16       // little-endian
//    type: uint8          // One of FULL, FIRST, MIDDLE, LAST
//    data: uint8[length]

use super::{Error, Result};
use std::io::{self, Cursor, Read, Write};

use bytes::Buf;
use crc::{Crc, CRC_32_ISCSI};

const RECORD_BLOCK_SIZE: usize = 32 * 1024; // 32kb for every block
const RECORD_HEADER_SIZE: usize = 1 + 2 + 4; // record_type + record_len + record_checksum
const ZERO_BYTES: &[u8] = b"\x00\x00\x00\x00\x00\x00";

// RecordType indicate the record type.
#[derive(Debug, PartialEq, Eq)]
pub enum RecordType {
    Illegal = 0,
    // Full record contains the full contents of entire record
    Full = 1,
    // First, Middle, Last record contains the fragments of single record
    First = 2,
    Middle = 3,
    Last = 4,
}

impl From<u8> for RecordType {
    fn from(n: u8) -> Self {
        match n {
            1 => RecordType::Full,
            2 => RecordType::First,
            3 => RecordType::Middle,
            4 => RecordType::Last,
            _ => RecordType::Illegal,
        }
    }
}

pub struct RecordReader<R: Read> {
    reader: R,
    buffer: Vec<u8>,
    offset: usize,
    crc: Crc<u32>,
}

impl<R: Read> RecordReader<R> {
    pub fn new(reader: R) -> Self {
        let mut r = RecordReader {
            reader,
            buffer: Vec::with_capacity(RECORD_BLOCK_SIZE),
            offset: 0,
            crc: Crc::<u32>::new(&CRC_32_ISCSI),
        };
        r.buffer.resize(RECORD_BLOCK_SIZE, b'\x00');
        r.offset = r.buffer.len();

        // buffer size mut be RECORD_BLOCK_SIZE
        debug_assert_eq!(r.buffer.len(), RECORD_BLOCK_SIZE);
        r
    }

    fn parse_record(&mut self) -> Result<Option<(RecordType, &[u8])>> {
        debug_assert!(self.offset <= self.buffer.len());
        if self.buffer.len() - self.offset < RECORD_HEADER_SIZE {
            // Skip the empty tailer of block
            self.fill_buffer_with_new_block()?;
            // Reaching the EOF or discovering incomplete record is expected log format, return None
            if self.buffer.len() < RECORD_HEADER_SIZE {
                return Ok(None);
            }
        }

        // Parse record type, illegal record type means illegal log format
        let record_type: RecordType = self.buffer[self.offset + 6].into();
        if record_type == RecordType::Illegal {
            return Err(Error::IllegalLogRecord);
        }

        // Parse length of data, exceeded length means illegal log format
        let length = u16::from_le_bytes(
            (&self.buffer[self.offset + 4..self.offset + 6])
                .try_into()
                .unwrap(),
        ) as usize;
        let data_right_boundary = self.offset + RECORD_HEADER_SIZE + length;
        if data_right_boundary > self.buffer.len() {
            // Discovering incomplete record at eof, return None.
            if self.buffer.len() < RECORD_BLOCK_SIZE {
                return Ok(None);
            }
            return Err(Error::IllegalLogRecord);
        }
        let data = &self.buffer[self.offset + RECORD_HEADER_SIZE..data_right_boundary];

        // Check checksum
        let checksum = u32::from_le_bytes(
            (&self.buffer[self.offset..self.offset + 4])
                .try_into()
                .unwrap(),
        );
        if self.crc.checksum(data) != checksum {
            return Err(Error::IllegalLogRecord);
        }

        self.offset += length + RECORD_HEADER_SIZE;
        Ok(Some((record_type, data)))
    }

    // Fill a new block into buffer
    fn fill_buffer_with_new_block(&mut self) -> Result<()> {
        // Clear the buffer and fill a new block
        let mut last: usize = 0;
        loop {
            match self.reader.read(&mut self.buffer[last..]) {
                Ok(n) if n == 0 => {
                    // Reach EOF
                    unsafe {
                        self.buffer.set_len(last);
                    }
                    break;
                }
                Ok(n) => {
                    last += n;
                    debug_assert!(last <= self.buffer.len());
                    if last == self.buffer.len() {
                        // Buffer is filled
                        break;
                    }
                    // Continue to fill buffer if buffer has empty space
                }
                Err(e) if matches!(e.kind(), io::ErrorKind::Interrupted) => {
                    // Continue to fill buffer if io is interrupted
                }
                Err(e) => Err(e)?,
            }
        }
        // Reset the buffer offset
        self.offset = 0;
        Ok(())
    }
}

impl<R: Read> Iterator for RecordReader<R> {
    type Item = Result<Vec<u8>>;

    fn next(&mut self) -> Option<Self::Item> {
        let mut content = Vec::new();
        let mut prev_record_type = None;
        loop {
            // Parse new record
            match self.parse_record() {
                Ok(Some((record_type, data))) => {
                    match record_type {
                        // Full record, just return full data
                        RecordType::Full => {
                            // Full record prev record type is not exist
                            if prev_record_type.is_some() {
                                return Some(Err(Error::IllegalLogRecord));
                            }
                            return Some(Ok(Vec::from(data)));
                        }
                        RecordType::First => {
                            // First record prev record type is not exist
                            if prev_record_type.is_some() {
                                return Some(Err(Error::IllegalLogRecord));
                            }
                            content.reserve(2 * data.len());
                            content.extend_from_slice(data);
                            prev_record_type = Some(RecordType::First);
                        }
                        RecordType::Middle => {
                            // Middle prev record type must be First or Middle
                            if !matches!(
                                prev_record_type,
                                Some(RecordType::First) | Some(RecordType::Middle)
                            ) {
                                return Some(Err(Error::IllegalLogRecord));
                            }
                            content.extend_from_slice(data);
                            prev_record_type = Some(RecordType::Middle);
                        }
                        RecordType::Last => {
                            // Last prev record type must be First or Middle
                            if !matches!(
                                prev_record_type,
                                Some(RecordType::First) | Some(RecordType::Middle)
                            ) {
                                return Some(Err(Error::IllegalLogRecord));
                            }
                            content.extend_from_slice(data);
                            return Some(Ok(content));
                        }
                        _ => unreachable!("never be illegal type"),
                    }
                }
                Ok(None) => {
                    // Reach EOF or ignore incomplete record
                    return None;
                }
                Err(err) => return Some(Err(err)),
            }
        }
    }
}

// Implement a binary log Writer
pub struct RecordWriter<W: Write> {
    writer: W,
    block_offset: usize,
    total_written: usize,
    crc: Crc<u32>,
}

impl<W: Write> RecordWriter<W> {
    pub fn new(file: W) -> Self {
        RecordWriter {
            writer: file,
            block_offset: 0,
            total_written: 0,
            crc: Crc::<u32>::new(&CRC_32_ISCSI),
        }
    }

    // Get total writen bytes count
    pub fn written(&self) -> usize {
        self.total_written
    }

    // Append data into record file
    pub fn append(&mut self, data: Vec<u8>) -> Result<()> {
        let mut cursor = Cursor::new(data);

        let mut is_first = true;
        loop {
            debug_assert!(self.block_offset <= RECORD_BLOCK_SIZE);
            let rest = RECORD_BLOCK_SIZE - self.block_offset;

            if rest < RECORD_HEADER_SIZE {
                // Append tailer and switch to new block if rest space of this block is not enough to append non-empty record
                if rest > 0 {
                    debug_assert_eq!(RECORD_HEADER_SIZE, 7);
                    self.writer.write_all(&ZERO_BYTES[0..rest])?;
                    self.total_written += rest;
                }
                self.block_offset = 0;
            }

            // Get fragment size
            debug_assert!(self.block_offset + RECORD_HEADER_SIZE <= RECORD_BLOCK_SIZE);
            let available_space = RECORD_BLOCK_SIZE - self.block_offset - RECORD_HEADER_SIZE;

            let fragment_len = if cursor.remaining() < available_space {
                cursor.remaining()
            } else {
                available_space
            };

            // Check if need to split
            let is_last = fragment_len == cursor.remaining();
            let record_type = if is_first && is_last {
                RecordType::Full
            } else if is_first {
                RecordType::First
            } else if is_last {
                RecordType::Last
            } else {
                RecordType::Middle
            };

            // Create a new record to file
            self.encode_record(record_type, &cursor.chunk()[0..fragment_len])?;
            cursor.advance(fragment_len);
            is_first = false;

            if !cursor.has_remaining() {
                break;
            }
        }

        // Flush data to persistent record
        self.writer.flush()?;

        Ok(())
    }

    // Encode a new record into record file
    fn encode_record(&mut self, t: RecordType, data: &[u8]) -> Result<()> {
        let length = data.len();

        // Assert numbers is not overflow in record HEADER format
        debug_assert!(length <= 0xffff);

        // Create a header layout
        // checksum: 4 bytes.
        // length: 2 bytes.
        // record type: 1 bytes.
        let checksum_bytes = self.crc.checksum(data).to_le_bytes();
        let length_bytes = length.to_le_bytes();
        let header: [u8; RECORD_HEADER_SIZE] = [
            checksum_bytes[0],
            checksum_bytes[1],
            checksum_bytes[2],
            checksum_bytes[3],
            length_bytes[0],
            length_bytes[1],
            t as u8,
        ];

        // Write a new record into file
        self.writer.write_all(&header[..])?;
        self.writer.write_all(data)?;

        // Update block size
        self.block_offset += RECORD_HEADER_SIZE + length;
        self.total_written += RECORD_HEADER_SIZE + length;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use rand::{Rng, RngCore};

    use super::*;

    #[test]
    fn test_read_writer_with_random_case() {
        let test_case = generate_random_bytes_vec(1000);

        // write to buffer
        let v = Vec::new();
        let mut w = RecordWriter::new(v);
        for bytes in test_case.iter() {
            assert!(w.append(bytes.clone()).is_ok());
        }

        // test complete size
        let v = Cursor::new(&w.writer[..]);
        let r = RecordReader::new(v);
        let mut total = 0;
        for (idx, elem) in r.enumerate() {
            total += 1;
            assert_eq!(&elem.unwrap()[..], test_case[idx]);
        }
        assert_eq!(total, test_case.len());

        // test tail lost case
        let v = Cursor::new(&w.writer[0..&w.writer.len() - 7]);
        let r = RecordReader::new(v);

        let mut total = 0;
        for (idx, elem) in r.enumerate() {
            total += 1;
            assert_eq!(&elem.unwrap()[..], test_case[idx]);
        }
        assert_eq!(total, test_case.len() - 1);
    }

    fn generate_random_bytes_vec(num: usize) -> Vec<Vec<u8>> {
        let mut v = Vec::with_capacity(num);

        for _ in 0..num {
            let mut rng = rand::thread_rng();
            let size: usize = rng.gen_range(0..10 * RECORD_BLOCK_SIZE);
            let mut new_bytes = vec![0; size];
            rng.fill_bytes(&mut new_bytes[..]);
            v.push(new_bytes);
        }
        v
    }
}
