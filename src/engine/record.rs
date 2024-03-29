// Write-ahead-log format, reader and writer implementation.
// Reference to leveldb log format implementation.
//  block := record* trailer?
//  record :=
//    checksum: uint32     // crc32c of type and data[] ; little-endian
//    length: uint16       // little-endian
//    type: uint8          // One of FULL, FIRST, MIDDLE, LAST
//    data: uint8[length]

use crate::util::{checksum, from_le_bytes_32, read_exact};

use super::{Error, Result};
use std::io::{Cursor, Read, Write};

use bytes::{Buf, Bytes};

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

// Read and parse item from given bytes stream, implemented as a Iterator
pub struct RecordReader<R: Read> {
    reader: R,
    buffer: Vec<u8>,
    offset: usize,
}

impl<R: Read> RecordReader<R> {
    pub fn new(reader: R) -> Self {
        let mut r = RecordReader {
            reader,
            buffer: Vec::with_capacity(RECORD_BLOCK_SIZE),
            offset: 0,
        };
        r.buffer.resize(RECORD_BLOCK_SIZE, b'\x00');
        r.offset = r.buffer.len();

        // buffer size mut be RECORD_BLOCK_SIZE
        debug_assert_eq!(r.buffer.len(), RECORD_BLOCK_SIZE);
        r
    }

    // Parse a new record, act as a cursor
    fn parse_record(&mut self) -> Result<Option<(RecordType, &[u8])>> {
        debug_assert!(self.offset <= self.buffer.len());
        if self.buffer.len() - self.offset < RECORD_HEADER_SIZE {
            // Skip the empty tailer of block and fill the buffer
            read_exact(&mut self.reader, &mut self.buffer)?;
            self.offset = 0;

            // Reaching the EOF or discovering incomplete record is expected log format, return None
            if self.buffer.len() < RECORD_HEADER_SIZE {
                return Ok(None);
            }
        }

        // Parse record type, illegal record type means illegal log format
        let record_type: RecordType = self.buffer[self.offset + 6].into();
        if record_type == RecordType::Illegal {
            return Err(Error::IllegalRecord);
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
            return Err(Error::IllegalRecord);
        }
        let data = &self.buffer[self.offset + RECORD_HEADER_SIZE..data_right_boundary];

        // Check checksum
        let checksum_result = from_le_bytes_32(&self.buffer[self.offset..self.offset + 4]) as u32;
        if checksum(data) != checksum_result {
            return Err(Error::IllegalRecord);
        }

        self.offset += length + RECORD_HEADER_SIZE;
        Ok(Some((record_type, data)))
    }
}

impl<R: Read> Iterator for RecordReader<R> {
    type Item = Result<Bytes>;

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
                                return Some(Err(Error::IllegalRecord));
                            }
                            return Some(Ok(Bytes::copy_from_slice(data)));
                        }
                        RecordType::First => {
                            // First record prev record type is not exist
                            if prev_record_type.is_some() {
                                return Some(Err(Error::IllegalRecord));
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
                                return Some(Err(Error::IllegalRecord));
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
                                return Some(Err(Error::IllegalRecord));
                            }
                            content.extend_from_slice(data);
                            return Some(Ok(Bytes::from(content)));
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
#[derive(Debug)]
pub struct RecordWriter<W: Write> {
    writer: W,
    block_offset: usize,
    total_written: usize,
}

impl<W: Write> RecordWriter<W> {
    pub fn new(file: W) -> Self {
        RecordWriter {
            writer: file,
            block_offset: 0,
            total_written: 0,
        }
    }

    // Get total writen bytes count
    #[inline]
    pub fn written(&self) -> usize {
        self.total_written
    }

    // Append data into record file and return the total written count
    pub fn append(&mut self, data: Bytes) -> Result<usize> {
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
        Ok(self.written())
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
        let checksum_bytes = checksum(data).to_le_bytes();
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
    use super::*;
    use crate::util::test_case::generate_random_bytes;

    #[test]
    fn test_read_writer_with_random_case() {
        let test_case = generate_random_bytes(1000, 10 * RECORD_BLOCK_SIZE);

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
}
