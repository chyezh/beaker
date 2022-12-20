use super::Result;
use crc::{Crc, CRC_32_ISCSI};
use std::io::{Read, Seek, SeekFrom};

static CRC_INSTANCE: Crc<u32> = Crc::<u32>::new(&CRC_32_ISCSI);

// Seek reader and read n exact bytes
pub fn seek_and_read_buf<R: Read + Seek>(
    reader: &mut R,
    seek: SeekFrom,
    n: usize,
) -> Result<Vec<u8>> {
    reader.seek(seek)?;
    let mut buffer = Vec::with_capacity(n);
    buffer.resize(n, b'\x00');

    read_exact(reader, &mut buffer)?;
    Ok(buffer)
}

// Read exact bytes(buffer.len()) from reader into buffer.
// When EOF is reached, resize buffer into bytes len that already read.
pub fn read_exact<R: Read>(reader: &mut R, buffer: &mut Vec<u8>) -> Result<()> {
    let mut last: usize = 0;
    let buffer_size = buffer.len();
    loop {
        match reader.read(&mut buffer[last..]) {
            Ok(n) if n == 0 => {
                // Reach EOF
                buffer.resize(last, b'\x00');
                break;
            }
            Ok(n) => {
                last += n;
                debug_assert!(last <= buffer_size);
                if last == buffer_size {
                    // Buffer is full filled
                    break;
                }
                // Continue to fill buffer if buffer has empty space
            }
            Err(e) if matches!(e.kind(), std::io::ErrorKind::Interrupted) => {
                // Continue to fill buffer if io is interrupted
            }
            Err(e) => Err(e)?,
        }
    }
    // Reset the buffer offset
    Ok(())
}

// Checksum
pub fn checksum(data: &[u8]) -> u32 {
    CRC_INSTANCE.checksum(data)
}

pub fn from_le_bytes_64(data: &[u8]) -> u64 {
    debug_assert_eq!(data.len(), 8);
    u64::from_le_bytes(data.try_into().unwrap())
}

pub fn from_le_bytes_32(data: &[u8]) -> usize {
    debug_assert_eq!(data.len(), 4);
    u32::from_le_bytes(data.try_into().unwrap()) as usize
}
