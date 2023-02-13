use std::io::SeekFrom;
use std::marker::Unpin;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncSeek, AsyncSeekExt};

// async reader and read n exact bytes
pub async fn seek_and_read_buf<R: AsyncRead + AsyncSeek + Unpin>(
    reader: &mut R,
    seek: SeekFrom,
    n: usize,
) -> std::io::Result<Vec<u8>> {
    reader.seek(seek).await?;
    let mut buffer = Vec::with_capacity(n);
    buffer.resize(n, b'\x00');

    read_exact(reader, &mut buffer).await?;
    Ok(buffer)
}

// Async read exact bytes(buffer.len()) from reader into buffer.
// When EOF is reached, resize buffer into bytes len that already read.
pub async fn read_exact<R: AsyncRead + Unpin>(
    reader: &mut R,
    buffer: &mut Vec<u8>,
) -> std::io::Result<()> {
    let mut last: usize = 0;
    let buffer_size = buffer.len();
    loop {
        match reader.read(&mut buffer[last..]).await {
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
