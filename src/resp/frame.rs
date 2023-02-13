use super::{Error, Result};
use bytes::{Buf, Bytes};
use std::io::{Cursor, Write};
use tokio::io::{AsyncWrite, AsyncWriteExt};

pub trait AsFrame {
    fn as_frame(&self) -> Frame;
}

/// An enum for RESP frame
#[derive(Debug, Clone, PartialEq)]
pub enum Frame {
    Simple(String),
    Error(String),
    Integer(usize),
    Bulk(Bytes),
    Array(Vec<Frame>),
    Null,
}

impl Frame {
    /// Create a new array Frame
    pub fn array() -> Frame {
        Frame::Array(vec![])
    }

    /// Push bulk string into Frame
    ///
    /// # Panics
    ///
    /// panics if `self` is not array
    pub fn push_bulk(&mut self, bytes: Bytes) {
        match self {
            Frame::Array(vec) => {
                vec.push(Frame::Bulk(bytes));
            }
            _ => unreachable!("push bulk must be operated on Frame::array"),
        }
    }

    /// Push integer into Frame
    ///
    /// # Panics
    ///
    /// panics if `self` is not array
    pub fn push_integer(&mut self, value: usize) {
        match self {
            Frame::Array(vec) => vec.push(Frame::Integer(value)),
            _ => unreachable!("push integer must be operated on Frame::array"),
        }
    }

    /// Check if the message can parse success
    pub fn check(src: &mut Cursor<&[u8]>) -> Result<()> {
        match get_u8(src)? {
            b'+' | b'-' => {
                get_string_from_line(src)?;
                Ok(())
            }
            b':' => {
                get_integer_from_line(src)?;
                Ok(())
            }
            b'$' => {
                if peek_u8(src)? == b'-' {
                    // skip '-1\r\n'
                    skip(src, 4)
                } else {
                    let len = get_integer_from_line(src)?;

                    // skip len of bytes and \r\n
                    skip(src, len + 2)
                }
            }
            b'*' => {
                let len = get_integer_from_line(src)?;

                for _ in 0..len {
                    Frame::check(src)?;
                }

                Ok(())
            }
            _ => Err(Error::Invalid)?,
        }
    }

    /// Parse a new `Frame` if the message has already `check` success
    pub fn parse(src: &mut Cursor<&[u8]>) -> Result<Frame> {
        match get_u8(src)? {
            b'+' => Ok(Frame::Simple(get_string_from_line(src)?)),
            b'-' => Ok(Frame::Error(get_string_from_line(src)?)),
            b':' => Ok(Frame::Integer(get_integer_from_line(src)?)),
            b'$' => {
                if peek_u8(src)? == b'-' {
                    if get_line(src)? != b"-1" {
                        Err(Error::Invalid)?;
                    }

                    Ok(Frame::Null)
                } else {
                    // read length of bulk string
                    let len = get_integer_from_line(src)?;
                    let n = len + 2;

                    // check if buffer is complete
                    if src.remaining() < n {
                        Err(Error::Incomplete)?;
                    }

                    // copy the bytes and skip
                    let bytes = Bytes::copy_from_slice(&src.chunk()[..len]);
                    skip(src, n)?;

                    Ok(Frame::Bulk(bytes))
                }
            }
            b'*' => {
                let len = get_integer_from_line(src)?;
                let mut arr = Vec::with_capacity(len);

                for _ in 0..len {
                    arr.push(Frame::parse(src)?);
                }
                Ok(Frame::Array(arr))
            }
            _ => Err(Error::Invalid)?,
        }
    }

    pub async fn write_to<W: AsyncWrite + Unpin>(&self, w: &mut W) -> Result<()> {
        match self {
            Frame::Array(val) => {
                w.write_u8(b'*').await?;
                write_integer_to(val.len() as u64, w).await?;
                for entry in val {
                    entry.write_single_to(w).await?;
                }
            }
            val => {
                val.write_single_to(w).await?;
            }
        }

        Ok(())
    }

    async fn write_single_to<W: AsyncWrite + Unpin>(&self, w: &mut W) -> Result<()> {
        match self {
            Frame::Simple(val) => {
                w.write_u8(b'+').await?;
                w.write_all(val.as_bytes()).await?;
                w.write_all(b"\r\n").await?;
            }
            Frame::Error(val) => {
                w.write_u8(b'-').await?;
                w.write_all(val.as_bytes()).await?;
                w.write_all(b"\r\n").await?;
            }
            Frame::Integer(val) => {
                w.write_u8(b':').await?;
                write_integer_to(*val as u64, w).await?;
            }
            Frame::Null => {
                w.write_all(b"$-1\r\n").await?;
            }
            Frame::Bulk(val) => {
                let len = val.len();

                w.write_u8(b'$').await?;
                write_integer_to(len as u64, w).await?;
                w.write_all(val).await?;
                w.write_all(b"\r\n").await?;
            }
            Frame::Array(_) => unreachable!(),
        }
        Ok(())
    }
}

impl From<String> for Frame {
    fn from(value: String) -> Self {
        Frame::Simple(value)
    }
}

// Convert bytes into frame bulk
impl From<Bytes> for Frame {
    fn from(value: Bytes) -> Self {
        Frame::Bulk(value)
    }
}

async fn write_integer_to<W: AsyncWriteExt + Unpin>(val: u64, w: &mut W) -> Result<()> {
    let mut buf = [0u8; 20];
    let mut buf = Cursor::new(&mut buf[..]);
    write!(&mut buf, "{}", val)?;

    let pos = buf.position() as usize;
    w.write_all(&buf.get_ref()[..pos]).await?;
    w.write_all(b"\r\n").await?;

    Ok(())
}

/// get a new line, convert it into usize and advance
fn get_integer_from_line(src: &mut Cursor<&[u8]>) -> Result<usize> {
    use atoi::atoi;
    let vec = get_line(src)?;
    atoi::<usize>(vec).ok_or(Error::Invalid)
}

/// get a new line, convert it into utf8 and advance
fn get_string_from_line(src: &mut Cursor<&[u8]>) -> Result<String> {
    String::from_utf8((get_line(src)?).to_vec()).map_err(|_| Error::Invalid)
}

/// get a new line and advance
fn get_line<'a>(src: &mut Cursor<&'a [u8]>) -> Result<&'a [u8]> {
    let begin = src.position() as usize;
    let end = src.get_ref().len() - 1;

    for i in begin..end {
        if src.get_ref()[i] == b'\r' && src.get_ref()[i + 1] == b'\n' {
            src.set_position((i + 2) as u64);

            return Ok(&src.get_ref()[begin..i]);
        }
    }

    Err(Error::Incomplete)?
}

/// Find a u8 and do not advance. return Err if cursor reach end
fn peek_u8(src: &Cursor<&[u8]>) -> Result<u8> {
    if !src.has_remaining() {
        Err(Error::Incomplete)?;
    }
    Ok(src.chunk()[0])
}

/// Find a u8 and advance. return Err if cursor reach end
fn get_u8(src: &mut Cursor<&[u8]>) -> Result<u8> {
    if !src.has_remaining() {
        Err(Error::Incomplete)?;
    }
    Ok(src.get_u8())
}

fn skip(src: &mut Cursor<&[u8]>, n: usize) -> Result<()> {
    if src.remaining() < n {
        Err(Error::Incomplete)?;
    }

    src.advance(n);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resp_item_serialize() {
        let rt = tokio::runtime::Runtime::new().unwrap();

        let test_frame_write = |x: Frame, d: Vec<u8>| async move {
            let mut data = Vec::new();
            x.write_to(&mut data).await.unwrap();
            assert_eq!(&data[..], &d[..]);
        };

        for (frame, v) in vec![
            (Frame::Simple(String::from("OK")), b"+OK\r\n".to_vec()),
            (
                Frame::Error(String::from(
                    "WRONGTYPE Operation against a key holding the wrong kind of value",
                )),
                b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n".to_vec(),
            ),
            (Frame::Integer(1000), b":1000\r\n".to_vec()),
            (
                Frame::Bulk(Bytes::from("hello world\r")),
                b"$12\r\nhello world\r\r\n".to_vec(),
            ),
            (Frame::Null, b"$-1\r\n".to_vec()),
            (
                Frame::Array(vec![
                    Frame::Integer(1),
                    Frame::Integer(2),
                    Frame::Integer(3),
                    Frame::Null,
                    Frame::Bulk(Bytes::from("hello")),
                ]),
                b"*5\r\n:1\r\n:2\r\n:3\r\n$-1\r\n$5\r\nhello\r\n".to_vec(),
            ),
        ] {
            rt.block_on(test_frame_write(frame, v));
        }
    }

    #[test]
    fn resp_item_unserialize() {
        let data: Vec<u8> = b"*5\r\n:1\r\n:2\r\n:3\r\n$-1\r\n$5\r\nhello\r\n*6\r\n:1\r\n:2\r\n:3\r\n$-1\r\n$5\r\nhello\r\n$-1\r\n".to_vec();

        let mut cursor1 = Cursor::new(&data[..]);
        let mut cursor2 = Cursor::new(&data[..]);

        assert!(Frame::check(&mut cursor1).is_ok());
        let v = Frame::parse(&mut cursor2).unwrap();

        assert_eq!(
            v,
            Frame::Array(vec![
                Frame::Integer(1),
                Frame::Integer(2),
                Frame::Integer(3),
                Frame::Null,
                Frame::Bulk(Bytes::from("hello")),
            ])
        );

        assert!(Frame::check(&mut cursor1).is_ok());
        let v = Frame::parse(&mut cursor2).unwrap();

        assert_eq!(
            v,
            Frame::Array(vec![
                Frame::Integer(1),
                Frame::Integer(2),
                Frame::Integer(3),
                Frame::Null,
                Frame::Bulk(Bytes::from("hello")),
                Frame::Null,
            ])
        );
    }
}
