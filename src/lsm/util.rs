use bytes::Bytes;
use std::io::Write;

use super::{Error, Result};

const LIVING_VALUE_TYPE: u8 = 1;
const TOMBSTONE_VALUE_TYPE: u8 = 0;

// LSM-Tree value
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum Value {
    Living(Bytes),

    // Deleted value
    Tombstone,
}

impl Value {
    pub fn living_static(data: &'static [u8]) -> Self {
        Value::Living(Bytes::from_static(data))
    }

    pub fn living_raw(data: &[u8]) -> Self {
        Value::Living(Bytes::copy_from_slice(data))
    }

    pub fn living(data: Bytes) -> Self {
        Value::Living(data)
    }

    pub fn tombstone() -> Self {
        Value::Tombstone
    }

    pub fn to_bytes(&self) -> Bytes {
        let mut v = Vec::with_capacity(self.encode_bytes_len());
        self.encode_to(&mut v);
        v.into()
    }

    // Parse value from bytes and return the rest bytes
    pub fn decode_from_bytes(data: Bytes) -> Result<Self> {
        debug_assert!(!data.is_empty());
        if data.is_empty() {
            return Err(Error::IllegalValueBinary);
        }

        match data[0] {
            LIVING_VALUE_TYPE => Ok(Value::Living(data.slice(1..))),
            TOMBSTONE_VALUE_TYPE => {
                if data.len() != 1 {
                    return Err(Error::IllegalValueBinary);
                }
                Ok(Value::Tombstone)
            }
            _ => Err(Error::IllegalValueBinary),
        }
    }

    //  Get pre-alloc bytes space for encode_to api
    #[inline]
    pub fn encode_bytes_len(&self) -> usize {
        match self {
            Value::Tombstone => 1,
            Value::Living(bytes) => 1 + bytes.len(),
        }
    }

    // Encode into bytes and write to given Write
    pub fn encode_to<W: Write>(&self, mut w: W) -> Result<()> {
        match self {
            Value::Tombstone => w.write_all(&[TOMBSTONE_VALUE_TYPE])?,
            Value::Living(data) => {
                w.write_all(&[LIVING_VALUE_TYPE])?;
                w.write_all(&data[..])?;
            }
        };
        Ok(())
    }
}
