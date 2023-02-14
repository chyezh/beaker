use super::{Error, Result};
use crate::util::from_le_bytes_32;
use bytes::Bytes;
use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};

const LIVING_META_VALUE_TYPE: u8 = 3;
const TOMBSTONE_META_VALUE_TYPE: u8 = 2;
const LIVING_VALUE_TYPE: u8 = 1;
const TOMBSTONE_VALUE_TYPE: u8 = 0;

const LIVING_META_VALUE_TYPE_HEADER: usize = 5;
const OTHER_VALUE_TYPE_HEADER: usize = 1;

// LSM-Tree value
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum Value {
    Living(Bytes),

    // Deleted value
    Tombstone,

    // Meta data is promised to live on log, but not on sstable
    LivingMeta(Bytes, Bytes),

    TombstoneMeta(Bytes),
}

impl Value {
    #[inline]
    pub fn to_bytes(&self) -> Bytes {
        let mut v = Vec::with_capacity(self.encode_bytes_len());
        self.encode_to(&mut v).ok();
        v.into()
    }

    // Parse value from bytes and return the rest bytes
    pub fn decode_from_bytes(data: Bytes) -> Result<Self> {
        debug_assert!(!data.is_empty());
        if data.is_empty() {
            return Err(Error::IllegalValueBinary);
        }

        match data[0] {
            LIVING_VALUE_TYPE => Ok(Self::Living(data.slice(OTHER_VALUE_TYPE_HEADER..))),
            TOMBSTONE_VALUE_TYPE => {
                if data.len() != OTHER_VALUE_TYPE_HEADER {
                    return Err(Error::IllegalValueBinary);
                }
                Ok(Self::Tombstone)
            }
            TOMBSTONE_META_VALUE_TYPE => {
                Ok(Self::TombstoneMeta(data.slice(OTHER_VALUE_TYPE_HEADER..)))
            }
            LIVING_META_VALUE_TYPE => {
                if data.len() < LIVING_META_VALUE_TYPE_HEADER {
                    return Err(Error::IllegalValueBinary);
                }
                let data_len = from_le_bytes_32(&data[1..5]);
                let meta_begin = data_len + LIVING_META_VALUE_TYPE_HEADER;
                if data.len() < meta_begin {
                    return Err(Error::IllegalValueBinary);
                }
                Ok(Self::LivingMeta(
                    data.slice(LIVING_META_VALUE_TYPE_HEADER..meta_begin),
                    data.slice(meta_begin..),
                ))
            }
            _ => Err(Error::IllegalValueBinary),
        }
    }

    //  Get pre-alloc bytes space for encode_to api
    #[inline]
    pub fn encode_bytes_len(&self) -> usize {
        match self {
            Self::Tombstone => OTHER_VALUE_TYPE_HEADER,
            Self::Living(bytes) => OTHER_VALUE_TYPE_HEADER + bytes.len(),
            Self::TombstoneMeta(meta) => OTHER_VALUE_TYPE_HEADER + meta.len(),
            Self::LivingMeta(bytes, meta) => {
                LIVING_META_VALUE_TYPE_HEADER + bytes.len() + meta.len()
            }
        }
    }

    // Encode into bytes and write to given Write
    pub fn encode_to<W: Write>(&self, mut w: W) -> Result<()> {
        match self {
            Self::Tombstone => w.write_all(&[TOMBSTONE_VALUE_TYPE])?,
            Self::Living(data) => {
                w.write_all(&[LIVING_VALUE_TYPE])?;
                w.write_all(&data[..])?;
            }
            Self::TombstoneMeta(meta) => {
                w.write_all(&[TOMBSTONE_META_VALUE_TYPE])?;
                w.write_all(&meta[..])?;
            }
            Self::LivingMeta(data, meta) => {
                w.write_all(&[LIVING_META_VALUE_TYPE])?;
                w.write_all(&data.len().to_le_bytes()[0..4])?;
                w.write_all(data)?;
                w.write_all(meta)?;
            }
        };
        Ok(())
    }

    #[inline]
    pub fn is_tombstone(&self) -> bool {
        matches!(self, Self::Tombstone | Self::TombstoneMeta(_))
    }
}

// Scan given directory and get all sorted file with given extension
pub fn scan_sorted_file_at_path(path: &Path, extension: &str) -> Result<Vec<(PathBuf, u64)>> {
    // example of log file name format: 1.log 2.log
    let mut filenames: Vec<_> = fs::read_dir(path)?
        .flat_map(|elem| -> Result<PathBuf> { Ok(elem?.path()) }) // filter read_dir failed paths
        .filter_map(|elem| {
            if !elem.is_file() || elem.extension() != Some(extension.as_ref()) {
                None
            } else if let Some(Ok(log_num)) = elem
                .file_stem()
                .and_then(std::ffi::OsStr::to_str)
                .map(str::parse::<u64>)
            {
                Some((elem, log_num))
            } else {
                None
            }
        }) // filter illegal filename and get log seq no
        .collect();

    // sort filenames with log seq no
    filenames.sort_by_key(|elem| elem.1);
    Ok(filenames)
}

// Scan given directory and get all file with given extension
pub async fn async_scan_file_at_path(path: &Path, extension: &str) -> Result<Vec<(PathBuf, u64)>> {
    let mut filenames: Vec<(PathBuf, u64)> = Vec::with_capacity(32);

    let mut reader = tokio::fs::read_dir(&path).await?;
    while let Ok(Some(entry)) = reader.next_entry().await {
        // Parse sequence id from filename
        let filepath = entry.path();
        if !filepath.is_file() || filepath.extension() != Some(extension.as_ref()) {
            continue;
        }

        if let Some(Ok(seq)) = filepath
            .file_stem()
            .and_then(std::ffi::OsStr::to_str)
            .map(str::parse::<u64>)
        {
            filenames.push((filepath, seq));
        }
    }

    Ok(filenames)
}
