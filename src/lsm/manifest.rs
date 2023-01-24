use bytes::Bytes;
use uuid::Uuid;

use crate::lsm::record::RecordReader;
use crate::util::{from_le_bytes_32, from_le_bytes_64};

use super::record::RecordWriter;
use super::util::scan_sorted_file_at_path;
use super::{Error, Result};
use parking_lot::{Mutex, MutexGuard, RwLock, RwLockUpgradableReadGuard};
use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Arc;

const SSTABLE_FILE_EXTENSION: &str = "tbl";
const MANIFEST_FILE_EXTENSION: &str = "manifest";
const MINIMUM_SSTABLE_ENTRY_SIZE: usize = 28;
const MINIMUM_MANIFEST_INNER_SIZE: usize = 8;
const SPLIT_MANIFEST_SIZE_THRESHOLD: usize = 32 * 1024 * 1024; // 32MB

struct SSTableEntry {
    lv: usize,
    uid: Uuid,
    root_path: PathBuf,
    compact_lock: Mutex<()>,
    range: (Bytes, Bytes),
}

impl SSTableEntry {
    // Open a sstable reader for this file
    pub fn open_reader(&self) -> Result<File> {
        let path = Self::file_path(self.root_path.clone(), &self.uid);
        let new_file = File::open(path)?;
        Ok(new_file)
    }

    // Open a sstable writer for this file
    pub fn open_writer(&self) -> Result<File> {
        // TODO: file_lock
        let path = Self::file_path(self.root_path.clone(), &self.uid);
        let new_file = OpenOptions::new()
            .truncate(true)
            .create(true)
            .write(true)
            .open(path)?;
        Ok(new_file)
    }

    pub fn set_range(&mut self, range: (Bytes, Bytes)) {
        debug_assert!(range.0 <= range.1);
        self.range = range;
    }

    // Get encode bytes length for pre-allocation
    #[inline]
    fn encode_bytes_len(&self) -> usize {
        MINIMUM_SSTABLE_ENTRY_SIZE + self.range.0.len() + self.range.1.len()
    }

    // Encode into bytes and write to given Write
    // 1. 0-4 lv
    // 2. 4-20 uuid
    // 3. length of range's left boundary
    // 4. left boundary
    // 5. length of range's right boundary
    // 6. right boundary
    fn encode_to<W: Write>(&self, mut w: W) -> Result<()> {
        w.write_all(&self.lv.to_le_bytes()[0..4])?;
        w.write_all(&self.uid.to_bytes_le())?;
        w.write_all(&self.range.0.len().to_le_bytes()[0..4])?;
        w.write_all(&self.range.0)?;
        w.write_all(&self.range.1.len().to_le_bytes()[0..4])?;
        w.write_all(&self.range.1)?;
        Ok(())
    }

    // Parse sstable entry from bytes
    fn decode_from_bytes(root_path: PathBuf, data: Bytes) -> Result<Self> {
        debug_assert!(data.len() >= MINIMUM_SSTABLE_ENTRY_SIZE);
        if data.len() < MINIMUM_SSTABLE_ENTRY_SIZE {
            return Err(Error::IllegalSSTableEntry);
        }

        let lv = from_le_bytes_32(&data[0..4]);
        let uid = Uuid::from_bytes_le((&data[4..20]).try_into().unwrap());

        // Parsing range
        let left_boundary_len = from_le_bytes_32(&data[20..24]);
        debug_assert!(data.len() >= MINIMUM_SSTABLE_ENTRY_SIZE + left_boundary_len);
        if data.len() < MINIMUM_SSTABLE_ENTRY_SIZE + left_boundary_len {
            return Err(Error::IllegalSSTableEntry);
        }
        let right_boundary_len_start = 24 + left_boundary_len;
        let left_boundary = data.slice(24..right_boundary_len_start);

        let right_boundary_len =
            from_le_bytes_32(&data[right_boundary_len_start..right_boundary_len_start + 4]);
        debug_assert_eq!(
            data.len(),
            MINIMUM_SSTABLE_ENTRY_SIZE + left_boundary_len + right_boundary_len
        );
        if data.len() != MINIMUM_SSTABLE_ENTRY_SIZE + left_boundary_len + right_boundary_len {
            return Err(Error::IllegalSSTableEntry);
        }
        let right_boundary = data
            .slice(right_boundary_len_start + 4..right_boundary_len_start + 4 + right_boundary_len);

        // Check if sstable file exist
        let file_path = Self::file_path(root_path.clone(), &uid);
        if !Self::file_path(root_path.clone(), &uid).is_file() {
            return Err(Error::IllegalSSTableEntry);
        }

        Ok(SSTableEntry {
            lv,
            uid,
            root_path,
            compact_lock: Mutex::new(()),
            range: (left_boundary, right_boundary),
        })
    }

    // Try to lock compact process
    fn try_compact_lock(&self) -> Option<MutexGuard<()>> {
        self.compact_lock.try_lock()
    }

    fn file_path(root_path: PathBuf, uid: &Uuid) -> PathBuf {
        let file_name = format!("{}.{}", uid, SSTABLE_FILE_EXTENSION);
        root_path.join(file_name)
    }

    fn sort_key(&self) -> Bytes {
        self.range.0.clone()
    }
}

#[derive(Clone)]
struct ManifestInner {
    version: u64,
    tables: Vec<Vec<Arc<SSTableEntry>>>,
}

impl ManifestInner {
    // Get encode bytes length for pre-allocation
    #[inline]
    fn encode_bytes_len(&self) -> usize {
        let mut length = MINIMUM_MANIFEST_INNER_SIZE;
        for table in self.tables.iter().flatten() {
            length += 4 + table.encode_bytes_len();
        }
        length
    }

    // 0-8: version
    // sstable entry length
    // sstable entry
    // ...
    fn encode_to<W: Write>(&self, mut w: W) -> Result<()> {
        w.write_all(&self.version.to_le_bytes()[0..8])?;
        for table in self.tables.iter().flatten() {
            w.write_all(&table.encode_bytes_len().to_le_bytes()[0..4])?;
            table.encode_to(&mut w)?;
        }
        Ok(())
    }

    // Parsing ManifestInner from bytes
    fn decode_from_bytes(root_path: PathBuf, data: Bytes) -> Result<Self> {
        debug_assert!(data.len() >= MINIMUM_MANIFEST_INNER_SIZE);
        if data.len() < MINIMUM_MANIFEST_INNER_SIZE {
            return Err(Error::IllegalManifest);
        }
        let version = from_le_bytes_64(&data[0..8]);
        let mut tables = Vec::new();

        let mut lv = 0;
        let mut remain = data.slice(8..);
        loop {
            // Parse a new tables
            if remain.is_empty() {
                break;
            }
            debug_assert!(remain.len() >= 4);
            if remain.len() < 4 {
                return Err(Error::IllegalManifest);
            }

            let sstable_len = from_le_bytes_32(&remain[0..4]);
            let sstable_end = 4 + sstable_len;
            debug_assert!(remain.len() >= sstable_end);
            if remain.len() < sstable_end {
                return Err(Error::IllegalManifest);
            }

            let entry = SSTableEntry::decode_from_bytes(
                root_path.clone(),
                remain.slice(4..4 + sstable_len),
            )?;

            // Check if level is valid
            // Level must be monotonic
            debug_assert!(entry.lv >= lv);
            if tables.len() <= entry.lv {
                tables.resize(entry.lv + 1, Vec::new());
            }

            // Update status
            remain = remain.slice(sstable_end..);
            lv = entry.lv;
            tables[entry.lv].push(Arc::new(entry));
        }

        Ok(ManifestInner { version, tables })
    }
}

struct ManifestPersister {
    log: RecordWriter<File>,
    root_path: PathBuf,
    next_log_seq: u64,
}

impl ManifestPersister {
    // Open persist file
    fn open(root_path: PathBuf, seq: u64) -> Result<Self> {
        let new_file = open_new_manifest(&root_path, seq)?;
        let log = RecordWriter::new(new_file);
        let next_log_seq = seq + 1;

        Ok(ManifestPersister {
            log,
            root_path,
            next_log_seq,
        })
    }

    // Persist given manifest info into file
    fn persist(&mut self, manifest: &ManifestInner) -> Result<()> {
        // Switch new file if needed
        let total_write = self.log.written();
        if total_write > SPLIT_MANIFEST_SIZE_THRESHOLD {
            self.log = RecordWriter::new(open_new_manifest(&self.root_path, self.next_log_seq)?);
            self.next_log_seq += 1;
        }

        // Write manifest information
        let mut buffer = Vec::with_capacity(manifest.encode_bytes_len());
        manifest.encode_to(&mut buffer);
        self.log.append(buffer.into())?;

        // TODO: clear old manifest file
        Ok(())
    }
}

#[derive(Clone)]
pub struct Manifest {
    inner: Arc<RwLock<ManifestInner>>,
    root_path: PathBuf,
    persister: Arc<Mutex<ManifestPersister>>,
}

impl Manifest {
    // Open a new manifest record
    pub fn open(root_path: impl Into<PathBuf>) -> Result<Self> {
        // Try to create manifest root directory
        let root_path: PathBuf = root_path.into();
        fs::create_dir_all(sstable_root_path(&root_path))?;
        fs::create_dir_all(manifest_root_path(&root_path))?;

        // Scan the manifest root path, find existed persistent manifest information
        let manifest_files = scan_sorted_file_at_path(
            manifest_root_path(&root_path).as_path(),
            MANIFEST_FILE_EXTENSION,
        )?;
        let (inner, next_log_seq) = if !manifest_files.is_empty() {
            // Read manifest in reverse order, find latest valid manifest snapshot
            let mut last_valid_manifest_binary: Option<Bytes> = None;
            for (path, _) in manifest_files.iter().rev() {
                let reader = RecordReader::new(File::open(path.as_path())?);
                for data in reader {
                    last_valid_manifest_binary = Some(data?);
                }
                if last_valid_manifest_binary.is_some() {
                    break;
                }
            }
            // No valid manifest found
            match last_valid_manifest_binary {
                Some(data) => {
                    // Parsing manifest inner information
                    let inner =
                        ManifestInner::decode_from_bytes(sstable_root_path(&root_path), data)?;
                    (inner, manifest_files.last().unwrap().1 + 1)
                }
                None => {
                    (
                        ManifestInner {
                            version: 0,
                            tables: vec![Vec::new()], // Create level 0 tables
                        },
                        0,
                    )
                }
            }
        } else {
            // Empty manifest root path, create new empty manifest
            (
                ManifestInner {
                    version: 0,
                    tables: vec![Vec::new()], // Create level 0 tables
                },
                0,
            )
        };
        // Open new persister
        let persister = ManifestPersister::open(manifest_root_path(&root_path), next_log_seq)?;

        // TODO: Add manifest cleaner
        Ok(Manifest {
            inner: Arc::new(RwLock::new(inner)),
            root_path,
            persister: Arc::new(Mutex::new(persister)),
        })
    }

    // Alloc a new sstable entry, and return its writer
    pub fn alloc_new_sstable_entry(&self) -> Result<SSTableEntry> {
        let uid = Uuid::new_v4();
        Ok(SSTableEntry {
            uid,
            root_path: sstable_root_path(&self.root_path),
            lv: 0,
            range: (Bytes::new(), Bytes::new()),
            compact_lock: Mutex::new(()),
        })
    }

    // Add a new sstable file into manifest
    pub fn add_new_sstable(&self, entry: SSTableEntry) -> Result<()> {
        debug_assert!(!entry.range.0.is_empty());
        debug_assert!(entry.range.0 <= entry.range.1);
        debug_assert_eq!(entry.lv, 0);

        let manifest = self.inner.upgradable_read();
        let mut new_manifest = manifest.clone();
        debug_assert!(!new_manifest.tables.is_empty());
        new_manifest.version += 1;
        new_manifest.tables[0].push(Arc::new(entry));

        // Persist manifest
        self.persister.lock().persist(&new_manifest)?;

        // Modify in-memory manifest
        let mut manifest = RwLockUpgradableReadGuard::upgrade(manifest);
        *manifest = new_manifest;

        Ok(())
    }

    // Generate a new compact task
    fn new_compact_task<'a>(&'a self) -> Option<CompactTask<'a>> {
        Some(CompactTask {
            low_lv: CompactTaskInput {
                tables: Vec::new(),
                lv: 0,
            },
            high_lv: CompactTaskInput {
                tables: Vec::new(),
                lv: 0,
            },
            new: Vec::new(),
            compact_size_threshold: 0,
            compact_locks: Vec::new(),
            manifest: self.clone(),
        })
    }
}

struct CompactTaskInput {
    tables: Vec<Arc<SSTableEntry>>,
    lv: usize,
}

struct CompactTask<'a> {
    pub low_lv: CompactTaskInput,           // compact low level inputs
    pub high_lv: CompactTaskInput,          // compact high level inputs
    pub new: Vec<SSTableEntry>,             // compact output
    pub compact_size_threshold: usize,      // compact output size threshold
    compact_locks: Vec<MutexGuard<'a, ()>>, // compact locks
    manifest: Manifest,
}

impl<'a> CompactTask<'a> {
    // Write down the compact result into manifest
    // Clear low_lv high_lv inputs, add new tables into high level layer
    pub fn finish(self) -> Result<()> {
        let manifest = self.manifest.inner.upgradable_read();
        debug_assert!(manifest.tables.len() > self.low_lv.lv); // Low level layer must exist
        debug_assert!(manifest.tables.len() >= self.high_lv.lv); // High level layer may be created at compaction

        // Clear outdate entry and add new entry in low or high level layer
        let low_lv_tables = Self::merge_tables(
            &manifest.tables[self.low_lv.lv],
            &self.low_lv.tables,
            Vec::new(),
        );
        let high_lv_tables = if manifest.tables.len() > self.high_lv.lv {
            Self::merge_tables(
                &manifest.tables[self.high_lv.lv],
                &self.high_lv.tables,
                self.new,
            )
        } else {
            let origin = Vec::new();
            Self::merge_tables(&origin, &self.high_lv.tables, self.new)
        };

        // Construct new tables
        let mut tables = Vec::with_capacity(manifest.tables.len() + 1);
        for lv in 0..self.low_lv.lv {
            tables.push(manifest.tables[lv].clone());
        }
        tables.push(low_lv_tables);
        if manifest.tables.len() > self.high_lv.lv {
            // No new layer
            for lv in self.low_lv.lv + 1..self.high_lv.lv {
                tables.push(manifest.tables[lv].clone());
            }
            tables.push(high_lv_tables);
            for lv in self.high_lv.lv + 1..manifest.tables.len() {
                tables.push(manifest.tables[lv].clone());
            }
        } else {
            // Add new layer
            for lv in self.low_lv.lv + 1..manifest.tables.len() {
                tables.push(manifest.tables[lv].clone());
            }
            tables.push(high_lv_tables);
        }

        let new_manifest = ManifestInner {
            version: manifest.version + 1,
            tables,
        };

        // Persist new version manifest
        self.manifest.persister.lock().persist(&new_manifest)?;

        // Write new version in memory
        let mut manifest = RwLockUpgradableReadGuard::upgrade(manifest);
        *manifest = new_manifest;

        Ok(())
    }

    // Clear "clear" in "origin" and add "new" into "origin"
    fn merge_tables(
        origin: &Vec<Arc<SSTableEntry>>,
        clear: &Vec<Arc<SSTableEntry>>,
        new: Vec<SSTableEntry>,
    ) -> Vec<Arc<SSTableEntry>> {
        let new_len = new.len();
        let mut new_tables = Vec::with_capacity(origin.len() + clear.len() - new_len);

        // Clear old data
        for entry in origin.iter() {
            let mut hold_on = true;
            for old_entry in clear.iter() {
                if entry.uid == old_entry.uid {
                    hold_on = false;
                    break;
                }
            }
            if hold_on {
                new_tables.push(Arc::clone(entry));
            }
        }

        // Add new data
        for entry in new {
            new_tables.push(Arc::new(entry));
        }

        // Sort entry
        new_tables.sort_by_key(|elem| elem.sort_key());

        debug_assert_eq!(new_tables.len() + clear.len() - new_len, origin.len());
        new_tables
    }
}

#[inline]
fn sstable_root_path(path: &PathBuf) -> PathBuf {
    path.join("tables")
}

#[inline]
fn manifest_root_path(path: &PathBuf) -> PathBuf {
    path.join("manifest")
}

// Open a new manifest file with given sequence number
fn open_new_manifest(dir: &Path, seq: u64) -> Result<File> {
    let path = dir.join(format!("{}.{}", seq, MANIFEST_FILE_EXTENSION));
    let new_file = OpenOptions::new()
        .truncate(true)
        .create(true)
        .write(true)
        .open(&path)?;

    Ok(new_file)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_manifest() {
        {
            let manifest = Manifest::open("./data").unwrap();
            let mut new_entry = manifest.alloc_new_sstable_entry().unwrap();
            let mut entry_writer = new_entry.open_writer().unwrap();
            entry_writer.write_all(b"123123123123").unwrap();
            new_entry.set_range((Bytes::from("123"), Bytes::from("456")));

            manifest.add_new_sstable(new_entry).unwrap();
        }

        {
            let manifest = Manifest::open("./data").unwrap();
            assert_eq!(manifest.inner.read().tables[0].len(), 1);
        }
    }
}
