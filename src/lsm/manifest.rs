use bytes::Bytes;
use uuid::Uuid;

use crate::lsm::manifest;

use super::Result;
use parking_lot::{Mutex, MutexGuard, RwLock, RwLockUpgradableReadGuard};
use std::fs::{File, OpenOptions};
use std::path::PathBuf;
use std::sync::Arc;

struct SSTableEntry {
    lv: u32,
    uid: Uuid,
    path: PathBuf,
    compact_lock: Mutex<()>,
    range: (Bytes, Bytes),
}

impl SSTableEntry {
    pub fn open_reader(&self) -> Result<File> {
        let new_file = File::open(self.path.as_path())?;
        Ok(new_file)
    }

    pub fn set_range(&mut self, range: (Bytes, Bytes)) {
        debug_assert!(range.0 <= range.1);
        self.range = range;
    }

    // Try to lock compact process
    pub fn try_compact_lock(&self) -> Option<MutexGuard<()>> {
        self.compact_lock.try_lock()
    }

    fn sort_key(&self) -> Bytes {
        self.range.0.clone()
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

        // Write new version
        let mut manifest = RwLockUpgradableReadGuard::upgrade(manifest);
        manifest.version += 1;
        manifest.tables = tables;

        // TODO: Persistent
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

#[derive(Clone)]
struct ManifestInner {
    version: u64,
    tables: Vec<Vec<Arc<SSTableEntry>>>,
}

#[derive(Clone)]
pub struct Manifest {
    inner: Arc<RwLock<ManifestInner>>,
    root_path: PathBuf,
}

impl Manifest {
    // Alloc a new sstable entry, and return its writer
    pub fn alloc_new_sstable_entry(&self) -> Result<(SSTableEntry, File)> {
        let uid = Uuid::new_v4();
        let file_name = format!("{}.tbl", uid);
        let path = self.root_path.join(file_name);

        let new_file = OpenOptions::new()
            .truncate(true)
            .create(true)
            .write(true)
            .open(path.as_path())?;

        Ok((
            SSTableEntry {
                uid,
                path,
                lv: 0,
                range: (Bytes::new(), Bytes::new()),
                compact_lock: Mutex::new(()),
            },
            new_file,
        ))
    }

    // Add a new sstable file into manifest
    pub fn add_new_sstable(&self, entry: SSTableEntry) {
        debug_assert!(!entry.range.0.is_empty());
        debug_assert!(entry.range.0 <= entry.range.1);
        debug_assert!(!self.inner.read().tables.is_empty());
        debug_assert_eq!(entry.lv, 0);

        // always add new entry into lv 0
        self.inner.write().tables[0].push(Arc::new(entry));
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
