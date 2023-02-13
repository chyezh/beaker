use super::event::{Event, EventNotifier};
use super::record::RecordWriter;
use super::sstable::{self, SSTableEntry, SSTableManager, SSTableRange};
use super::util::scan_sorted_file_at_path;
use super::{Error, Result};
use crate::engine::record::RecordReader;
use crate::util::{from_le_bytes_32, from_le_bytes_64};
use bytes::Bytes;
use parking_lot::{Mutex, RwLock, RwLockUpgradableReadGuard};
use std::fmt::Debug;
use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::Arc;
use tracing::{info, warn};
use uuid::Uuid;

const MANIFEST_FILE_EXTENSION: &str = "manifest";
const MINIMUM_MANIFEST_INNER_SIZE: usize = 8;
const SPLIT_MANIFEST_SIZE_THRESHOLD: usize = 32 * 1024 * 1024; // 32MB

#[derive(Clone, Default, Debug)]
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
    fn decode_from_bytes(
        root_path: PathBuf,
        data: Bytes,
        manager: &SSTableManager<tokio::fs::File>,
    ) -> Result<Self> {
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

            let entry = manager.parse_entry(root_path.clone(), remain.slice(4..4 + sstable_len))?;

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
        let mut need_clear = false;
        let total_write = self.log.written();
        let next_log_seq = self.next_log_seq;
        if total_write > SPLIT_MANIFEST_SIZE_THRESHOLD {
            self.log = RecordWriter::new(open_new_manifest(&self.root_path, next_log_seq)?);
            self.next_log_seq += 1;
            need_clear = true;
        }

        // Write manifest information
        let mut buffer = Vec::with_capacity(manifest.encode_bytes_len());
        manifest.encode_to(&mut buffer)?;
        self.log.append(buffer.into())?;

        // TODO: make async
        if need_clear {
            self.clear_old_manifest();
        }

        Ok(())
    }

    // Remove all old manifest
    fn clear_old_manifest(&self) {
        // Scan root_path, clear log expired
        if let Ok(files) =
            scan_sorted_file_at_path(self.root_path.as_path(), MANIFEST_FILE_EXTENSION)
        {
            for (path, i) in files {
                if i < self.next_log_seq - 1 {
                    if let Err(err) = fs::remove_file(path) {
                        warn!(
                            error = err.to_string(),
                            log_seq = i,
                            "remove expired manifest file failed",
                        );
                    }
                }
            }
        }
    }
}

#[derive(Clone)]
pub struct Manifest {
    inner: Arc<RwLock<ManifestInner>>,
    persister: Arc<Mutex<ManifestPersister>>,
    root_path: PathBuf,
    event_sender: EventNotifier,
    manager: SSTableManager<tokio::fs::File>,
}

impl Manifest {
    // Open a new manifest record
    pub fn open(
        root_path: impl Into<PathBuf>,
        event_sender: EventNotifier,
        manager: SSTableManager<tokio::fs::File>,
    ) -> Result<Self> {
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
                    let inner = ManifestInner::decode_from_bytes(
                        sstable_root_path(&root_path),
                        data,
                        &manager,
                    )?;
                    (inner, manifest_files.last().unwrap().1 + 1)
                }
                None => (ManifestInner::default(), 0),
            }
        } else {
            // Empty manifest root path, create new empty manifest
            (ManifestInner::default(), 0)
        };

        // Open new persister
        let mut persister = ManifestPersister::open(manifest_root_path(&root_path), next_log_seq)?;
        // Persist manifest to the newest log and clear old manifest log
        persister.persist(&inner)?;
        persister.clear_old_manifest();

        Ok(Manifest {
            inner: Arc::new(RwLock::new(inner)),
            root_path,
            persister: Arc::new(Mutex::new(persister)),
            event_sender,
            manager,
        })
    }

    // Alloc a new sstable entry, and return its writer
    pub fn alloc_new_sstable_entry(&self, lv: usize) -> SSTableEntry {
        self.manager
            .alloc_entry(lv, sstable_root_path(&self.root_path))
    }

    // Add a new sstable file into manifest, persist modification and update in-memory manifest
    pub fn add_new_sstable(&self, entry: SSTableEntry) -> Result<()> {
        debug_assert!(!entry.range.is_empty());
        debug_assert_eq!(entry.lv, 0);

        let manifest = self.inner.upgradable_read();
        let mut new_manifest = manifest.clone();
        if new_manifest.tables.is_empty() {
            // Create level 0 if manifest is empty
            new_manifest.tables = vec![Vec::new()];
        }

        new_manifest.version += 1;
        new_manifest.tables[0].push(Arc::new(entry));

        // Persist manifest
        self.persister.lock().persist(&new_manifest)?;

        // Modify in-memory manifest
        let mut manifest = RwLockUpgradableReadGuard::upgrade(manifest);
        *manifest = new_manifest;

        // Trigger to find new compact task
        if let Err(err) = self.event_sender.notify(Event::Compact) {
            info!(
                error = err.to_string(),
                "receiver for compact trigger has been released"
            );
        }
        Ok(())
    }

    // Finish compact, try to persist compact result and update in-memory manifest
    pub fn finish_compact(&self, mut task: CompactTask) -> Result<()> {
        let manifest = self.inner.upgradable_read();
        let original_lv = task.target_lv() - 1;
        let target_lv = task.target_lv();
        let new_sstable_entries = task.take_new();
        debug_assert!(manifest.tables.len() > original_lv); // Low level layer must exist
        debug_assert!(manifest.tables.len() >= target_lv); // High level layer may be created at compaction

        // Clear outdate entry and add new entry in low or high level layer
        let low_lv_tables = merge_tables(
            &manifest.tables[original_lv],
            &task.low_lv,
            Vec::new(),
            original_lv,
        );
        let high_lv_tables = if manifest.tables.len() > target_lv {
            merge_tables(
                &manifest.tables[target_lv],
                &task.high_lv,
                new_sstable_entries,
                target_lv,
            )
        } else {
            let origin = Vec::new();
            merge_tables(&origin, &task.high_lv, new_sstable_entries, target_lv)
        };

        // Construct new tables
        let mut tables = Vec::with_capacity(manifest.tables.len() + 1);
        for lv in 0..original_lv {
            tables.push(manifest.tables[lv].clone());
        }
        tables.push(low_lv_tables);
        if manifest.tables.len() > target_lv {
            // No new layer
            for lv in original_lv + 1..target_lv {
                tables.push(manifest.tables[lv].clone());
            }
            tables.push(high_lv_tables);
            for lv in target_lv + 1..manifest.tables.len() {
                tables.push(manifest.tables[lv].clone());
            }
        } else {
            // Add new layer
            for lv in original_lv + 1..manifest.tables.len() {
                tables.push(manifest.tables[lv].clone());
            }
            tables.push(high_lv_tables);
        }

        let new_manifest = ManifestInner {
            version: manifest.version + 1,
            tables,
        };

        // Persist new version manifest
        self.persister.lock().persist(&new_manifest)?;

        // Write new version in memory
        let mut manifest = RwLockUpgradableReadGuard::upgrade(manifest);
        *manifest = new_manifest;

        // Clear old files
        if let Err(err) = self.event_sender.notify(Event::InactiveSSTableClean) {
            info!(
                error = err.to_string(),
                "receiver for clean sstable trigger has been released"
            );
        }

        // Trigger to find new compact task
        if let Err(err) = self.event_sender.notify(Event::Compact) {
            info!(
                error = err.to_string(),
                "receiver for compact trigger has been released"
            );
        }
        Ok(())
    }

    // Create iterator iterating all sstable which may contain key
    pub fn search(&self, key: &[u8]) -> Vec<Arc<SSTableEntry>> {
        // Read a manifest snapshot
        let manifest = self.inner.read();

        // Max sstable count may be count of non-zero-level + count of sstable in level 0 (level 0 may overlap)
        let mut may_len = manifest.tables.len();
        if let Some(lv0) = manifest.tables.first() {
            may_len += lv0.len();
        }
        let mut sstables = Vec::with_capacity(may_len);

        // Search sstable from young level to old level
        for (lv, tables) in manifest.tables.iter().enumerate() {
            if lv == 0 {
                // Level 0 may overlap, need to iterate all sstable in reverse order
                for table in tables.iter().rev() {
                    if table.range.in_range(key) {
                        sstables.push(table.clone());
                    }
                }
            } else {
                // Other level never overlap, binary search
                if let Ok(idx) = tables.binary_search_by(|probe| probe.range.order(key)) {
                    sstables.push(tables[idx].clone());
                }
            }
        }

        sstables
    }

    // Find new compaction task
    pub fn find_new_compact_tasks(&self) -> Vec<CompactTask> {
        let manifest = self.inner.read();
        let mut tasks = Vec::new();
        let empty_level = Vec::new();

        for (lv, tables) in manifest.tables.iter().enumerate() {
            // Try to compact if level-0's tables count is gte 4
            // Try to compact if other level tables count is gte 10

            if (lv == 0 && tables.len() >= 4) || tables.len() >= 10 {
                let low_lv = &manifest.tables[lv];
                let high_lv = manifest.tables.get(lv + 1).unwrap_or(&empty_level);
                // Find one task for one level
                // Try to find single seed
                let mut found = false;
                for idx in 0..tables.len() {
                    let seed_idx = [idx];
                    if let Ok(task) = CompactTaskBuilder::new(low_lv, high_lv, lv)
                        .erase_tombstone(lv + 2 >= manifest.tables.len())
                        .build(&seed_idx)
                    {
                        tasks.push(task);
                        found = true;
                        break;
                    }
                }

                // Try to find with multi seed
                if !found {
                    for idx in 0..tables.len() - 2 {
                        let seed_idx = [idx, idx + 1, idx + 2];
                        if let Ok(task) = CompactTaskBuilder::new(low_lv, high_lv, lv)
                            .erase_tombstone(lv + 2 >= manifest.tables.len())
                            .build(&seed_idx)
                        {
                            tasks.push(task);
                            break;
                        }
                    }
                }
            }
        }
        tasks
    }

    // Clean the inactive sstable on disk
    pub async fn clean_inactive_sstable(&self) -> Result<()> {
        let path = sstable_root_path(&self.root_path);
        let mut reader = tokio::fs::read_dir(&path).await?;
        while let Ok(Some(entry)) = reader.next_entry().await {
            // Parse uid from filename
            let filepath = entry.path();
            if !filepath.is_file()
                || filepath.extension() != Some(sstable::SSTABLE_FILE_EXTENSION.as_ref())
            {
                continue;
            }

            if let Some(Ok(uid)) = filepath
                .file_stem()
                .and_then(std::ffi::OsStr::to_str)
                .map(Uuid::from_str)
            {
                if !self.manager.is_active_uuid(&uid) {
                    // Remove inactive uuid file
                    tokio::fs::remove_file(filepath).await?;
                    info!("remove inactive sstable file, {}", uid);
                }
            }
        }
        Ok(())
    }
}

struct CompactTaskBuilder<'a> {
    low_lv: &'a [Arc<SSTableEntry>],
    high_lv: &'a [Arc<SSTableEntry>],
    lv: usize,
    erase_tombstone: bool,
    low_lv_select: Vec<Option<&'a Arc<SSTableEntry>>>,
    high_lv_select: Vec<Option<&'a Arc<SSTableEntry>>>,
    range: SSTableRange,
}

impl<'a> CompactTaskBuilder<'a> {
    #[inline]
    fn new(
        low_lv: &'a Vec<Arc<SSTableEntry>>,
        high_lv: &'a Vec<Arc<SSTableEntry>>,
        lv: usize,
    ) -> Self {
        CompactTaskBuilder {
            low_lv,
            high_lv,
            lv,
            erase_tombstone: false,
            low_lv_select: vec![None; low_lv.len()],
            high_lv_select: vec![None; high_lv.len()],
            range: SSTableRange::default(),
        }
    }

    #[inline]
    fn erase_tombstone(mut self, b: bool) -> Self {
        self.erase_tombstone = b;
        self
    }

    // Add a new sstable into low level
    fn add_new_low_lv(&mut self, idx: usize) -> Result<()> {
        debug_assert!(idx < self.low_lv.len());
        debug_assert_eq!(self.low_lv_select.len(), self.low_lv.len());

        self.low_lv[idx].try_compact_lock()?;
        self.low_lv_select[idx] = Some(&self.low_lv[idx]);
        self.range.set_to_minimum_contain(&self.low_lv[idx].range);
        Ok(())
    }

    // Add a new index into high level
    fn add_new_high_lv(&mut self, idx: usize) -> Result<()> {
        debug_assert!(idx < self.high_lv.len());
        debug_assert_eq!(self.high_lv_select.len(), self.high_lv.len());
        debug_assert!(!self.range.is_empty());

        self.high_lv[idx].try_compact_lock()?;
        self.high_lv_select[idx] = Some(&self.high_lv[idx]);
        self.range.set_to_minimum_contain(&self.high_lv[idx].range);
        Ok(())
    }

    #[inline]
    fn initial_with_seeds(&mut self, idx_seeds: &'a [usize]) -> Result<()> {
        debug_assert!(!idx_seeds.is_empty());
        for &idx in idx_seeds {
            self.add_new_low_lv(idx)?;
        }
        // Range should never be empty
        debug_assert!(!self.range.is_empty());
        Ok(())
    }

    // Build with seeds
    fn build(mut self, idx_seeds: &'a [usize]) -> Result<CompactTask> {
        // Initial with given idx seeds and get range of seeds
        self.initial_with_seeds(idx_seeds)?;

        // Zero level may overlap
        if self.lv == 0 {
            debug_assert_eq!(self.low_lv_select.len(), self.low_lv.len());
            loop {
                let mut new_select = false;
                for (i, other) in self.low_lv.iter().enumerate() {
                    if self.low_lv_select[i].is_none() && self.range.is_overlap(&other.range) {
                        // Overlap found, recheck overlap again
                        self.add_new_low_lv(i)?;
                        new_select = true;
                    }
                }
                if !new_select {
                    break;
                }
            }
        }

        // Search overlap high level
        for (i, other) in self.high_lv.iter().enumerate() {
            if self.high_lv_select[i].is_none() && self.range.is_overlap(&other.range) {
                // Overlap found
                // It's no necessary to recheck overlap again, because high level would never overlap
                self.add_new_high_lv(i)?;
            }
        }

        let task = CompactTask {
            erase_tombstone: self.erase_tombstone,
            low_lv: self
                .low_lv_select
                .iter_mut()
                .filter_map(|entry| entry.take().cloned())
                .collect(),
            high_lv: self
                .high_lv_select
                .iter_mut()
                .filter_map(|entry| entry.take().cloned())
                .collect(),
            target_lv: self.lv + 1,
            new: Some(Vec::new()),
        };

        if task.low_lv.len() + task.high_lv.len() <= 1 {
            return Err(Error::Other);
        }
        Ok(task)
    }
}

impl<'a> Drop for CompactTaskBuilder<'a> {
    fn drop(&mut self) {
        // Should release all compaction lock
        for table in self.low_lv_select.iter().filter_map(|&entry| entry) {
            table.release_lock();
        }
        for table in self.high_lv_select.iter().filter_map(|&entry| entry) {
            table.release_lock();
        }
    }
}

#[derive(Debug)]
pub struct CompactTask {
    low_lv: Vec<Arc<SSTableEntry>>,
    high_lv: Vec<Arc<SSTableEntry>>,
    new: Option<Vec<SSTableEntry>>,
    target_lv: usize,
    erase_tombstone: bool,
}

impl CompactTask {
    // Get new iterator iterating all tables waiting to compact
    // older kv should be iterated first,
    // so high level should be iterated first,
    #[inline]
    pub fn tables(&self) -> impl Iterator<Item = &Arc<SSTableEntry>> {
        self.high_lv.iter().chain(self.low_lv.iter())
    }

    // Get compact target level
    #[inline]
    pub fn compact_size(&self) -> usize {
        debug_assert!(self.target_lv > 0);
        // 10^L MB
        (10_usize).pow(self.target_lv as u32) * 1024 * 1024
    }

    #[inline]
    pub fn target_lv(&self) -> usize {
        self.target_lv
    }

    // Add new compact result
    #[inline]
    pub fn add_compact_result(&mut self, entry: SSTableEntry) {
        if !entry.range.is_empty() {
            self.new.as_mut().unwrap().push(entry);
        }
    }

    #[inline]
    pub fn need_erase_tombstone(&self) -> bool {
        self.erase_tombstone
    }

    #[inline]
    pub fn take_new(&mut self) -> Vec<SSTableEntry> {
        self.new.take().unwrap()
    }
}

impl Drop for CompactTask {
    fn drop(&mut self) {
        // Should release all compaction lock
        for table in self.low_lv.iter() {
            table.release_lock();
        }
        for table in self.high_lv.iter() {
            table.release_lock();
        }
    }
}

// Clear "clear" in "origin" and add "new" into "origin"
fn merge_tables(
    origin: &Vec<Arc<SSTableEntry>>,
    clear: &Vec<Arc<SSTableEntry>>,
    new: Vec<SSTableEntry>,
    lv: usize,
) -> Vec<Arc<SSTableEntry>> {
    let new_len = new.len();
    let mut new_tables = Vec::with_capacity(origin.len() - clear.len() + new_len);

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

    // Non-zero level must keep sstable sorted by key range
    if lv > 0 {
        new_tables.sort_by_key(|elem| elem.sort_key_range());
    }

    debug_assert_eq!(new_tables.len() + clear.len() - new_len, origin.len());
    new_tables
}

#[inline]
fn sstable_root_path(path: &Path) -> PathBuf {
    path.join("tables")
}

#[inline]
fn manifest_root_path(path: &Path) -> PathBuf {
    path.join("manifest")
}

// Open a new manifest file with given sequence number
fn open_new_manifest(dir: &Path, seq: u64) -> Result<File> {
    let path = dir.join(format!("{}.{}", seq, MANIFEST_FILE_EXTENSION));
    let new_file = OpenOptions::new()
        .truncate(true)
        .create(true)
        .write(true)
        .open(path)?;

    Ok(new_file)
}
