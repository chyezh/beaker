use super::record::RecordWriter;
use super::sstable::{self, SSTableEntry};
use super::util::scan_sorted_file_at_path;
use super::{Error, Result};
use crate::leveldb::record::RecordReader;
use crate::util::{from_le_bytes_32, from_le_bytes_64};
use bytes::Bytes;
use parking_lot::{Mutex, RwLock, RwLockUpgradableReadGuard};
use std::fmt::Debug;
use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::Arc;
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};
use tracing::{info, warn};
use uuid::Uuid;

const SSTABLE_FILE_EXTENSION: &str = "tbl";
const MANIFEST_FILE_EXTENSION: &str = "manifest";
const MINIMUM_SSTABLE_ENTRY_SIZE: usize = 28;
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
    compact_tx: UnboundedSender<()>,
    clean_sstable_tx: UnboundedSender<()>,
}

impl Manifest {
    // Open a new manifest record
    pub fn open(
        root_path: impl Into<PathBuf>,
    ) -> Result<(Self, UnboundedReceiver<()>, UnboundedReceiver<()>)> {
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
                None => (ManifestInner::default(), 0),
            }
        } else {
            // Empty manifest root path, create new empty manifest
            (ManifestInner::default(), 0)
        };
        // Open new persister
        let mut persister = ManifestPersister::open(manifest_root_path(&root_path), next_log_seq)?;
        let (compact_tx, compact_rx) = unbounded_channel();
        let (clean_sstable_tx, clean_sstable_rx) = unbounded_channel();

        // Persist manifest to the newest log and clear old manifest log
        persister.persist(&inner)?;
        persister.clear_old_manifest();

        Ok((
            Manifest {
                inner: Arc::new(RwLock::new(inner)),
                root_path,
                persister: Arc::new(Mutex::new(persister)),
                compact_tx,
                clean_sstable_tx,
            },
            compact_rx,
            clean_sstable_rx,
        ))
    }

    // Alloc a new sstable entry, and return its writer
    pub fn alloc_new_sstable_entry(&self, lv: usize) -> SSTableEntry {
        SSTableEntry::new(lv, sstable_root_path(&self.root_path))
    }

    // Add a new sstable file into manifest, persist modification and update in-memory manifest
    pub fn add_new_sstable(&self, entry: SSTableEntry) -> Result<()> {
        debug_assert!(!entry.range.left.is_empty());
        debug_assert!(entry.range.left <= entry.range.right);
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
        if let Err(err) = self.compact_tx.send(()) {
            info!(
                error = err.to_string(),
                "receiver for compact trigger has been released"
            );
        }
        Ok(())
    }

    // Finish compact, try to persist compact result and update in-memory manifest
    pub fn finish_compact(&self, task: CompactTask) -> Result<()> {
        let manifest = self.inner.upgradable_read();
        debug_assert!(manifest.tables.len() > task.low_lv.lv); // Low level layer must exist
        debug_assert!(manifest.tables.len() >= task.high_lv.lv); // High level layer may be created at compaction

        // Clear outdate entry and add new entry in low or high level layer
        let low_lv_tables = merge_tables(
            &manifest.tables[task.low_lv.lv],
            &task.low_lv.tables,
            Vec::new(),
        );
        let high_lv_tables = if manifest.tables.len() > task.high_lv.lv {
            merge_tables(
                &manifest.tables[task.high_lv.lv],
                &task.high_lv.tables,
                task.new,
            )
        } else {
            let origin = Vec::new();
            merge_tables(&origin, &task.high_lv.tables, task.new)
        };

        // Construct new tables
        let mut tables = Vec::with_capacity(manifest.tables.len() + 1);
        for lv in 0..task.low_lv.lv {
            tables.push(manifest.tables[lv].clone());
        }
        tables.push(low_lv_tables);
        if manifest.tables.len() > task.high_lv.lv {
            // No new layer
            for lv in task.low_lv.lv + 1..task.high_lv.lv {
                tables.push(manifest.tables[lv].clone());
            }
            tables.push(high_lv_tables);
            for lv in task.high_lv.lv + 1..manifest.tables.len() {
                tables.push(manifest.tables[lv].clone());
            }
        } else {
            // Add new layer
            for lv in task.low_lv.lv + 1..manifest.tables.len() {
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
        if let Err(err) = self.clean_sstable_tx.send(()) {
            info!(
                error = err.to_string(),
                "receiver for clean sstable trigger has been released"
            );
        }

        // Trigger to find new compact task
        if let Err(err) = self.compact_tx.send(()) {
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

        for (lv, tables) in manifest.tables.iter().enumerate() {
            // Try to compact if level-0's tables count is gte 4
            // Try to compact if other level tables count is gte 10
            if (lv == 0 && tables.len() >= 4) || tables.len() >= 10 {
                for (idx, _) in tables.iter().enumerate() {
                    if let Ok(mut task) = CompactTask::new(&manifest, lv, idx) {
                        // The compaction of greatest two level should erase tombstone
                        if lv + 2 >= manifest.tables.len() {
                            task.erase_tombstone = true
                        }
                        tasks.push(task);
                        // One compact task for one level
                        break;
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
                if !sstable::is_active_uuid(&uid) {
                    // Remove inactive uuid file
                    tokio::fs::remove_file(filepath).await?;
                    info!("remove inactive sstable file, {}", uid);
                }
            }
        }
        Ok(())
    }
}

#[derive(Debug)]
pub struct CompactSSTables {
    tables: Vec<Arc<SSTableEntry>>,
    lv: usize,
}

impl Drop for CompactSSTables {
    fn drop(&mut self) {
        // Should release all compaction lock
        // CompactTask can be finished or failed
        for table in self.tables.iter() {
            table.release_lock();
        }
    }
}

#[derive(Debug)]
pub struct CompactTask {
    erase_tombstone: bool,
    low_lv: CompactSSTables,
    high_lv: CompactSSTables,
    new: Vec<SSTableEntry>,
}

impl CompactTask {
    // Create new compact task with given manifest and compact target sstable
    // Find all overlapping sstables and create task
    fn new(manifest: &ManifestInner, lv: usize, idx: usize) -> Result<Self> {
        debug_assert!(manifest.tables.len() > lv);
        debug_assert!(manifest.tables[lv].len() > idx);

        let table = &manifest.tables[lv][idx];
        let mut range = table.range.clone();

        // Try to get compaction lock
        table.try_compact_lock()?;
        let mut low_lv = CompactSSTables {
            tables: vec![table.clone()],
            lv,
        };
        let mut high_lv = CompactSSTables {
            tables: Vec::new(),
            lv: lv + 1,
        };

        // Find all overlap sstable
        if lv == 0 {
            // Search overlap sstable in level-0
            for (i, other) in manifest.tables[0].iter().enumerate() {
                if idx != i && range.is_overlap(&other.range) {
                    // Overlap found, try lock and merge range
                    other.try_compact_lock()?;
                    low_lv.tables.push(other.clone());
                    range.get_minimum_contain(&other.range);
                }
            }
        }

        // Search overlap sstable in high level
        if let Some(tables) = manifest.tables.get(lv + 1) {
            for other in tables.iter() {
                // Overlap found, try lock
                // High level range never overlap with each other, no range merge needed
                if range.is_overlap(&other.range) {
                    other.try_compact_lock()?;
                    high_lv.tables.push(other.clone());
                    range.get_minimum_contain(&other.range);
                }
            }
        }

        // Add nearby low level sstable to join compact if file is not enough
        if high_lv.tables.len() + low_lv.tables.len() <= 1 {
            // Push left item
            if idx > 0 {
                let other = &manifest.tables[lv][idx - 1];
                if other.try_compact_lock().is_ok() {
                    low_lv.tables.insert(0, other.clone());
                    range.get_minimum_contain(&other.range);
                }
            }

            // Push right item
            if idx < low_lv.tables.len() - 1 {
                let other = &manifest.tables[lv][idx + 1];
                if other.try_compact_lock().is_ok() {
                    low_lv.tables.push(other.clone());
                    range.get_minimum_contain(&other.range);
                }
            }

            // Search overlap sstable in high level again
            if let Some(tables) = manifest.tables.get(lv + 1) {
                for other in tables.iter() {
                    // Overlap found, try lock
                    // High level range never overlap with each other, no range merge needed
                    if range.is_overlap(&other.range) {
                        other.try_compact_lock()?;
                        high_lv.tables.push(other.clone());
                        range.get_minimum_contain(&other.range);
                    }
                }
            }
        }

        // Compact task must contain more than one sstable
        if low_lv.tables.len() + high_lv.tables.len() <= 1 {
            return Err(Error::Other);
        }

        Ok(CompactTask {
            erase_tombstone: false,
            low_lv,
            high_lv,
            new: Vec::new(),
        })
    }

    // Get new iterator iterating all tables waiting to compact
    // older kv should be iterated first,
    // so high level should be iterated first,
    #[inline]
    pub fn tables(&self) -> impl Iterator<Item = &Arc<SSTableEntry>> {
        self.high_lv.tables.iter().chain(self.low_lv.tables.iter())
    }

    // Get compact target level
    #[inline]
    pub fn compact_size(&self) -> usize {
        debug_assert!(self.high_lv.lv > 0);
        // 10^L MB
        (10_usize).pow(self.high_lv.lv as u32) * 1024 * 1024
    }

    #[inline]
    pub fn target_lv(&self) -> usize {
        self.high_lv.lv
    }

    // Add new compact result
    #[inline]
    pub fn add_compact_result(&mut self, entry: SSTableEntry) {
        self.new.push(entry);
    }

    #[inline]
    pub fn need_erase_tombstone(&self) -> bool {
        self.erase_tombstone
    }
}

// Clear "clear" in "origin" and add "new" into "origin"
fn merge_tables(
    origin: &Vec<Arc<SSTableEntry>>,
    clear: &Vec<Arc<SSTableEntry>>,
    new: Vec<SSTableEntry>,
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

    // Sort entry
    new_tables.sort_by_key(|elem| elem.sort_key());

    debug_assert_eq!(new_tables.len() + clear.len() - new_len, origin.len());
    new_tables
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
