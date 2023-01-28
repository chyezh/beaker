use super::manifest::{CompactTask, Manifest, SSTableEntry};
use super::sstable::{SSTable, SSTableBuilder, SSTableIntoIterator};
use super::util::Value;
use super::Result;
use bytes::Bytes;
use std::fs::File;
use std::io::{Read, Seek};

pub struct Compactor {
    task: CompactTask,
    manifest: Manifest,
}

impl Compactor {
    pub fn new(task: CompactTask, manifest: Manifest) -> Self {
        Compactor { task, manifest }
    }

    // Do the compact operation
    pub fn compact(mut self) -> Result<()> {
        let compact_size = self.task.compact_size();
        let erase_tombstone = self.task.need_erase_tombstone();
        let mut new_entry: Option<(SSTableEntry, SSTableBuilder<File>)> = None;

        for element in self.open_all_sstable()? {
            if new_entry.is_none() {
                // Start a new builder
                let entry = self.manifest.alloc_new_sstable_entry();
                let builder = SSTableBuilder::new(entry.open_writer()?);
                new_entry = Some((entry, builder));
            }

            let (key, value) = element?;

            // Skip tombstone if erase tombstone option is set
            if erase_tombstone && value.is_tombstone() {
                continue;
            }

            let size = new_entry.as_mut().unwrap().1.add(key, value)?;
            if size >= compact_size {
                // Finish one compaction
                new_entry.as_mut().unwrap().1.finish()?;
                self.task.add_compact_result(new_entry.take().unwrap().0);
            }
        }

        // Finish remaining compaction
        if let Some(mut entry) = new_entry {
            entry.1.finish()?;
            self.task.add_compact_result(entry.0);
        }

        // Finish all compaction
        self.manifest.finish_compact(self.task)
    }

    // Open all sstable for compact operation
    fn open_all_sstable(&self) -> Result<SSTablesIter<File>> {
        let mut sstables = Vec::with_capacity(self.task.tables().size_hint().0);
        for table in self.task.tables() {
            sstables.push(SSTable::open(table.open_reader()?)?.into_iter());
        }
        let table_count = sstables.len();

        Ok(SSTablesIter {
            iters: sstables,
            kvs: vec![None; table_count],
        })
    }
}

// SSTables iteration util struct, iterating sstables by merge-sort algorithm
struct SSTablesIter<R: Seek + Read> {
    iters: Vec<SSTableIntoIterator<R>>,
    kvs: Vec<Option<(Bytes, Value)>>,
}

impl<R: Seek + Read> SSTablesIter<R> {
    // Fill the none element in kvs array with respective iterator
    fn fill_kvs(&mut self) -> Result<()> {
        debug_assert_eq!(self.iters.len(), self.kvs.len());
        for (idx, last) in self.kvs.iter_mut().enumerate() {
            if last.is_none() {
                match self.iters[idx].next() {
                    Some(Ok(item)) => *last = Some(item),
                    Some(Err(e)) => return Err(e),
                    None => {}
                }
            }
        }

        Ok(())
    }

    // Select minimal key in kvs array, and take it
    fn take_min_kv(&mut self) -> Option<(Bytes, Value)> {
        debug_assert!(self.iters.len() > 1);

        let mut need_take_idx = Vec::with_capacity(self.iters.len());
        let mut min_idx = 0;
        for (idx, value) in self.kvs.iter().enumerate().skip(1) {
            match (&self.kvs[min_idx], value) {
                (None, _) => {
                    min_idx = idx;
                }
                (Some(v1), Some(v2)) if v1.0 == v2.0 => {
                    // Key is equal, new key should be select, old key should be ignore
                    need_take_idx.push(min_idx);
                    min_idx = idx;
                }
                (Some(v1), Some(v2)) if v1.0 > v2.0 => {
                    // New minimal key is found, clear previous need_take_idx, update new minimal key index
                    need_take_idx.clear();
                    min_idx = idx;
                }
                // Ignore cases:
                // 1. value.is_none
                // 2. key is not minimal
                (_, _) => {}
            }
        }

        // Clear all equal-key kvs recorded in need_take_idx
        for idx in need_take_idx {
            debug_assert_ne!(idx, min_idx);
            self.kvs[idx].take();
        }

        // Return kv with minimal key in the newest layer
        self.kvs[min_idx].take()
    }
}

impl<R: Seek + Read> Iterator for SSTablesIter<R> {
    type Item = Result<(Bytes, Value)>;

    // Iterating iters, give minimal and newest key
    fn next(&mut self) -> Option<Self::Item> {
        if let Err(e) = self.fill_kvs() {
            return Some(Err(e));
        }

        self.take_min_kv().map(Ok)
    }
}
