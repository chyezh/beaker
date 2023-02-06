use std::sync::Arc;

use super::manifest::{CompactTask, Manifest};
use super::sstable::{self, SSTableBuilder, SSTableStream};
use super::util::Value;
use super::Result;
use bytes::Bytes;
use tokio::fs::File;
use tokio::io::{AsyncRead, AsyncSeek};

pub struct Compactor {
    task: CompactTask,
    manifest: Manifest,
}

impl Compactor {
    pub fn new(task: CompactTask, manifest: Manifest) -> Self {
        Compactor { task, manifest }
    }

    // Do the compact operation
    pub async fn compact(mut self) -> Result<()> {
        let compact_size = self.task.compact_size();
        let erase_tombstone = self.task.need_erase_tombstone();
        let target_lv = self.task.target_lv();
        let mut new_builder: Option<SSTableBuilder<File>> = None;
        let mut sstable_iter = self.open_all_sstable().await?;

        while let Some(element) = sstable_iter.next().await {
            if new_builder.is_none() {
                // Start a new builder
                let entry = self.manifest.alloc_new_sstable_entry(target_lv);
                let builder = entry.open_builder().await?;
                new_builder = Some(builder);
            }

            let (key, value) = element?;

            // Skip tombstone if erase tombstone option is set
            if erase_tombstone && value.is_tombstone() {
                continue;
            }

            let size = new_builder.as_mut().unwrap().add(key, value).await?;
            if size >= compact_size {
                // Finish one compaction
                let new_entry = new_builder.take().unwrap().finish().await?;
                self.task.add_compact_result(new_entry);
            }
        }

        // Finish remaining compaction
        if let Some(builder) = new_builder {
            let new_entry = builder.finish().await?;
            self.task.add_compact_result(new_entry);
        }

        // Finish all compaction
        self.manifest.finish_compact(self.task)
    }

    // Open all sstable for compact operation
    async fn open_all_sstable(&self) -> Result<SSTablesIter<tokio::fs::File>> {
        let mut sstables = Vec::with_capacity(self.task.tables().size_hint().0);
        for entry in self.task.tables() {
            // Open a new stream for this entry
            let s = sstable::open(Arc::clone(entry))
                .await?
                .open_stream()
                .await?;
            sstables.push(s);
        }
        let table_count = sstables.len();

        Ok(SSTablesIter {
            iters: sstables,
            kvs: vec![None; table_count],
        })
    }
}

// SSTables iteration util struct, iterating sstables by merge-sort algorithm
struct SSTablesIter<R: AsyncRead + AsyncSeek + Unpin> {
    iters: Vec<SSTableStream<R>>,
    kvs: Vec<Option<(Bytes, Value)>>,
}

impl<R: AsyncRead + AsyncSeek + Unpin> SSTablesIter<R> {
    // Fill the none element in kvs array with respective iterator
    async fn fill_kvs(&mut self) -> Result<()> {
        debug_assert_eq!(self.iters.len(), self.kvs.len());
        for (idx, last) in self.kvs.iter_mut().enumerate() {
            if last.is_none() {
                match self.iters[idx].next().await {
                    Some(Ok(item)) => *last = Some(item),
                    Some(Err(e)) => return Err(e),
                    None => {}
                }
            }
        }

        Ok(())
    }

    // Select minimal key in kvs array, and take it
    async fn take_min_kv(&mut self) -> Option<(Bytes, Value)> {
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

    pub async fn next(&mut self) -> Option<Result<(Bytes, Value)>> {
        if let Err(e) = self.fill_kvs().await {
            return Some(Err(e));
        }
        self.take_min_kv().await.map(Ok)
    }
}
