use super::util::Value;
use super::{Error, Result};
use bytes::Bytes;
use std::collections::BTreeMap;

// Implement kv table
// TODO: replace by skip list in future
#[derive(Debug)]
pub struct KVTable {
    seq_id: u64,
    entries: BTreeMap<Bytes, Value>,
    next_log_id: u64,
    log_id_range: (u64, u64),
}

impl KVTable {
    /// Initialize a kvtable from given iter and next_log_id
    /// If next_log_id is zero, it will use first log_id of iteration
    #[inline]
    pub fn from_iter<I: Iterator<Item = Result<(u64, Bytes, Value)>>>(
        seq_id: u64,
        next_log_id: u64,
        iter: I,
    ) -> Result<Self> {
        let mut table = Self::new(seq_id, next_log_id);

        for data in iter {
            let (log_id, key, value) = data?;
            // Check log_id is increasing legally
            if table.next_log_id == 0 {
                table.next_log_id = log_id;
            }
            if table.next_log_id != log_id {
                Err(Error::IllegalLog)?;
            }
            table.set(key, value);
        }
        Ok(table)
    }

    // Create a new memtable
    #[inline]
    pub fn new(seq_id: u64, next_log_id: u64) -> KVTable {
        KVTable {
            seq_id,
            entries: BTreeMap::default(),
            next_log_id,
            log_id_range: (0, 0),
        }
    }

    /// Return if kvtable is empty
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    // Get iterator of the table
    #[inline]
    pub fn iter(&self) -> impl Iterator<Item = (&Bytes, &Value)> {
        self.entries.iter()
    }

    // Get length of inner list
    #[inline]
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    // Set key value pair into table and return inserted log_id
    #[inline]
    pub fn set(&mut self, key: Bytes, value: Value) -> u64 {
        if self.log_id_range.0 == 0 {
            self.log_id_range.0 = self.next_log_id;
        }
        self.log_id_range.1 = self.next_log_id;
        let log_id = self.next_log_id;
        self.next_log_id += 1;
        self.entries.insert(key, value);
        log_id
    }

    // Get value by given key
    #[inline]
    pub fn get(&self, key: &[u8]) -> Option<Value> {
        self.entries.get(key).cloned()
    }

    #[inline]
    pub fn seq_id(&self) -> u64 {
        self.seq_id
    }

    #[inline]
    pub fn log_id_range(&self) -> Option<(u64, u64)> {
        if self.log_id_range.0 != 0 {
            return Some(self.log_id_range);
        }
        None
    }

    /// Get next log id
    #[inline]
    pub fn next_log_id(&self) -> u64 {
        self.next_log_id
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::util::test_case::generate_random_bytes;

    #[test]
    fn test_kv_table_iter() {
        let test_count = 10000;

        let test_case_key = generate_random_bytes(test_count, 10000);
        let test_case_value = generate_random_bytes(test_count, 10 * 32 * 1024);

        let mut kv: Vec<_> = test_case_key
            .into_iter()
            .zip(test_case_value.into_iter().map(Value::Living))
            .collect();
        let mut table = KVTable::new(0, 0);

        for (key, value) in kv.iter() {
            table.set(key.clone(), value.clone());
        }

        assert_eq!(table.entries.len(), kv.len());

        kv.sort_by_key(|elem| elem.0.clone());

        for ((k1, v1), (k2, v2)) in table.iter().zip(kv.iter()) {
            assert_eq!(k1, k2);
            assert_eq!(v1, v2);
        }
    }
}
