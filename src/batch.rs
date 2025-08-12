use crate::codec::{BincodeCodec, KeyCodec, ValueCodec};
use crate::error::{Error, Result};
use rocksdb::{WriteBatch, WriteOptions, DB};
use serde::{de::DeserializeOwned, Serialize};
use std::marker::PhantomData;

/// A batch of write operations that can be committed atomically
pub struct RocksMapBatch<'a, K, V, KC = BincodeCodec<K>, VC = BincodeCodec<V>>
where
    K: Serialize + DeserializeOwned,
    V: Serialize + DeserializeOwned,
{
    db: &'a DB,
    batch: WriteBatch,
    cf_name: Option<String>,
    _key_marker: PhantomData<K>,
    _value_marker: PhantomData<V>,
    _key_codec: PhantomData<KC>,
    _value_codec: PhantomData<VC>,
}

impl<'a, K, V> RocksMapBatch<'a, K, V, BincodeCodec<K>, BincodeCodec<V>>
where
    K: Serialize + DeserializeOwned + Clone,
    V: Serialize + DeserializeOwned + Clone,
{
    /// Create a new batch operation instance for a RocksDB instance
    pub fn new(db: &'a DB, cf_name: Option<String>) -> Self {
        Self {
            db,
            batch: WriteBatch::default(),
            cf_name,
            _key_marker: PhantomData,
            _value_marker: PhantomData,
            _key_codec: PhantomData,
            _value_codec: PhantomData,
        }
    }

    /// Add a put operation to the batch
    pub fn put(&mut self, key: &K, value: &V) -> Result<&mut Self> {
        let key_bytes = <BincodeCodec<K> as KeyCodec<K>>::encode(&key)?;
        let value_bytes = <BincodeCodec<V> as ValueCodec<V>>::encode(value)?;

        match &self.cf_name {
            Some(cf_name) => {
                let cf = self
                    .db
                    .cf_handle(cf_name)
                    .ok_or_else(|| Error::ColumnFamilyNotFound(cf_name.clone()))?;
                self.batch.put_cf(cf, key_bytes, value_bytes);
            }
            None => self.batch.put(key_bytes, value_bytes),
        }

        Ok(self)
    }

    /// Add a delete operation to the batch
    pub fn delete(&mut self, key: &K) -> Result<&mut Self> {
        let key_bytes = <BincodeCodec<K> as KeyCodec<K>>::encode(key)?;

        match &self.cf_name {
            Some(cf_name) => {
                let cf = self
                    .db
                    .cf_handle(cf_name)
                    .ok_or_else(|| Error::ColumnFamilyNotFound(cf_name.clone()))?;
                self.batch.delete_cf(cf, key_bytes);
            }
            None => self.batch.delete(key_bytes),
        }

        Ok(self)
    }

    /// Commit all operations in the batch atomically
    pub fn commit(self) -> Result<()> {
        let write_opts = WriteOptions::default();
        self.db
            .write_opt(self.batch, &write_opts)
            .map_err(Error::from)
    }

    /// Clears all operations in the batch without committing them
    pub fn clear(&mut self) {
        self.batch = WriteBatch::default();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::RocksMap;
    use serde::{Deserialize, Serialize};
    use tempfile::TempDir;

    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
    struct TestItem {
        id: u32,
        value: String,
    }

    #[test]
    fn test_batch_operations() {
        let temp_dir = TempDir::new().unwrap();
        let db = RocksMap::<u32, TestItem>::open(temp_dir.path()).unwrap();

        let items = vec![
            TestItem {
                id: 1,
                value: "one".to_string(),
            },
            TestItem {
                id: 2,
                value: "two".to_string(),
            },
            TestItem {
                id: 3,
                value: "three".to_string(),
            },
        ];

        {
            let mut batch = db.batch();
            for item in &items {
                batch.put(&item.id, item).unwrap();
            }
            batch.commit().unwrap();
        }

        for item in &items {
            let retrieved = db.get(&item.id).unwrap().unwrap();
            assert_eq!(&retrieved, item);
        }

        {
            let mut batch = db.batch();
            batch.delete(&1).unwrap();
            batch.delete(&2).unwrap();

            let new_item = TestItem {
                id: 4,
                value: "four".to_string(),
            };
            batch.put(&4, &new_item).unwrap();

            batch.commit().unwrap();
        }

        assert!(db.get(&1).unwrap().is_none());
        assert!(db.get(&2).unwrap().is_none());
        assert!(db.get(&3).unwrap().is_some());
        assert!(db.get(&4).unwrap().is_some());
    }
}
