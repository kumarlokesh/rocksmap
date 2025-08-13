use crate::{
    codec::{BincodeCodec, KeyCodec, ValueCodec},
    error::{Error, Result},
};
use rocksdb::{ColumnFamilyDescriptor, Direction, IteratorMode, Options, DB};
use serde::{de::DeserializeOwned, Serialize};
use std::{marker::PhantomData, path::Path};

/// The main key-value store abstraction over RocksDB
pub struct RocksMap<K, V>
where
    K: Serialize + DeserializeOwned + Clone,
    V: Serialize + DeserializeOwned + Clone,
{
    db: DB,
    cf_name: Option<String>,
    _marker: PhantomData<(K, V)>,
}

impl<K, V> RocksMap<K, V>
where
    K: Serialize + DeserializeOwned + Clone,
    V: Serialize + DeserializeOwned + Clone,
{
    /// Opens a new RocksMap at the given path, creating it if it doesn't exist
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        Self::open_with_options(path, Options::default())
    }

    /// Opens a RocksMap with custom options
    pub fn open_with_options<P: AsRef<Path>>(path: P, mut options: Options) -> Result<Self> {
        let path = path.as_ref().to_path_buf();

        if !path.exists() {
            std::fs::create_dir_all(&path).map_err(|_| Error::InvalidPath(path.clone()))?;
        }

        options.create_if_missing(true);
        options.create_missing_column_families(true);

        let db = DB::open(&options, &path).map_err(Error::from)?;

        Ok(Self {
            db,
            cf_name: None,
            _marker: PhantomData,
        })
    }

    /// Opens a RocksMap with the specified column families
    pub fn open_with_cfs<P: AsRef<Path>>(
        path: P,
        mut options: Options,
        column_families: &[&str],
    ) -> Result<Self> {
        let path = path.as_ref().to_path_buf();

        if !path.exists() {
            std::fs::create_dir_all(&path).map_err(|_| Error::InvalidPath(path.clone()))?;
        }

        options.create_if_missing(true);
        options.create_missing_column_families(true);

        let cf_descriptors: Vec<ColumnFamilyDescriptor> = column_families
            .iter()
            .map(|name| ColumnFamilyDescriptor::new(*name, options.clone()))
            .collect();

        let db = DB::open_cf_descriptors(&options, &path, cf_descriptors).map_err(Error::from)?;

        Ok(Self {
            db,
            cf_name: None,
            _marker: PhantomData,
        })
    }

    /// Gets a column family handle by name, creating it if it doesn't exist
    pub fn column_family(&mut self, name: &str) -> Result<RocksMapRef<K, V>> {
        if !self.db.cf_handle(name).is_some() {
            self.db
                .create_cf(name, &Options::default())
                .map_err(Error::from)?;
        }

        Ok(RocksMapRef {
            db: &self.db,
            cf_name: Some(name.to_string()),
            marker: PhantomData,
        })
    }

    /// Creates a reference to a RocksMap with the same database but different column family
    pub fn with_cf(&self, cf_name: &str) -> RocksMapRef<'_, K, V> {
        RocksMapRef {
            db: &self.db,
            cf_name: Some(cf_name.to_string()),
            marker: PhantomData,
        }
    }

    /// Returns a reference to the underlying database
    pub fn db(&self) -> &DB {
        &self.db
    }
}

/// A reference to a RocksMap that holds a reference to the database rather than owning it.
/// This allows us to create multiple views into the same database with different column families.
pub struct RocksMapRef<'a, K, V>
where
    K: Serialize + DeserializeOwned + Clone,
    V: Serialize + DeserializeOwned + Clone,
{
    db: &'a DB,
    cf_name: Option<String>,
    marker: PhantomData<(K, V)>,
}

impl<'a, K, V> RocksMapRef<'a, K, V>
where
    K: Serialize + DeserializeOwned + Clone,
    V: Serialize + DeserializeOwned + Clone,
{
    /// Returns a reference to the underlying database
    pub fn db(&self) -> &DB {
        self.db
    }

    /// Retrieve a value by key
    pub fn get(&self, key: &K) -> Result<Option<V>> {
        let key_bytes = <BincodeCodec<K> as KeyCodec<K>>::encode(key)?;
        let result = match &self.cf_name {
            Some(cf_name) => {
                let cf = self
                    .db
                    .cf_handle(cf_name)
                    .ok_or_else(|| Error::ColumnFamilyNotFound(cf_name.clone()))?;
                self.db.get_cf(cf, key_bytes)
            }
            None => self.db.get(key_bytes),
        }
        .map_err(Error::from)?;

        match result {
            Some(value_bytes) => Ok(Some(<BincodeCodec<V> as ValueCodec<V>>::decode(
                &value_bytes,
            )?)),
            None => Ok(None),
        }
    }

    /// Store a value with the given key
    pub fn put(&self, key: &K, value: &V) -> Result<()> {
        let key_bytes = <BincodeCodec<K> as KeyCodec<K>>::encode(key)?;
        let value_bytes = <BincodeCodec<V> as ValueCodec<V>>::encode(value)?;

        match &self.cf_name {
            Some(cf_name) => {
                let cf = self
                    .db
                    .cf_handle(cf_name)
                    .ok_or_else(|| Error::ColumnFamilyNotFound(cf_name.clone()))?;
                self.db.put_cf(cf, key_bytes, value_bytes)
            }
            None => self.db.put(key_bytes, value_bytes),
        }
        .map_err(Error::from)
    }

    /// Delete a key-value pair
    pub fn delete(&self, key: &K) -> Result<()> {
        let key_bytes = <BincodeCodec<K> as KeyCodec<K>>::encode(key)?;

        match &self.cf_name {
            Some(cf_name) => {
                let cf = self
                    .db
                    .cf_handle(cf_name)
                    .ok_or_else(|| Error::ColumnFamilyNotFound(cf_name.clone()))?;
                self.db.delete_cf(cf, key_bytes)
            }
            None => self.db.delete(key_bytes),
        }
        .map_err(Error::from)
    }

    /// Returns a batch operation builder that can be used to perform multiple
    /// operations in a single atomic batch
    pub fn batch(&self) -> crate::batch::RocksMapBatch<K, V> {
        crate::batch::RocksMapBatch::new(self.db, self.cf_name.clone())
    }

    /// Iterator over all key-value pairs
    pub fn iter(&self) -> Result<RocksMapIterator<K, V>> {
        let mode = IteratorMode::Start;
        let iter = match &self.cf_name {
            Some(cf_name) => {
                let cf = self
                    .db
                    .cf_handle(cf_name)
                    .ok_or_else(|| Error::ColumnFamilyNotFound(cf_name.clone()))?;
                self.db.iterator_cf(cf, mode)
            }
            None => self.db.iterator(mode),
        };

        Ok(RocksMapIterator {
            inner: iter,
            marker: PhantomData,
            end_condition: Box::new(|_| false),
            prefix_filter: None,
        })
    }

    /// Range query: Retrieve all key-value pairs within a range [from, to]
    pub fn range(&self, from: &K, to: &K) -> Result<RocksMapIterator<K, V>> {
        let from_bytes = <BincodeCodec<K> as KeyCodec<K>>::encode(from)?;

        let iter = match &self.cf_name {
            Some(cf_name) => {
                let cf = self
                    .db
                    .cf_handle(cf_name)
                    .ok_or_else(|| Error::ColumnFamilyNotFound(cf_name.clone()))?;
                let mode = IteratorMode::From(&from_bytes, Direction::Forward);
                self.db.iterator_cf(cf, mode)
            }
            None => {
                let mode = IteratorMode::From(&from_bytes, Direction::Forward);
                self.db.iterator(mode)
            }
        };

        let to_bytes = <BincodeCodec<K> as KeyCodec<K>>::encode(to)?;

        Ok(RocksMapIterator {
            inner: iter,
            marker: PhantomData,
            end_condition: Box::new(move |key| key > &to_bytes),
            prefix_filter: None,
        })
    }

    /// Prefix scan: Retrieve all key-value pairs with keys starting with the given prefix
    pub fn prefix_scan(&self, prefix: &K) -> Result<RocksMapIterator<K, V>> {
        let iter = match &self.cf_name {
            Some(cf_name) => {
                let cf = self
                    .db
                    .cf_handle(cf_name)
                    .ok_or_else(|| Error::ColumnFamilyNotFound(cf_name.clone()))?;
                self.db.iterator_cf(cf, IteratorMode::Start)
            }
            None => self.db.iterator(IteratorMode::Start),
        };

        let prefix_clone = prefix.clone();

        Ok(RocksMapIterator {
            inner: iter,
            marker: PhantomData,
            end_condition: Box::new(move |_key_bytes| false),
            prefix_filter: Some(prefix_clone),
        })
    }
}

impl<K, V> RocksMap<K, V>
where
    K: Serialize + DeserializeOwned + Clone,
    V: Serialize + DeserializeOwned + Clone,
{
    pub fn get(&self, key: &K) -> Result<Option<V>> {
        let key_bytes = <BincodeCodec<K> as KeyCodec<K>>::encode(key)?;
        let result = match &self.cf_name {
            Some(cf_name) => {
                let cf = self
                    .db
                    .cf_handle(cf_name)
                    .ok_or_else(|| Error::ColumnFamilyNotFound(cf_name.clone()))?;
                self.db.get_cf(cf, key_bytes)
            }
            None => self.db.get(key_bytes),
        }
        .map_err(Error::from)?;

        match result {
            Some(value_bytes) => Ok(Some(<BincodeCodec<V> as ValueCodec<V>>::decode(
                &value_bytes,
            )?)),
            None => Ok(None),
        }
    }

    /// Store a value with the given key
    pub fn put(&self, key: K, value: &V) -> Result<()> {
        let key_bytes = <BincodeCodec<K> as KeyCodec<K>>::encode(&key)?;
        let value_bytes = <BincodeCodec<V> as ValueCodec<V>>::encode(value)?;

        match &self.cf_name {
            Some(cf_name) => {
                let cf = self
                    .db
                    .cf_handle(cf_name)
                    .ok_or_else(|| Error::ColumnFamilyNotFound(cf_name.clone()))?;
                self.db.put_cf(cf, key_bytes, value_bytes)
            }
            None => self.db.put(key_bytes, value_bytes),
        }
        .map_err(Error::from)?;

        Ok(())
    }

    /// Delete a key-value pair
    pub fn delete(&self, key: &K) -> Result<()> {
        let key_bytes = <BincodeCodec<K> as KeyCodec<K>>::encode(key)?;

        match &self.cf_name {
            Some(cf_name) => {
                let cf = self
                    .db
                    .cf_handle(cf_name)
                    .ok_or_else(|| Error::ColumnFamilyNotFound(cf_name.clone()))?;
                self.db.delete_cf(cf, key_bytes)
            }
            None => self.db.delete(key_bytes),
        }
        .map_err(Error::from)?;

        Ok(())
    }

    /// Iterator over all key-value pairs
    pub fn iter(&self) -> Result<RocksMapIterator<K, V>> {
        let mode = IteratorMode::Start;
        let iter = match &self.cf_name {
            Some(cf_name) => {
                let cf = self
                    .db
                    .cf_handle(cf_name)
                    .ok_or_else(|| Error::ColumnFamilyNotFound(cf_name.clone()))?;
                self.db.iterator_cf(cf, mode)
            }
            None => self.db.iterator(mode),
        };

        Ok(RocksMapIterator {
            inner: iter,
            marker: PhantomData,
            end_condition: Box::new(|_| false),
            prefix_filter: None,
        })
    }

    /// Create a batch operation instance for this database
    pub fn batch(&self) -> crate::batch::RocksMapBatch<K, V> {
        crate::batch::RocksMapBatch::new(&self.db, self.cf_name.clone())
    }

    /// Range query: Retrieve all key-value pairs within a range [from, to]
    pub fn range(&self, from: &K, to: &K) -> Result<RocksMapIterator<K, V>> {
        let from_bytes = <BincodeCodec<K> as KeyCodec<K>>::encode(from)?;

        let iter = match &self.cf_name {
            Some(cf_name) => {
                let cf = self
                    .db
                    .cf_handle(cf_name)
                    .ok_or_else(|| Error::ColumnFamilyNotFound(cf_name.clone()))?;
                let mode = IteratorMode::From(&from_bytes, Direction::Forward);
                self.db.iterator_cf(cf, mode)
            }
            None => {
                let mode = IteratorMode::From(&from_bytes, Direction::Forward);
                self.db.iterator(mode)
            }
        };

        let to_bytes = <BincodeCodec<K> as KeyCodec<K>>::encode(to)?;

        Ok(RocksMapIterator {
            inner: iter,
            marker: PhantomData,
            end_condition: Box::new(move |key| key > &to_bytes),
            prefix_filter: None,
        })
    }

    /// Prefix scan: Retrieve all key-value pairs with keys starting with the given prefix
    /// Note: This is a simplified implementation that works by iterating all keys
    pub fn prefix_scan(&self, prefix: &K) -> Result<RocksMapIterator<K, V>> {
        let iter = match &self.cf_name {
            Some(cf_name) => {
                let cf = self
                    .db
                    .cf_handle(cf_name)
                    .ok_or_else(|| Error::ColumnFamilyNotFound(cf_name.clone()))?;
                self.db.iterator_cf(cf, IteratorMode::Start)
            }
            None => self.db.iterator(IteratorMode::Start),
        };

        let prefix_clone = prefix.clone();

        Ok(RocksMapIterator {
            inner: iter,
            marker: PhantomData,
            end_condition: Box::new(move |_key_bytes| false),
            prefix_filter: Some(prefix_clone),
        })
    }
}

/// Iterator over RocksMap key-value pairs
pub struct RocksMapIterator<'a, K, V>
where
    K: Serialize + DeserializeOwned,
    V: Serialize + DeserializeOwned,
{
    inner: rocksdb::DBIterator<'a>,
    marker: PhantomData<(K, V)>,
    end_condition: Box<dyn Fn(&[u8]) -> bool>,
    prefix_filter: Option<K>,
}

impl<'a, K, V> Iterator for RocksMapIterator<'a, K, V>
where
    K: Serialize + serde::de::DeserializeOwned + std::fmt::Debug,
    V: Serialize + serde::de::DeserializeOwned,
{
    type Item = Result<(K, V)>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let result = self.inner.next()?;

            if let Ok((ref key_bytes, _)) = result {
                if (self.end_condition)(key_bytes) {
                    return None;
                }
            }

            let decoded_result =
                result
                    .map_err(Error::from)
                    .and_then(|(key_bytes, value_bytes)| {
                        let key = <BincodeCodec<K> as KeyCodec<K>>::decode(&key_bytes)?;
                        let value = <BincodeCodec<V> as ValueCodec<V>>::decode(&value_bytes)?;
                        Ok((key, value))
                    });

            if let Some(ref prefix) = self.prefix_filter {
                if let Ok((ref key, _)) = decoded_result {
                    let key_str = format!("{:?}", key).trim_matches('"').to_string();
                    let prefix_str = format!("{:?}", prefix).trim_matches('"').to_string();

                    if key_str.starts_with(&prefix_str) {
                        return Some(decoded_result);
                    }
                    continue;
                }
            }

            return Some(decoded_result);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::{Deserialize, Serialize};
    use tempfile::TempDir;

    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
    struct TestUser {
        id: u64,
        name: String,
        active: bool,
    }

    #[test]
    fn test_basic_operations() {
        let temp_dir = TempDir::new().unwrap();
        let db = RocksMap::<u64, TestUser>::open(temp_dir.path()).unwrap();

        let user = TestUser {
            id: 1,
            name: "Alice".to_string(),
            active: true,
        };
        db.put(1, &user).unwrap();

        let retrieved = db.get(&1).unwrap().unwrap();
        assert_eq!(retrieved, user);

        db.delete(&1).unwrap();
        assert!(db.get(&1).unwrap().is_none());
    }

    #[test]
    fn test_column_family() {
        let temp_dir = TempDir::new().unwrap();
        let mut db = RocksMap::<u64, TestUser>::open(temp_dir.path()).unwrap();

        let user = TestUser {
            id: 1,
            name: "Bob".to_string(),
            active: true,
        };

        let setting = TestUser {
            id: 1,
            name: "dark-mode".to_string(),
            active: true,
        };

        {
            let users_cf = db.column_family("users").unwrap();
            users_cf.put(&1, &user).unwrap();
            let user_from_cf = users_cf.get(&1).unwrap().unwrap();
            assert_eq!(user_from_cf, user);
        }

        {
            let settings_cf = db.column_family("settings").unwrap();
            settings_cf.put(&1, &setting).unwrap();
            let setting_from_cf = settings_cf.get(&1).unwrap().unwrap();
            assert_eq!(setting_from_cf, setting);
        }
    }

    #[test]
    fn test_iterator() {
        let temp_dir = TempDir::new().unwrap();
        let db = RocksMap::<u64, TestUser>::open(temp_dir.path()).unwrap();

        for i in 1..=5 {
            let user = TestUser {
                id: i,
                name: format!("User-{}", i),
                active: i % 2 == 0,
            };
            db.put(i, &user).unwrap();
        }

        let mut count = 0;
        for (_count, item) in db.iter().unwrap().enumerate() {
            let (key, value) = item.unwrap();
            assert_eq!(key, value.id);
            count += 1;
        }

        assert_eq!(count, 5);
    }

    #[test]
    fn test_range_query() {
        let temp_dir = TempDir::new().unwrap();
        let db = RocksMap::<u64, TestUser>::open(temp_dir.path()).unwrap();

        for i in 1..=10 {
            let user = TestUser {
                id: i,
                name: format!("User-{}", i),
                active: i % 2 == 0,
            };
            db.put(i, &user).unwrap();
        }

        let mut _count = 0;
        let mut ids = Vec::new();
        for result in db.range(&3, &7).unwrap() {
            let (key, value) = result.unwrap();
            assert_eq!(key, value.id);
            ids.push(key);
            _count += 1;
        }

        assert!(ids.contains(&3));
        assert!(ids.contains(&4));
        assert!(ids.contains(&5));
        assert!(ids.contains(&6));
        assert!(ids.contains(&7));
        assert!(!ids.contains(&2));
        assert!(!ids.contains(&8));
    }

    #[test]
    fn test_prefix_scan() {
        let temp_dir = TempDir::new().unwrap();
        let db = RocksMap::<String, String>::open(temp_dir.path()).unwrap();

        let test_data = vec![
            ("user:001".to_string(), "Alice".to_string()),
            ("user:002".to_string(), "Bob".to_string()),
            ("user:003".to_string(), "Charlie".to_string()),
            ("post:001".to_string(), "Hello World".to_string()),
            ("post:002".to_string(), "Another Post".to_string()),
        ];

        for (key, value) in &test_data {
            db.put(key.clone(), value).unwrap();
        }

        let mut user_count = 0;
        let prefix = "user:".to_string();
        for result in db.prefix_scan(&prefix).unwrap() {
            let (key, _) = result.unwrap();
            assert!(key.starts_with("user:"));
            user_count += 1;
        }

        assert_eq!(user_count, 3);

        let mut post_count = 0;
        let prefix = "post:".to_string();
        for result in db.prefix_scan(&prefix).unwrap() {
            let (key, _) = result.unwrap();
            assert!(key.starts_with("post:"));
            post_count += 1;
        }

        assert_eq!(post_count, 2);
    }
}
