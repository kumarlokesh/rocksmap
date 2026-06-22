//! Atomic, consistent secondary indexes over a single transactional RocksDB database.
//!
//! [`IndexedRocksMap`] stores the data and every secondary index in one [`TransactionDB`], so a
//! `put`/`delete` updates the row and all index entries in a single transaction — a crash or a
//! concurrent writer can never leave an index diverged from the data. Indexes are declared up
//! front via the builder, which returns a typed [`Index`] handle used for lookups.
//!
//! Index entry layout:
//! - non-unique: composite key `encode((secondary_key, primary_key)) -> []`; a lookup is a
//!   tuple prefix scan.
//! - unique: `encode(secondary_key) -> encode(primary_key)`; a lookup is a point read, and a
//!   second primary key for the same secondary key is rejected.

use crate::codec::{BincodeCodec, KeyCodec, ValueCodec};
use crate::error::{Error, Result};
use crate::meta::{self, MapKind};
use crate::ordered::{OrderedCodec, OrderedKey};
use rocksdb::{
    ColumnFamily, ColumnFamilyDescriptor, IteratorMode, Options, ReadOptions, TransactionDB,
    TransactionDBOptions,
};
use serde::{de::DeserializeOwned, Serialize};
use std::marker::PhantomData;
use std::path::{Path, PathBuf};

const DATA_CF: &str = "default";

/// Smallest byte string greater than every string starting with `prefix`, or `None` if none
/// exists (empty prefix or all trailing `0xFF`).
fn byte_successor(prefix: &[u8]) -> Option<Vec<u8>> {
    let mut out = prefix.to_vec();
    while let Some(last) = out.last_mut() {
        if *last != 0xFF {
            *last += 1;
            return Some(out);
        }
        out.pop();
    }
    None
}

/// Encodes a value's secondary key to ordered bytes, or `None` to skip indexing that value.
type Extractor<V> = Box<dyn Fn(&V) -> Result<Option<Vec<u8>>> + Send + Sync>;

struct IndexDef<V> {
    name: String,
    cf_name: String,
    unique: bool,
    extract: Extractor<V>,
}

/// A typed handle to a declared secondary index, returned by the builder. The `SK` type binds
/// the lookup key type so `find_by`/`find_keys_by` can only be called with the right key.
pub struct Index<SK> {
    name: String,
    _marker: PhantomData<fn() -> SK>,
}

impl<SK> Clone for Index<SK> {
    fn clone(&self) -> Self {
        Index {
            name: self.name.clone(),
            _marker: PhantomData,
        }
    }
}

/// Builder that declares the indexes for an [`IndexedRocksMap`] before opening it.
pub struct IndexedRocksMapBuilder<K, V> {
    path: PathBuf,
    indexes: Vec<IndexDef<V>>,
    _marker: PhantomData<(K, V)>,
}

impl<K, V> IndexedRocksMapBuilder<K, V>
where
    K: Serialize + DeserializeOwned + Clone + OrderedKey,
    V: Serialize + DeserializeOwned + Clone,
{
    /// Declare a non-unique index named `name`; `extract` derives the secondary key from a
    /// value (return `None` to leave a value out of the index).
    pub fn index<SK, F>(&mut self, name: &str, extract: F) -> Index<SK>
    where
        SK: OrderedKey + 'static,
        F: Fn(&V) -> Option<SK> + Send + Sync + 'static,
    {
        self.register(name, false, extract)
    }

    /// Declare a unique index: at most one primary key may map to a given secondary key.
    pub fn unique_index<SK, F>(&mut self, name: &str, extract: F) -> Index<SK>
    where
        SK: OrderedKey + 'static,
        F: Fn(&V) -> Option<SK> + Send + Sync + 'static,
    {
        self.register(name, true, extract)
    }

    fn register<SK, F>(&mut self, name: &str, unique: bool, extract: F) -> Index<SK>
    where
        SK: OrderedKey + 'static,
        F: Fn(&V) -> Option<SK> + Send + Sync + 'static,
    {
        let extractor: Extractor<V> = Box::new(move |v| match extract(v) {
            None => Ok(None),
            Some(sk) => Ok(Some(<OrderedCodec<SK> as KeyCodec<SK>>::encode(&sk)?)),
        });
        self.indexes.push(IndexDef {
            name: name.to_string(),
            cf_name: format!("__idx_{name}"),
            unique,
            extract: extractor,
        });
        Index {
            name: name.to_string(),
            _marker: PhantomData,
        }
    }

    /// Open the database, creating column families and verifying the metadata.
    pub fn open(self) -> Result<IndexedRocksMap<K, V>> {
        IndexedRocksMap::open_internal(self.path, self.indexes)
    }
}

/// A typed map with one or more atomically-maintained secondary indexes.
pub struct IndexedRocksMap<K, V>
where
    K: Serialize + DeserializeOwned + Clone + OrderedKey,
    V: Serialize + DeserializeOwned + Clone,
{
    db: TransactionDB,
    indexes: Vec<IndexDef<V>>,
    _marker: PhantomData<(K, V)>,
}

impl<K, V> IndexedRocksMap<K, V>
where
    K: Serialize + DeserializeOwned + Clone + OrderedKey,
    V: Serialize + DeserializeOwned + Clone,
{
    /// Start building an indexed map at `path`.
    pub fn builder<P: AsRef<Path>>(path: P) -> IndexedRocksMapBuilder<K, V> {
        IndexedRocksMapBuilder {
            path: path.as_ref().to_path_buf(),
            indexes: Vec::new(),
            _marker: PhantomData,
        }
    }

    fn open_internal(path: PathBuf, indexes: Vec<IndexDef<V>>) -> Result<Self> {
        if !path.exists() {
            std::fs::create_dir_all(&path).map_err(|_| Error::InvalidPath(path.clone()))?;
        }

        let mut db_opts = Options::default();
        db_opts.create_if_missing(true);
        db_opts.create_missing_column_families(true);

        let idx_cf_names: Vec<String> = indexes.iter().map(|i| i.cf_name.clone()).collect();
        let idx_cf_refs: Vec<&str> = idx_cf_names.iter().map(String::as_str).collect();
        let names = meta::all_cf_names(&db_opts, &path, &idx_cf_refs);
        let descriptors: Vec<ColumnFamilyDescriptor> = names
            .iter()
            .map(|n| ColumnFamilyDescriptor::new(n, Options::default()))
            .collect();

        let txn_db_opts = TransactionDBOptions::default();
        let db = TransactionDB::open_cf_descriptors(&db_opts, &txn_db_opts, &path, descriptors)
            .map_err(Error::from)?;

        meta::verify_or_write_kind(&db, MapKind::Indexed)?;
        let mut sorted_names: Vec<String> = indexes.iter().map(|i| i.name.clone()).collect();
        sorted_names.sort();
        meta::verify_or_write_indexes(&db, &sorted_names)?;

        let map = Self {
            db,
            indexes,
            _marker: PhantomData,
        };
        map.resume_pending_rebuild()?;
        Ok(map)
    }

    fn cf(&self, name: &str) -> Result<&ColumnFamily> {
        self.db
            .cf_handle(name)
            .ok_or_else(|| Error::ColumnFamilyNotFound(name.to_string()))
    }

    fn index_def(&self, name: &str) -> Result<&IndexDef<V>> {
        self.indexes
            .iter()
            .find(|i| i.name == name)
            .ok_or_else(|| Error::Other(format!("unknown index `{name}`")))
    }

    /// Retrieve a value by primary key.
    pub fn get(&self, key: &K) -> Result<Option<V>> {
        let data_cf = self.cf(DATA_CF)?;
        let key_bytes = <OrderedCodec<K> as KeyCodec<K>>::encode(key)?;
        match self.db.get_cf(data_cf, key_bytes).map_err(Error::from)? {
            None => Ok(None),
            Some(bytes) => Ok(Some(<BincodeCodec<V> as ValueCodec<V>>::decode(&bytes)?)),
        }
    }

    /// Returns `true` if the map contains a value for `key`.
    pub fn contains(&self, key: &K) -> Result<bool> {
        let data_cf = self.cf(DATA_CF)?;
        let key_bytes = <OrderedCodec<K> as KeyCodec<K>>::encode(key)?;
        Ok(self
            .db
            .get_cf(data_cf, key_bytes)
            .map_err(Error::from)?
            .is_some())
    }

    /// Returns `true` if the map has no entries.
    pub fn is_empty(&self) -> Result<bool> {
        let data_cf = self.cf(DATA_CF)?;
        match self.db.iterator_cf(data_cf, IteratorMode::Start).next() {
            None => Ok(true),
            Some(Ok(_)) => Ok(false),
            Some(Err(e)) => Err(Error::from(e)),
        }
    }

    /// Exact number of entries. **O(n)** — performs a full scan of the data.
    pub fn count(&self) -> Result<usize> {
        let data_cf = self.cf(DATA_CF)?;
        let mut count = 0;
        for item in self.db.iterator_cf(data_cf, IteratorMode::Start) {
            item.map_err(Error::from)?;
            count += 1;
        }
        Ok(count)
    }

    /// Insert or replace a value, updating every index in one transaction.
    pub fn put(&self, key: K, value: &V) -> Result<()> {
        let key_bytes = <OrderedCodec<K> as KeyCodec<K>>::encode(&key)?;
        let data_cf = self.cf(DATA_CF)?;
        let txn = self.db.transaction();

        let old_value: Option<V> = match txn
            .get_for_update_cf(data_cf, &key_bytes, true)
            .map_err(Error::from)?
        {
            Some(bytes) => Some(<BincodeCodec<V> as ValueCodec<V>>::decode(&bytes)?),
            None => None,
        };

        for idx in &self.indexes {
            let idx_cf = self.cf(&idx.cf_name)?;
            let old_sk = match &old_value {
                Some(ov) => (idx.extract)(ov)?,
                None => None,
            };
            let new_sk = (idx.extract)(value)?;
            if old_sk == new_sk {
                continue; // index entry unchanged
            }

            if let Some(osk) = old_sk {
                if idx.unique {
                    txn.delete_cf(idx_cf, &osk).map_err(Error::from)?;
                } else {
                    let mut entry = osk;
                    entry.extend_from_slice(&key_bytes);
                    txn.delete_cf(idx_cf, &entry).map_err(Error::from)?;
                }
            }

            if let Some(nsk) = new_sk {
                if idx.unique {
                    if let Some(existing) = txn
                        .get_for_update_cf(idx_cf, &nsk, true)
                        .map_err(Error::from)?
                    {
                        if existing != key_bytes {
                            return Err(Error::UniqueViolation(format!(
                                "index `{}` already has an entry for this secondary key",
                                idx.name
                            )));
                        }
                    }
                    txn.put_cf(idx_cf, &nsk, &key_bytes).map_err(Error::from)?;
                } else {
                    let mut entry = nsk;
                    entry.extend_from_slice(&key_bytes);
                    txn.put_cf(idx_cf, &entry, b"").map_err(Error::from)?;
                }
            }
        }

        let value_bytes = <BincodeCodec<V> as ValueCodec<V>>::encode(value)?;
        txn.put_cf(data_cf, &key_bytes, value_bytes)
            .map_err(Error::from)?;
        txn.commit().map_err(Error::from)
    }

    /// Delete a value and all of its index entries in one transaction.
    pub fn delete(&self, key: &K) -> Result<()> {
        let key_bytes = <OrderedCodec<K> as KeyCodec<K>>::encode(key)?;
        let data_cf = self.cf(DATA_CF)?;
        let txn = self.db.transaction();

        let old = txn
            .get_for_update_cf(data_cf, &key_bytes, true)
            .map_err(Error::from)?;
        let Some(bytes) = old else {
            return Ok(()); // nothing to delete; transaction drops (no-op)
        };
        let value = <BincodeCodec<V> as ValueCodec<V>>::decode(&bytes)?;

        for idx in &self.indexes {
            let idx_cf = self.cf(&idx.cf_name)?;
            if let Some(sk) = (idx.extract)(&value)? {
                if idx.unique {
                    txn.delete_cf(idx_cf, &sk).map_err(Error::from)?;
                } else {
                    let mut entry = sk;
                    entry.extend_from_slice(&key_bytes);
                    txn.delete_cf(idx_cf, &entry).map_err(Error::from)?;
                }
            }
        }

        txn.delete_cf(data_cf, &key_bytes).map_err(Error::from)?;
        txn.commit().map_err(Error::from)
    }

    /// Primary keys whose value maps to `secondary_key` under `index`, in ascending order.
    pub fn find_keys_by<SK: OrderedKey>(
        &self,
        index: &Index<SK>,
        secondary_key: &SK,
    ) -> Result<Vec<K>> {
        let def = self.index_def(&index.name)?;
        let idx_cf = self.cf(&def.cf_name)?;
        let sk_bytes = <OrderedCodec<SK> as KeyCodec<SK>>::encode(secondary_key)?;

        if def.unique {
            return match self.db.get_cf(idx_cf, &sk_bytes).map_err(Error::from)? {
                Some(pk_bytes) => Ok(vec![<OrderedCodec<K> as KeyCodec<K>>::decode(&pk_bytes)?]),
                None => Ok(Vec::new()),
            };
        }

        let mut readopts = ReadOptions::default();
        readopts.set_iterate_lower_bound(sk_bytes.clone());
        if let Some(upper) = byte_successor(&sk_bytes) {
            readopts.set_iterate_upper_bound(upper);
        }
        let mut keys = Vec::new();
        for item in self
            .db
            .iterator_cf_opt(idx_cf, readopts, IteratorMode::Start)
        {
            let (entry, _) = item.map_err(Error::from)?;
            // entry = sk_bytes ++ pk_bytes; the primary key is the suffix.
            let pk_bytes = &entry[sk_bytes.len()..];
            keys.push(<OrderedCodec<K> as KeyCodec<K>>::decode(pk_bytes)?);
        }
        Ok(keys)
    }

    /// Values whose secondary key matches `secondary_key` under `index`.
    pub fn find_by<SK: OrderedKey>(&self, index: &Index<SK>, secondary_key: &SK) -> Result<Vec<V>> {
        let keys = self.find_keys_by(index, secondary_key)?;
        let data_cf = self.cf(DATA_CF)?;
        let mut values = Vec::with_capacity(keys.len());
        for key in &keys {
            let key_bytes = <OrderedCodec<K> as KeyCodec<K>>::encode(key)?;
            if let Some(bytes) = self.db.get_cf(data_cf, key_bytes).map_err(Error::from)? {
                values.push(<BincodeCodec<V> as ValueCodec<V>>::decode(&bytes)?);
            }
        }
        Ok(values)
    }

    /// Rebuild a single index from the data (for recovery, or after changing an extractor).
    pub fn rebuild<SK>(&self, index: &Index<SK>) -> Result<()> {
        self.rebuild_named(&index.name)
    }

    /// Rebuild every index from the data.
    pub fn rebuild_all(&self) -> Result<()> {
        let names: Vec<String> = self.indexes.iter().map(|i| i.name.clone()).collect();
        for name in names {
            self.rebuild_named(&name)?;
        }
        Ok(())
    }

    fn rebuild_named(&self, name: &str) -> Result<()> {
        let def = self.index_def(name)?;
        let idx_cf = self.cf(&def.cf_name)?;
        let data_cf = self.cf(DATA_CF)?;

        meta::set_rebuilding(&self.db, name)?;

        // Clear the index CF (collect keys first, then delete, to avoid mutating mid-iteration).
        let stale: Vec<Box<[u8]>> = self
            .db
            .iterator_cf(idx_cf, IteratorMode::Start)
            .map(|item| item.map(|(k, _)| k).map_err(Error::from))
            .collect::<Result<_>>()?;
        for key in stale {
            self.db.delete_cf(idx_cf, key).map_err(Error::from)?;
        }

        // Repopulate by scanning the data CF.
        for item in self.db.iterator_cf(data_cf, IteratorMode::Start) {
            let (key_bytes, value_bytes) = item.map_err(Error::from)?;
            let value = <BincodeCodec<V> as ValueCodec<V>>::decode(&value_bytes)?;
            if let Some(sk) = (def.extract)(&value)? {
                if def.unique {
                    if let Some(existing) = self.db.get_cf(idx_cf, &sk).map_err(Error::from)? {
                        if existing[..] != key_bytes[..] {
                            return Err(Error::UniqueViolation(format!(
                                "rebuild of index `{name}` found duplicate secondary keys"
                            )));
                        }
                    }
                    self.db
                        .put_cf(idx_cf, &sk, &key_bytes)
                        .map_err(Error::from)?;
                } else {
                    let mut entry = sk;
                    entry.extend_from_slice(&key_bytes);
                    self.db.put_cf(idx_cf, &entry, b"").map_err(Error::from)?;
                }
            }
        }

        meta::clear_rebuilding(&self.db)
    }

    fn resume_pending_rebuild(&self) -> Result<()> {
        if let Some(name) = meta::get_rebuilding(&self.db)? {
            if self.indexes.iter().any(|i| i.name == name) {
                self.rebuild_named(&name)?;
            } else {
                meta::clear_rebuilding(&self.db)?;
            }
        }
        Ok(())
    }

    /// Access the underlying transactional RocksDB handle.
    pub fn db(&self) -> &TransactionDB {
        &self.db
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::{Deserialize, Serialize};
    use tempfile::TempDir;

    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
    struct User {
        id: u64,
        email: String,
        org: String,
    }

    fn user(id: u64, email: &str, org: &str) -> User {
        User {
            id,
            email: email.to_string(),
            org: org.to_string(),
        }
    }

    #[test]
    fn lookup_by_secondary_key() {
        let dir = TempDir::new().unwrap();
        let mut builder = IndexedRocksMap::<u64, User>::builder(dir.path());
        let by_org = builder.index("by_org", |u: &User| Some(u.org.clone()));
        let map = builder.open().unwrap();

        map.put(1, &user(1, "a@x.com", "x")).unwrap();
        map.put(2, &user(2, "b@x.com", "x")).unwrap();
        map.put(3, &user(3, "c@y.com", "y")).unwrap();

        let mut emails: Vec<String> = map
            .find_by(&by_org, &"x".to_string())
            .unwrap()
            .into_iter()
            .map(|u| u.email)
            .collect();
        emails.sort();
        assert_eq!(emails, vec!["a@x.com", "b@x.com"]);

        assert_eq!(
            map.find_keys_by(&by_org, &"y".to_string()).unwrap(),
            vec![3]
        );
        assert!(map.find_by(&by_org, &"z".to_string()).unwrap().is_empty());
    }

    #[test]
    fn update_removes_stale_index_entry() {
        let dir = TempDir::new().unwrap();
        let mut builder = IndexedRocksMap::<u64, User>::builder(dir.path());
        let by_email = builder.index("by_email", |u: &User| Some(u.email.clone()));
        let map = builder.open().unwrap();

        map.put(1, &user(1, "old@x.com", "x")).unwrap();
        assert_eq!(
            map.find_keys_by(&by_email, &"old@x.com".to_string())
                .unwrap(),
            vec![1]
        );

        // Change the indexed field: the old entry must disappear, the new one appear.
        map.put(1, &user(1, "new@x.com", "x")).unwrap();
        assert!(map
            .find_by(&by_email, &"old@x.com".to_string())
            .unwrap()
            .is_empty());
        assert_eq!(
            map.find_keys_by(&by_email, &"new@x.com".to_string())
                .unwrap(),
            vec![1]
        );
    }

    #[test]
    fn delete_clears_index_entries() {
        let dir = TempDir::new().unwrap();
        let mut builder = IndexedRocksMap::<u64, User>::builder(dir.path());
        let by_org = builder.index("by_org", |u: &User| Some(u.org.clone()));
        let map = builder.open().unwrap();

        map.put(1, &user(1, "a@x.com", "x")).unwrap();
        map.delete(&1).unwrap();

        assert!(map.get(&1).unwrap().is_none());
        assert!(map.find_by(&by_org, &"x".to_string()).unwrap().is_empty());
    }

    #[test]
    fn unique_index_rejects_duplicate_and_rolls_back() {
        let dir = TempDir::new().unwrap();
        let mut builder = IndexedRocksMap::<u64, User>::builder(dir.path());
        let by_email = builder.unique_index("uniq_email", |u: &User| Some(u.email.clone()));
        let map = builder.open().unwrap();

        map.put(1, &user(1, "a@x.com", "x")).unwrap();
        let err = map.put(2, &user(2, "a@x.com", "y"));
        assert!(matches!(err, Err(Error::UniqueViolation(_))));

        // The rejected write left no trace: row 2 absent, index still points to 1.
        assert!(map.get(&2).unwrap().is_none());
        assert_eq!(
            map.find_keys_by(&by_email, &"a@x.com".to_string()).unwrap(),
            vec![1]
        );
    }

    #[test]
    fn multiple_indexes_stay_consistent() {
        let dir = TempDir::new().unwrap();
        let mut builder = IndexedRocksMap::<u64, User>::builder(dir.path());
        let by_org = builder.index("by_org", |u: &User| Some(u.org.clone()));
        let by_email = builder.unique_index("by_email", |u: &User| Some(u.email.clone()));
        let map = builder.open().unwrap();

        map.put(1, &user(1, "a@x.com", "x")).unwrap();
        map.put(2, &user(2, "b@x.com", "x")).unwrap();
        map.put(1, &user(1, "a2@x.com", "y")).unwrap(); // move org x->y, email change

        assert_eq!(
            map.find_keys_by(&by_org, &"x".to_string()).unwrap(),
            vec![2]
        );
        assert_eq!(
            map.find_keys_by(&by_org, &"y".to_string()).unwrap(),
            vec![1]
        );
        assert!(map
            .find_by(&by_email, &"a@x.com".to_string())
            .unwrap()
            .is_empty());
        assert_eq!(
            map.find_keys_by(&by_email, &"a2@x.com".to_string())
                .unwrap(),
            vec![1]
        );
    }

    #[test]
    fn rebuild_repopulates_index() {
        let dir = TempDir::new().unwrap();
        let mut builder = IndexedRocksMap::<u64, User>::builder(dir.path());
        let by_org = builder.index("by_org", |u: &User| Some(u.org.clone()));
        let map = builder.open().unwrap();

        map.put(1, &user(1, "a@x.com", "x")).unwrap();
        map.put(2, &user(2, "b@x.com", "x")).unwrap();

        // Corrupt the index by clearing its CF out of band, then rebuild.
        let idx_cf = map.cf("__idx_by_org").unwrap();
        let keys: Vec<Box<[u8]>> = map
            .db()
            .iterator_cf(idx_cf, IteratorMode::Start)
            .map(|r| r.unwrap().0)
            .collect();
        for k in keys {
            map.db().delete_cf(idx_cf, k).unwrap();
        }
        assert!(map.find_by(&by_org, &"x".to_string()).unwrap().is_empty());

        map.rebuild(&by_org).unwrap();
        let mut ids = map.find_keys_by(&by_org, &"x".to_string()).unwrap();
        ids.sort();
        assert_eq!(ids, vec![1, 2]);
    }

    #[test]
    fn open_mismatched_index_set_fails() {
        let dir = TempDir::new().unwrap();
        {
            let mut builder = IndexedRocksMap::<u64, User>::builder(dir.path());
            let _ = builder.index("by_org", |u: &User| Some(u.org.clone()));
            builder.open().unwrap();
        }
        // Reopen declaring a different index set -> FormatMismatch.
        let mut builder = IndexedRocksMap::<u64, User>::builder(dir.path());
        let _ = builder.index("by_email", |u: &User| Some(u.email.clone()));
        assert!(matches!(builder.open(), Err(Error::FormatMismatch(_))));
    }

    #[test]
    fn concurrent_writes_keep_index_consistent() {
        use std::thread;

        let dir = TempDir::new().unwrap();
        let mut builder = IndexedRocksMap::<u64, User>::builder(dir.path());
        let by_org = builder.index("by_org", |u: &User| Some(u.org.clone()));
        let map = builder.open().unwrap();

        thread::scope(|s| {
            // Disjoint writers: ids 0..400, grouped into org0..org3 by id % 4.
            for t in 0..8u64 {
                let map = &map;
                s.spawn(move || {
                    for i in 0..50u64 {
                        let id = t * 50 + i;
                        let org = format!("org{}", id % 4);
                        map.put(id, &user(id, &format!("u{id}@x"), &org)).unwrap();
                    }
                });
            }
            // Contended writers: all hammer the same key with different orgs.
            for v in 0..4u64 {
                let map = &map;
                s.spawn(move || {
                    let org = format!("corg{v}");
                    for _ in 0..50 {
                        map.put(9999, &user(9999, "c@x", &org)).unwrap();
                    }
                });
            }
        });

        // Disjoint keys: each org group has exactly its 100 members.
        for g in 0..4u64 {
            let keys = map.find_keys_by(&by_org, &format!("org{g}")).unwrap();
            assert_eq!(keys.len(), 100);
            assert!(keys.iter().all(|k| k % 4 == g));
        }

        // Contended key: indexed under exactly one org, matching its final value — proving the
        // read-modify-write was isolated (no stale duplicate index entries).
        let final_org = map.get(&9999).unwrap().unwrap().org;
        let mut appearances = 0;
        for v in 0..4u64 {
            let org = format!("corg{v}");
            if map.find_keys_by(&by_org, &org).unwrap().contains(&9999) {
                appearances += 1;
                assert_eq!(org, final_org);
            }
        }
        assert_eq!(
            appearances, 1,
            "contended key must be indexed under exactly one org"
        );
    }
}
