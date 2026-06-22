use crate::{
    codec::{BincodeCodec, KeyCodec, ValueCodec},
    error::{Error, Result},
    meta,
    ordered::{OrderedCodec, OrderedKey, PrefixKey},
};
use rocksdb::{ColumnFamily, ColumnFamilyDescriptor, IteratorMode, Options, ReadOptions, DB};
use serde::{de::DeserializeOwned, Serialize};
use std::{
    marker::PhantomData,
    ops::{Bound, RangeBounds},
    path::Path,
};

const ESTIMATE_NUM_KEYS: &str = "rocksdb.estimate-num-keys";

/// The main key-value store abstraction over RocksDB.
///
/// `K`/`V` are the key/value types; `KC` is the key codec. By default keys use the
/// order-preserving [`OrderedCodec`], which enables `range`/`range_rev` and the prefix scans.
/// Keys that never need ordered queries can opt into [`BincodeCodec`] (or a custom codec); the
/// ordered operations are then simply not available — a compile error rather than a silent
/// wrong result:
///
/// ```compile_fail
/// use rocksmap::{BincodeCodec, RocksMap};
/// let db = RocksMap::<u64, String, BincodeCodec<u64>>::open("db").unwrap();
/// // `range` does not exist for a non-ordered key codec:
/// for _ in db.range(1..=5).unwrap() {}
/// ```
pub struct RocksMap<K, V, KC = OrderedCodec<K>>
where
    K: Serialize + DeserializeOwned + Clone,
    V: Serialize + DeserializeOwned + Clone,
    KC: KeyCodec<K>,
{
    db: DB,
    cf_name: Option<String>,
    _marker: PhantomData<(K, V, KC)>,
}

impl<K, V, KC> RocksMap<K, V, KC>
where
    K: Serialize + DeserializeOwned + Clone,
    V: Serialize + DeserializeOwned + Clone,
    KC: KeyCodec<K>,
{
    /// Opens a new RocksMap at the given path, creating it if it doesn't exist
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        Self::open_with_options(path, Options::default())
    }

    /// Opens a RocksMap with custom options
    pub fn open_with_options<P: AsRef<Path>>(path: P, options: Options) -> Result<Self> {
        Self::open_internal(path, options, &[])
    }

    /// Opens a RocksMap with the specified column families
    pub fn open_with_cfs<P: AsRef<Path>>(
        path: P,
        options: Options,
        column_families: &[&str],
    ) -> Result<Self> {
        Self::open_internal(path, options, column_families)
    }

    fn open_internal<P: AsRef<Path>>(
        path: P,
        mut options: Options,
        extra_cfs: &[&str],
    ) -> Result<Self> {
        let path = path.as_ref().to_path_buf();

        if !path.exists() {
            std::fs::create_dir_all(&path).map_err(|_| Error::InvalidPath(path.clone()))?;
        }

        options.create_if_missing(true);
        options.create_missing_column_families(true);

        // Open every existing column family plus the metadata CF; the metadata CF uses default
        // options (it holds no user data), the rest use the caller's options.
        let names = meta::all_cf_names(&options, &path, extra_cfs);
        let descriptors: Vec<ColumnFamilyDescriptor> = names
            .iter()
            .map(|name| {
                let cf_opts = if name == meta::META_CF {
                    Options::default()
                } else {
                    options.clone()
                };
                ColumnFamilyDescriptor::new(name, cf_opts)
            })
            .collect();

        let db = DB::open_cf_descriptors(&options, &path, descriptors).map_err(Error::from)?;
        meta::verify_or_write_kind(&db, meta::MapKind::Plain)?;
        meta::verify_or_write_key_codec(&db, <KC as KeyCodec<K>>::ID)?;

        Ok(Self {
            db,
            cf_name: None,
            _marker: PhantomData,
        })
    }

    /// Gets a column family handle by name, creating it if it doesn't exist
    pub fn column_family(&mut self, name: &str) -> Result<RocksMapRef<'_, K, V, KC>> {
        if self.db.cf_handle(name).is_none() {
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
    pub fn with_cf(&self, cf_name: &str) -> RocksMapRef<'_, K, V, KC> {
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

    /// Retrieve a value by key
    pub fn get(&self, key: &K) -> Result<Option<V>> {
        get_impl::<K, V, KC>(&self.db, self.cf_name.as_deref(), key)
    }

    /// Store a value with the given key
    pub fn put(&self, key: K, value: &V) -> Result<()> {
        put_impl::<K, V, KC>(&self.db, self.cf_name.as_deref(), &key, value)
    }

    /// Delete a key-value pair
    pub fn delete(&self, key: &K) -> Result<()> {
        delete_impl::<K, KC>(&self.db, self.cf_name.as_deref(), key)
    }

    /// Returns `true` if the map contains a value for `key` (a point lookup).
    pub fn contains(&self, key: &K) -> Result<bool> {
        contains_impl::<K, KC>(&self.db, self.cf_name.as_deref(), key)
    }

    /// Returns `true` if the map has no entries (cheap; checks the first key).
    pub fn is_empty(&self) -> Result<bool> {
        is_empty_impl(&self.db, self.cf_name.as_deref())
    }

    /// Exact number of entries. **O(n)** — performs a full scan.
    pub fn count(&self) -> Result<usize> {
        count_impl(&self.db, self.cf_name.as_deref())
    }

    /// Approximate number of entries from RocksDB's estimate (O(1), not exact).
    pub fn len_estimate(&self) -> usize {
        len_estimate_impl(&self.db, self.cf_name.as_deref())
    }

    /// Create a batch operation instance for this database
    pub fn batch(&self) -> crate::batch::RocksMapBatch<'_, K, V, KC> {
        crate::batch::RocksMapBatch::new(&self.db, self.cf_name.clone())
    }

    /// Iterator over all key-value pairs, in key-codec byte order.
    pub fn iter(&self) -> Result<RocksMapIterator<'_, K, V, KC>> {
        make_iter::<K, V, KC>(&self.db, self.cf_name.as_deref(), None, None, false)
    }
}

/// Ordered queries — only available when keys use the default order-preserving [`OrderedCodec`].
impl<K, V> RocksMap<K, V, OrderedCodec<K>>
where
    K: Serialize + DeserializeOwned + Clone + OrderedKey,
    V: Serialize + DeserializeOwned + Clone,
{
    /// Iterate the key-value pairs whose keys fall in `range`, in ascending key order.
    ///
    /// Accepts any [`RangeBounds`], e.g. `10..=20`, `10..20`, `10..`, `..20`, `..`.
    pub fn range<R: RangeBounds<K>>(
        &self,
        range: R,
    ) -> Result<RocksMapIterator<'_, K, V, OrderedCodec<K>>> {
        let (lower, upper) = range_to_bounds(&range)?;
        make_iter::<K, V, OrderedCodec<K>>(&self.db, self.cf_name.as_deref(), lower, upper, false)
    }

    /// Like [`range`](Self::range) but yields pairs in descending key order.
    pub fn range_rev<R: RangeBounds<K>>(
        &self,
        range: R,
    ) -> Result<RocksMapIterator<'_, K, V, OrderedCodec<K>>> {
        let (lower, upper) = range_to_bounds(&range)?;
        make_iter::<K, V, OrderedCodec<K>>(&self.db, self.cf_name.as_deref(), lower, upper, true)
    }

    /// Iterate all pairs whose (byte-string) key begins with `prefix` (for `String`/`Vec<u8>` keys).
    pub fn scan_prefix(
        &self,
        prefix: &<K as PrefixKey>::Prefix,
    ) -> Result<RocksMapIterator<'_, K, V, OrderedCodec<K>>>
    where
        K: PrefixKey,
    {
        let (lower, upper) = prefix_to_bounds(<K as PrefixKey>::encode_prefix(prefix));
        make_iter::<K, V, OrderedCodec<K>>(
            &self.db,
            self.cf_name.as_deref(),
            Some(lower),
            upper,
            false,
        )
    }

    /// Iterate all pairs whose composite key begins with the given leading fields.
    pub fn scan_prefix_fields<P: OrderedKey>(
        &self,
        prefix: &P,
    ) -> Result<RocksMapIterator<'_, K, V, OrderedCodec<K>>> {
        let (lower, upper) = prefix_to_bounds(encode_ordered(prefix)?);
        make_iter::<K, V, OrderedCodec<K>>(
            &self.db,
            self.cf_name.as_deref(),
            Some(lower),
            upper,
            false,
        )
    }
}

/// A reference to a RocksMap that holds a reference to the database rather than owning it.
/// This allows us to create multiple views into the same database with different column families.
pub struct RocksMapRef<'a, K, V, KC = OrderedCodec<K>>
where
    K: Serialize + DeserializeOwned + Clone,
    V: Serialize + DeserializeOwned + Clone,
    KC: KeyCodec<K>,
{
    db: &'a DB,
    cf_name: Option<String>,
    marker: PhantomData<(K, V, KC)>,
}

impl<'a, K, V, KC> RocksMapRef<'a, K, V, KC>
where
    K: Serialize + DeserializeOwned + Clone,
    V: Serialize + DeserializeOwned + Clone,
    KC: KeyCodec<K>,
{
    /// Returns a reference to the underlying database
    pub fn db(&self) -> &DB {
        self.db
    }

    /// Retrieve a value by key
    pub fn get(&self, key: &K) -> Result<Option<V>> {
        get_impl::<K, V, KC>(self.db, self.cf_name.as_deref(), key)
    }

    /// Store a value with the given key
    pub fn put(&self, key: K, value: &V) -> Result<()> {
        put_impl::<K, V, KC>(self.db, self.cf_name.as_deref(), &key, value)
    }

    /// Delete a key-value pair
    pub fn delete(&self, key: &K) -> Result<()> {
        delete_impl::<K, KC>(self.db, self.cf_name.as_deref(), key)
    }

    /// Returns `true` if the column family contains a value for `key`.
    pub fn contains(&self, key: &K) -> Result<bool> {
        contains_impl::<K, KC>(self.db, self.cf_name.as_deref(), key)
    }

    /// Returns `true` if the column family has no entries.
    pub fn is_empty(&self) -> Result<bool> {
        is_empty_impl(self.db, self.cf_name.as_deref())
    }

    /// Exact number of entries. **O(n)** — performs a full scan.
    pub fn count(&self) -> Result<usize> {
        count_impl(self.db, self.cf_name.as_deref())
    }

    /// Approximate number of entries from RocksDB's estimate (O(1), not exact).
    pub fn len_estimate(&self) -> usize {
        len_estimate_impl(self.db, self.cf_name.as_deref())
    }

    /// Returns a batch operation builder for this column family.
    pub fn batch(&self) -> crate::batch::RocksMapBatch<'_, K, V, KC> {
        crate::batch::RocksMapBatch::new(self.db, self.cf_name.clone())
    }

    /// Iterator over all key-value pairs, in key-codec byte order.
    pub fn iter(&self) -> Result<RocksMapIterator<'_, K, V, KC>> {
        make_iter::<K, V, KC>(self.db, self.cf_name.as_deref(), None, None, false)
    }
}

/// Ordered queries on a column-family view — only when keys use [`OrderedCodec`].
impl<'a, K, V> RocksMapRef<'a, K, V, OrderedCodec<K>>
where
    K: Serialize + DeserializeOwned + Clone + OrderedKey,
    V: Serialize + DeserializeOwned + Clone,
{
    /// Iterate the key-value pairs whose keys fall in `range`, in ascending key order.
    pub fn range<R: RangeBounds<K>>(
        &self,
        range: R,
    ) -> Result<RocksMapIterator<'_, K, V, OrderedCodec<K>>> {
        let (lower, upper) = range_to_bounds(&range)?;
        make_iter::<K, V, OrderedCodec<K>>(self.db, self.cf_name.as_deref(), lower, upper, false)
    }

    /// Like [`range`](Self::range) but yields pairs in descending key order.
    pub fn range_rev<R: RangeBounds<K>>(
        &self,
        range: R,
    ) -> Result<RocksMapIterator<'_, K, V, OrderedCodec<K>>> {
        let (lower, upper) = range_to_bounds(&range)?;
        make_iter::<K, V, OrderedCodec<K>>(self.db, self.cf_name.as_deref(), lower, upper, true)
    }

    /// Iterate all pairs whose (byte-string) key begins with `prefix`.
    pub fn scan_prefix(
        &self,
        prefix: &<K as PrefixKey>::Prefix,
    ) -> Result<RocksMapIterator<'_, K, V, OrderedCodec<K>>>
    where
        K: PrefixKey,
    {
        let (lower, upper) = prefix_to_bounds(<K as PrefixKey>::encode_prefix(prefix));
        make_iter::<K, V, OrderedCodec<K>>(
            self.db,
            self.cf_name.as_deref(),
            Some(lower),
            upper,
            false,
        )
    }

    /// Iterate all pairs whose composite key begins with the given leading fields.
    pub fn scan_prefix_fields<P: OrderedKey>(
        &self,
        prefix: &P,
    ) -> Result<RocksMapIterator<'_, K, V, OrderedCodec<K>>> {
        let (lower, upper) = prefix_to_bounds(encode_ordered(prefix)?);
        make_iter::<K, V, OrderedCodec<K>>(
            self.db,
            self.cf_name.as_deref(),
            Some(lower),
            upper,
            false,
        )
    }
}

// --- Shared implementation helpers ---

/// Inclusive lower bound and exclusive upper bound byte strings for a key range.
type ByteBounds = (Option<Vec<u8>>, Option<Vec<u8>>);

fn cf_handle<'a>(db: &'a DB, cf_name: Option<&str>) -> Result<Option<&'a ColumnFamily>> {
    match cf_name {
        Some(name) => match db.cf_handle(name) {
            Some(cf) => Ok(Some(cf)),
            None => Err(Error::ColumnFamilyNotFound(name.to_string())),
        },
        None => Ok(None),
    }
}

fn encode_ordered<K: OrderedKey>(key: &K) -> Result<Vec<u8>> {
    <OrderedCodec<K> as KeyCodec<K>>::encode(key)
}

fn get_impl<K, V, KC>(db: &DB, cf_name: Option<&str>, key: &K) -> Result<Option<V>>
where
    KC: KeyCodec<K>,
    V: Serialize + DeserializeOwned,
{
    let key_bytes = KC::encode(key)?;
    let result = match cf_handle(db, cf_name)? {
        Some(cf) => db.get_cf(cf, key_bytes),
        None => db.get(key_bytes),
    }
    .map_err(Error::from)?;

    match result {
        Some(value_bytes) => Ok(Some(<BincodeCodec<V> as ValueCodec<V>>::decode(
            &value_bytes,
        )?)),
        None => Ok(None),
    }
}

fn put_impl<K, V, KC>(db: &DB, cf_name: Option<&str>, key: &K, value: &V) -> Result<()>
where
    KC: KeyCodec<K>,
    V: Serialize + DeserializeOwned,
{
    let key_bytes = KC::encode(key)?;
    let value_bytes = <BincodeCodec<V> as ValueCodec<V>>::encode(value)?;

    match cf_handle(db, cf_name)? {
        Some(cf) => db.put_cf(cf, key_bytes, value_bytes),
        None => db.put(key_bytes, value_bytes),
    }
    .map_err(Error::from)
}

fn delete_impl<K, KC>(db: &DB, cf_name: Option<&str>, key: &K) -> Result<()>
where
    KC: KeyCodec<K>,
{
    let key_bytes = KC::encode(key)?;

    match cf_handle(db, cf_name)? {
        Some(cf) => db.delete_cf(cf, key_bytes),
        None => db.delete(key_bytes),
    }
    .map_err(Error::from)
}

fn contains_impl<K, KC>(db: &DB, cf_name: Option<&str>, key: &K) -> Result<bool>
where
    KC: KeyCodec<K>,
{
    let key_bytes = KC::encode(key)?;
    let found = match cf_handle(db, cf_name)? {
        Some(cf) => db.get_cf(cf, key_bytes),
        None => db.get(key_bytes),
    }
    .map_err(Error::from)?;
    Ok(found.is_some())
}

fn raw_iter<'a>(db: &'a DB, cf_name: Option<&str>) -> Result<rocksdb::DBIterator<'a>> {
    Ok(match cf_handle(db, cf_name)? {
        Some(cf) => db.iterator_cf(cf, IteratorMode::Start),
        None => db.iterator(IteratorMode::Start),
    })
}

fn is_empty_impl(db: &DB, cf_name: Option<&str>) -> Result<bool> {
    match raw_iter(db, cf_name)?.next() {
        None => Ok(true),
        Some(Ok(_)) => Ok(false),
        Some(Err(e)) => Err(Error::from(e)),
    }
}

fn count_impl(db: &DB, cf_name: Option<&str>) -> Result<usize> {
    let mut count = 0;
    for item in raw_iter(db, cf_name)? {
        item.map_err(Error::from)?;
        count += 1;
    }
    Ok(count)
}

fn len_estimate_impl(db: &DB, cf_name: Option<&str>) -> usize {
    let estimate = match cf_name.and_then(|name| db.cf_handle(name)) {
        Some(cf) => db.property_int_value_cf(cf, ESTIMATE_NUM_KEYS),
        None => db.property_int_value(ESTIMATE_NUM_KEYS),
    };
    estimate.ok().flatten().unwrap_or(0) as usize
}

/// Convert a [`RangeBounds`] to `(inclusive_lower, exclusive_upper)` byte bounds suitable for
/// RocksDB `ReadOptions`. Relies on the ordered key encoding being prefix-free: appending `0x00`
/// to a key's encoding yields a byte string strictly between it and the next possible key, so an
/// exclusive lower / inclusive upper both map to native bounds without a stop predicate.
fn range_to_bounds<K, R>(range: &R) -> Result<ByteBounds>
where
    K: OrderedKey,
    R: RangeBounds<K>,
{
    let lower = match range.start_bound() {
        Bound::Unbounded => None,
        Bound::Included(k) => Some(encode_ordered(k)?),
        Bound::Excluded(k) => Some(successor(encode_ordered(k)?)),
    };
    let upper = match range.end_bound() {
        Bound::Unbounded => None,
        Bound::Excluded(k) => Some(encode_ordered(k)?),
        Bound::Included(k) => Some(successor(encode_ordered(k)?)),
    };
    Ok((lower, upper))
}

/// `(lower, upper)` byte bounds matching exactly the keys whose encoding starts with `prefix`.
fn prefix_to_bounds(prefix: Vec<u8>) -> (Vec<u8>, Option<Vec<u8>>) {
    let upper = byte_successor(&prefix);
    (prefix, upper)
}

/// `bytes` with a trailing `0x00` appended — the smallest byte string strictly greater than
/// `bytes` that no (prefix-free) key encoding can equal.
fn successor(mut bytes: Vec<u8>) -> Vec<u8> {
    bytes.push(0x00);
    bytes
}

/// The smallest byte string greater than every string starting with `prefix`, or `None` if no
/// such bound exists (empty prefix, or all trailing `0xFF`).
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

fn make_iter<'a, K, V, KC>(
    db: &'a DB,
    cf_name: Option<&str>,
    lower: Option<Vec<u8>>,
    upper: Option<Vec<u8>>,
    reverse: bool,
) -> Result<RocksMapIterator<'a, K, V, KC>>
where
    KC: KeyCodec<K>,
    V: Serialize + DeserializeOwned,
{
    let mut readopts = ReadOptions::default();
    if let Some(lb) = lower {
        readopts.set_iterate_lower_bound(lb);
    }
    if let Some(ub) = upper {
        readopts.set_iterate_upper_bound(ub);
    }
    let mode = if reverse {
        IteratorMode::End
    } else {
        IteratorMode::Start
    };

    let inner = match cf_handle(db, cf_name)? {
        Some(cf) => db.iterator_cf_opt(cf, readopts, mode),
        None => db.iterator_opt(mode, readopts),
    };

    Ok(RocksMapIterator {
        inner,
        marker: PhantomData,
    })
}

/// Iterator over RocksMap key-value pairs.
///
/// The matching key range is bounded by RocksDB itself (via `ReadOptions`), so this iterator
/// only decodes; it does not filter.
pub struct RocksMapIterator<'a, K, V, KC = OrderedCodec<K>>
where
    KC: KeyCodec<K>,
    V: Serialize + DeserializeOwned,
{
    inner: rocksdb::DBIterator<'a>,
    marker: PhantomData<(K, V, KC)>,
}

impl<'a, K, V, KC> Iterator for RocksMapIterator<'a, K, V, KC>
where
    KC: KeyCodec<K>,
    V: Serialize + DeserializeOwned,
{
    type Item = Result<(K, V)>;

    fn next(&mut self) -> Option<Self::Item> {
        let item = self.inner.next()?;
        Some(
            item.map_err(Error::from)
                .and_then(|(key_bytes, value_bytes)| {
                    let key = KC::decode(&key_bytes)?;
                    let value = <BincodeCodec<V> as ValueCodec<V>>::decode(&value_bytes)?;
                    Ok((key, value))
                }),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::BincodeCodec;
    use serde::{Deserialize, Serialize};
    use tempfile::TempDir;

    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
    struct TestUser {
        id: u64,
        name: String,
        active: bool,
    }

    fn keys_of<V>(iter: RocksMapIterator<'_, u64, V, OrderedCodec<u64>>) -> Vec<u64>
    where
        V: Serialize + DeserializeOwned,
    {
        iter.map(|r| r.unwrap().0).collect()
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

        {
            let users_cf = db.column_family("users").unwrap();
            users_cf.put(1, &user).unwrap();
            let user_from_cf = users_cf.get(&1).unwrap().unwrap();
            assert_eq!(user_from_cf, user);
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

        let count = db.iter().unwrap().count();
        assert_eq!(count, 5);
    }

    #[test]
    fn test_membership_and_size() {
        let temp_dir = TempDir::new().unwrap();
        let db = RocksMap::<u64, u64>::open(temp_dir.path()).unwrap();

        assert!(db.is_empty().unwrap());
        assert_eq!(db.count().unwrap(), 0);
        assert!(!db.contains(&1).unwrap());

        for k in 1..=10u64 {
            db.put(k, &k).unwrap();
        }

        assert!(!db.is_empty().unwrap());
        assert_eq!(db.count().unwrap(), 10);
        assert!(db.contains(&5).unwrap());
        assert!(!db.contains(&99).unwrap());

        db.delete(&5).unwrap();
        assert!(!db.contains(&5).unwrap());
        assert_eq!(db.count().unwrap(), 9);
    }

    #[test]
    fn test_range_bound_kinds() {
        let temp_dir = TempDir::new().unwrap();
        let db = RocksMap::<u64, u64>::open(temp_dir.path()).unwrap();
        for k in [1u64, 2, 10, 256, 1000] {
            db.put(k, &k).unwrap();
        }

        assert_eq!(keys_of(db.range(10..=256).unwrap()), vec![10, 256]);
        assert_eq!(keys_of(db.range(10..256).unwrap()), vec![10]);
        assert_eq!(keys_of(db.range(10..).unwrap()), vec![10, 256, 1000]);
        assert_eq!(keys_of(db.range(..256).unwrap()), vec![1, 2, 10]);
        assert_eq!(keys_of(db.range(..).unwrap()), vec![1, 2, 10, 256, 1000]);
        assert_eq!(keys_of(db.range_rev(2..=256).unwrap()), vec![256, 10, 2]);
    }

    #[test]
    fn test_range_negative_keys() {
        let temp_dir = TempDir::new().unwrap();
        let db = RocksMap::<i64, i64>::open(temp_dir.path()).unwrap();
        for k in [-100i64, -1, 0, 1, 100] {
            db.put(k, &k).unwrap();
        }

        let in_range: Vec<i64> = db.range(-50..=50).unwrap().map(|r| r.unwrap().0).collect();
        assert_eq!(in_range, vec![-1, 0, 1]);
    }

    #[test]
    fn test_scan_prefix_strings() {
        let temp_dir = TempDir::new().unwrap();
        let db = RocksMap::<String, String>::open(temp_dir.path()).unwrap();

        for (k, v) in [("user:1", "a"), ("user:2", "b"), ("post:1", "c")] {
            db.put(k.to_string(), &v.to_string()).unwrap();
        }

        let users: Vec<String> = db
            .scan_prefix("user:")
            .unwrap()
            .map(|r| r.unwrap().0)
            .collect();
        assert_eq!(users, vec!["user:1", "user:2"]);
    }

    #[test]
    fn test_scan_prefix_fields_composite() {
        let temp_dir = TempDir::new().unwrap();
        let db = RocksMap::<(u64, u64), u64>::open(temp_dir.path()).unwrap();
        for user in [1u64, 2] {
            for ts in [10u64, 20] {
                db.put((user, ts), &(user * 100 + ts)).unwrap();
            }
        }

        let user1: Vec<(u64, u64)> = db
            .scan_prefix_fields(&(1u64,))
            .unwrap()
            .map(|r| r.unwrap().0)
            .collect();
        assert_eq!(user1, vec![(1, 10), (1, 20)]);
    }

    #[test]
    fn test_bincode_key_codec_opt_out() {
        // A non-ordered key codec: point ops work; range/prefix are not available (compile-fail
        // is covered by a doctest on `RocksMap`).
        let temp_dir = TempDir::new().unwrap();
        let db = RocksMap::<u64, String, BincodeCodec<u64>>::open(temp_dir.path()).unwrap();
        db.put(1, &"a".to_string()).unwrap();
        db.put(2, &"b".to_string()).unwrap();
        assert_eq!(db.get(&1).unwrap(), Some("a".to_string()));
        assert!(db.contains(&2).unwrap());
        assert_eq!(db.iter().unwrap().count(), 2);
    }

    #[test]
    fn test_codec_mismatch_on_reopen() {
        let temp_dir = TempDir::new().unwrap();
        {
            let db = RocksMap::<u64, String>::open(temp_dir.path()).unwrap(); // OrderedCodec
            db.put(1, &"a".to_string()).unwrap();
        }
        // Reopen with a different key codec -> FormatMismatch.
        let reopened = RocksMap::<u64, String, BincodeCodec<u64>>::open(temp_dir.path());
        assert!(matches!(reopened, Err(Error::FormatMismatch(_))));
    }
}
