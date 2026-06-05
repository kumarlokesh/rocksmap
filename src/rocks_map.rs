use crate::{
    codec::{BincodeCodec, KeyCodec, ValueCodec},
    error::{Error, Result},
    ordered::{OrderedCodec, OrderedKey, PrefixKey},
};
use rocksdb::{ColumnFamily, ColumnFamilyDescriptor, IteratorMode, Options, ReadOptions, DB};
use serde::{de::DeserializeOwned, Serialize};
use std::{
    marker::PhantomData,
    ops::{Bound, RangeBounds},
    path::Path,
};

/// The main key-value store abstraction over RocksDB
pub struct RocksMap<K, V>
where
    K: Serialize + DeserializeOwned + Clone + OrderedKey,
    V: Serialize + DeserializeOwned + Clone,
{
    db: DB,
    cf_name: Option<String>,
    _marker: PhantomData<(K, V)>,
}

impl<K, V> RocksMap<K, V>
where
    K: Serialize + DeserializeOwned + Clone + OrderedKey,
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
    pub fn column_family(&mut self, name: &str) -> Result<RocksMapRef<'_, K, V>> {
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

    /// Retrieve a value by key
    pub fn get(&self, key: &K) -> Result<Option<V>> {
        get_impl(&self.db, self.cf_name.as_deref(), key)
    }

    /// Store a value with the given key
    pub fn put(&self, key: K, value: &V) -> Result<()> {
        put_impl(&self.db, self.cf_name.as_deref(), &key, value)
    }

    /// Delete a key-value pair
    pub fn delete(&self, key: &K) -> Result<()> {
        delete_impl(&self.db, self.cf_name.as_deref(), key)
    }

    /// Create a batch operation instance for this database
    pub fn batch(&self) -> crate::batch::RocksMapBatch<'_, K, V> {
        crate::batch::RocksMapBatch::new(&self.db, self.cf_name.clone())
    }

    /// Iterator over all key-value pairs, in ascending key order.
    pub fn iter(&self) -> Result<RocksMapIterator<'_, K, V>> {
        make_iter(&self.db, self.cf_name.as_deref(), None, None, false)
    }

    /// Iterate the key-value pairs whose keys fall in `range`, in ascending key order.
    ///
    /// Accepts any [`RangeBounds`], e.g. `10..=20`, `10..20`, `10..`, `..20`, `..`. Order
    /// follows the key type's logical order.
    pub fn range<R: RangeBounds<K>>(&self, range: R) -> Result<RocksMapIterator<'_, K, V>> {
        let (lower, upper) = range_to_bounds(&range)?;
        make_iter(&self.db, self.cf_name.as_deref(), lower, upper, false)
    }

    /// Like [`range`](Self::range) but yields pairs in descending key order.
    pub fn range_rev<R: RangeBounds<K>>(&self, range: R) -> Result<RocksMapIterator<'_, K, V>> {
        let (lower, upper) = range_to_bounds(&range)?;
        make_iter(&self.db, self.cf_name.as_deref(), lower, upper, true)
    }

    /// Iterate all pairs whose (byte-string) key begins with `prefix`, in ascending order.
    ///
    /// Available for `String` and `Vec<u8>` keys.
    pub fn scan_prefix(
        &self,
        prefix: &<K as PrefixKey>::Prefix,
    ) -> Result<RocksMapIterator<'_, K, V>>
    where
        K: PrefixKey,
    {
        let (lower, upper) = prefix_to_bounds(<K as PrefixKey>::encode_prefix(prefix));
        make_iter(&self.db, self.cf_name.as_deref(), Some(lower), upper, false)
    }

    /// Iterate all pairs whose composite key begins with the given leading fields.
    ///
    /// `prefix` is the leading field(s) as their own ordered value, e.g. `(user_id,)` for a
    /// `(u64, u64)` key. Its encoding must be a true leading-field prefix of `K`.
    pub fn scan_prefix_fields<P: OrderedKey>(
        &self,
        prefix: &P,
    ) -> Result<RocksMapIterator<'_, K, V>> {
        let (lower, upper) = prefix_to_bounds(encode_key(prefix)?);
        make_iter(&self.db, self.cf_name.as_deref(), Some(lower), upper, false)
    }
}

/// A reference to a RocksMap that holds a reference to the database rather than owning it.
/// This allows us to create multiple views into the same database with different column families.
pub struct RocksMapRef<'a, K, V>
where
    K: Serialize + DeserializeOwned + Clone + OrderedKey,
    V: Serialize + DeserializeOwned + Clone,
{
    db: &'a DB,
    cf_name: Option<String>,
    marker: PhantomData<(K, V)>,
}

impl<'a, K, V> RocksMapRef<'a, K, V>
where
    K: Serialize + DeserializeOwned + Clone + OrderedKey,
    V: Serialize + DeserializeOwned + Clone,
{
    /// Returns a reference to the underlying database
    pub fn db(&self) -> &DB {
        self.db
    }

    /// Retrieve a value by key
    pub fn get(&self, key: &K) -> Result<Option<V>> {
        get_impl(self.db, self.cf_name.as_deref(), key)
    }

    /// Store a value with the given key
    pub fn put(&self, key: &K, value: &V) -> Result<()> {
        put_impl(self.db, self.cf_name.as_deref(), key, value)
    }

    /// Delete a key-value pair
    pub fn delete(&self, key: &K) -> Result<()> {
        delete_impl(self.db, self.cf_name.as_deref(), key)
    }

    /// Returns a batch operation builder that can be used to perform multiple
    /// operations in a single atomic batch
    pub fn batch(&self) -> crate::batch::RocksMapBatch<'_, K, V> {
        crate::batch::RocksMapBatch::new(self.db, self.cf_name.clone())
    }

    /// Iterator over all key-value pairs, in ascending key order.
    pub fn iter(&self) -> Result<RocksMapIterator<'_, K, V>> {
        make_iter(self.db, self.cf_name.as_deref(), None, None, false)
    }

    /// Iterate the key-value pairs whose keys fall in `range`, in ascending key order.
    pub fn range<R: RangeBounds<K>>(&self, range: R) -> Result<RocksMapIterator<'_, K, V>> {
        let (lower, upper) = range_to_bounds(&range)?;
        make_iter(self.db, self.cf_name.as_deref(), lower, upper, false)
    }

    /// Like [`range`](Self::range) but yields pairs in descending key order.
    pub fn range_rev<R: RangeBounds<K>>(&self, range: R) -> Result<RocksMapIterator<'_, K, V>> {
        let (lower, upper) = range_to_bounds(&range)?;
        make_iter(self.db, self.cf_name.as_deref(), lower, upper, true)
    }

    /// Iterate all pairs whose (byte-string) key begins with `prefix`, in ascending order.
    pub fn scan_prefix(
        &self,
        prefix: &<K as PrefixKey>::Prefix,
    ) -> Result<RocksMapIterator<'_, K, V>>
    where
        K: PrefixKey,
    {
        let (lower, upper) = prefix_to_bounds(<K as PrefixKey>::encode_prefix(prefix));
        make_iter(self.db, self.cf_name.as_deref(), Some(lower), upper, false)
    }

    /// Iterate all pairs whose composite key begins with the given leading fields.
    pub fn scan_prefix_fields<P: OrderedKey>(
        &self,
        prefix: &P,
    ) -> Result<RocksMapIterator<'_, K, V>> {
        let (lower, upper) = prefix_to_bounds(encode_key(prefix)?);
        make_iter(self.db, self.cf_name.as_deref(), Some(lower), upper, false)
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

fn encode_key<K: OrderedKey>(key: &K) -> Result<Vec<u8>> {
    <OrderedCodec<K> as KeyCodec<K>>::encode(key)
}

fn get_impl<K, V>(db: &DB, cf_name: Option<&str>, key: &K) -> Result<Option<V>>
where
    K: OrderedKey,
    V: Serialize + DeserializeOwned,
{
    let key_bytes = encode_key(key)?;
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

fn put_impl<K, V>(db: &DB, cf_name: Option<&str>, key: &K, value: &V) -> Result<()>
where
    K: OrderedKey,
    V: Serialize + DeserializeOwned,
{
    let key_bytes = encode_key(key)?;
    let value_bytes = <BincodeCodec<V> as ValueCodec<V>>::encode(value)?;

    match cf_handle(db, cf_name)? {
        Some(cf) => db.put_cf(cf, key_bytes, value_bytes),
        None => db.put(key_bytes, value_bytes),
    }
    .map_err(Error::from)
}

fn delete_impl<K>(db: &DB, cf_name: Option<&str>, key: &K) -> Result<()>
where
    K: OrderedKey,
{
    let key_bytes = encode_key(key)?;

    match cf_handle(db, cf_name)? {
        Some(cf) => db.delete_cf(cf, key_bytes),
        None => db.delete(key_bytes),
    }
    .map_err(Error::from)
}

/// Convert a [`RangeBounds`] to `(inclusive_lower, exclusive_upper)` byte bounds suitable for
/// RocksDB `ReadOptions`. Relies on the key encoding being prefix-free: appending `0x00` to a
/// key's encoding yields a byte string strictly between it and the next possible key, so an
/// exclusive lower / inclusive upper both map to native bounds without a stop predicate.
fn range_to_bounds<K, R>(range: &R) -> Result<ByteBounds>
where
    K: OrderedKey,
    R: RangeBounds<K>,
{
    let lower = match range.start_bound() {
        Bound::Unbounded => None,
        Bound::Included(k) => Some(encode_key(k)?),
        Bound::Excluded(k) => Some(successor(encode_key(k)?)),
    };
    let upper = match range.end_bound() {
        Bound::Unbounded => None,
        Bound::Excluded(k) => Some(encode_key(k)?),
        Bound::Included(k) => Some(successor(encode_key(k)?)),
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

fn make_iter<'a, K, V>(
    db: &'a DB,
    cf_name: Option<&str>,
    lower: Option<Vec<u8>>,
    upper: Option<Vec<u8>>,
    reverse: bool,
) -> Result<RocksMapIterator<'a, K, V>>
where
    K: Serialize + DeserializeOwned + OrderedKey,
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
pub struct RocksMapIterator<'a, K, V>
where
    K: Serialize + DeserializeOwned + OrderedKey,
    V: Serialize + DeserializeOwned,
{
    inner: rocksdb::DBIterator<'a>,
    marker: PhantomData<(K, V)>,
}

impl<'a, K, V> Iterator for RocksMapIterator<'a, K, V>
where
    K: Serialize + DeserializeOwned + OrderedKey,
    V: Serialize + DeserializeOwned,
{
    type Item = Result<(K, V)>;

    fn next(&mut self) -> Option<Self::Item> {
        let item = self.inner.next()?;
        Some(
            item.map_err(Error::from)
                .and_then(|(key_bytes, value_bytes)| {
                    let key = <OrderedCodec<K> as KeyCodec<K>>::decode(&key_bytes)?;
                    let value = <BincodeCodec<V> as ValueCodec<V>>::decode(&value_bytes)?;
                    Ok((key, value))
                }),
        )
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

    fn keys_of<V>(iter: RocksMapIterator<'_, u64, V>) -> Vec<u64>
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
        for item in db.iter().unwrap() {
            let (key, value) = item.unwrap();
            assert_eq!(key, value.id);
            count += 1;
        }

        assert_eq!(count, 5);
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
    }

    #[test]
    fn test_range_empty_and_single() {
        let temp_dir = TempDir::new().unwrap();
        let db = RocksMap::<u64, u64>::open(temp_dir.path()).unwrap();
        for k in 1..=5u64 {
            db.put(k, &k).unwrap();
        }

        assert_eq!(keys_of(db.range(3..3).unwrap()), Vec::<u64>::new());
        assert_eq!(keys_of(db.range(3..=3).unwrap()), vec![3]);
    }

    #[test]
    fn test_range_reverse() {
        let temp_dir = TempDir::new().unwrap();
        let db = RocksMap::<u64, u64>::open(temp_dir.path()).unwrap();
        for k in [1u64, 2, 10, 256, 1000] {
            db.put(k, &k).unwrap();
        }

        assert_eq!(keys_of(db.range_rev(2..=256).unwrap()), vec![256, 10, 2]);
        assert_eq!(
            keys_of(db.range_rev(..).unwrap()),
            vec![1000, 256, 10, 2, 1]
        );
    }

    #[test]
    fn test_range_across_byte_boundary() {
        let temp_dir = TempDir::new().unwrap();
        let db = RocksMap::<u64, u64>::open(temp_dir.path()).unwrap();
        for k in [1u64, 2, 10, 256, 300, 1000] {
            db.put(k, &k).unwrap();
        }

        assert_eq!(keys_of(db.iter().unwrap()), vec![1, 2, 10, 256, 300, 1000]);
        assert_eq!(keys_of(db.range(10..=300).unwrap()), vec![10, 256, 300]);
    }

    #[test]
    fn test_range_negative_keys() {
        let temp_dir = TempDir::new().unwrap();
        let db = RocksMap::<i64, i64>::open(temp_dir.path()).unwrap();
        for k in [-100i64, -1, 0, 1, 100] {
            db.put(k, &k).unwrap();
        }

        let order: Vec<i64> = db.iter().unwrap().map(|r| r.unwrap().0).collect();
        assert_eq!(order, vec![-100, -1, 0, 1, 100]);

        let in_range: Vec<i64> = db.range(-50..=50).unwrap().map(|r| r.unwrap().0).collect();
        assert_eq!(in_range, vec![-1, 0, 1]);
    }

    #[test]
    fn test_scan_prefix_strings() {
        let temp_dir = TempDir::new().unwrap();
        let db = RocksMap::<String, String>::open(temp_dir.path()).unwrap();

        for (k, v) in [
            ("user:001", "Alice"),
            ("user:002", "Bob"),
            ("user:003", "Charlie"),
            ("post:001", "Hello"),
            ("post:002", "World"),
        ] {
            db.put(k.to_string(), &v.to_string()).unwrap();
        }

        let users: Vec<String> = db
            .scan_prefix("user:")
            .unwrap()
            .map(|r| r.unwrap().0)
            .collect();
        assert_eq!(users, vec!["user:001", "user:002", "user:003"]);

        let posts: Vec<String> = db
            .scan_prefix("post:")
            .unwrap()
            .map(|r| r.unwrap().0)
            .collect();
        assert_eq!(posts, vec!["post:001", "post:002"]);

        // empty prefix matches everything; a non-matching prefix matches nothing.
        assert_eq!(db.scan_prefix("").unwrap().count(), 5);
        assert_eq!(db.scan_prefix("zzz").unwrap().count(), 0);
    }

    #[test]
    fn test_scan_prefix_fields_composite() {
        let temp_dir = TempDir::new().unwrap();
        let db = RocksMap::<(u64, u64), u64>::open(temp_dir.path()).unwrap();

        for user in [1u64, 2] {
            for ts in [10u64, 20, 30] {
                db.put((user, ts), &(user * 100 + ts)).unwrap();
            }
        }

        let user1: Vec<(u64, u64)> = db
            .scan_prefix_fields(&(1u64,))
            .unwrap()
            .map(|r| r.unwrap().0)
            .collect();
        assert_eq!(user1, vec![(1, 10), (1, 20), (1, 30)]);

        let user2: Vec<(u64, u64)> = db
            .scan_prefix_fields(&(2u64,))
            .unwrap()
            .map(|r| r.unwrap().0)
            .collect();
        assert_eq!(user2, vec![(2, 10), (2, 20), (2, 30)]);
    }

    // A key whose `Debug` is deliberately unrelated to its logical/byte order still scans
    // correctly — proving iteration does not depend on `Debug`.
    #[test]
    fn test_no_debug_dependency() {
        #[derive(Clone, PartialEq, Serialize, Deserialize)]
        struct Id(u32);

        impl std::fmt::Debug for Id {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(f, "Id(<redacted>)")
            }
        }

        impl OrderedKey for Id {
            fn encode_into(&self, out: &mut Vec<u8>) {
                self.0.encode_into(out);
            }
            fn decode_from(input: &mut &[u8]) -> Result<Self> {
                Ok(Id(u32::decode_from(input)?))
            }
        }

        let temp_dir = TempDir::new().unwrap();
        let db = RocksMap::<Id, u32>::open(temp_dir.path()).unwrap();
        for k in [1u32, 2, 256, 1000] {
            db.put(Id(k), &k).unwrap();
        }

        let got: Vec<u32> = db
            .range(Id(2)..=Id(256))
            .unwrap()
            .map(|r| r.unwrap().1)
            .collect();
        assert_eq!(got, vec![2, 256]);
    }
}
