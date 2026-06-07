//! Per-key TTL support via a value envelope plus a RocksDB compaction filter.
//!
//! [`TtlRocksMap`] is a distinct type from [`RocksMap`](crate::RocksMap) (the typestate
//! pattern): `put_with_ttl` only exists here, so a non-TTL map cannot be given expiring keys.
//! Values are wrapped in an envelope carrying an optional expiry deadline:
//!
//! ```text
//! tag = 0x00            -> no expiry,  followed by the payload
//! tag = 0x01 + u64 (BE) -> expires at that UNIX-millis deadline, then the payload
//! ```
//!
//! Expiry is enforced two ways: the **read path** (`get`/`iter`) treats an expired entry as
//! absent, so expiry is logically immediate; and a **compaction filter** physically drops
//! expired entries during background compaction. Deadlines use a wall-clock [`Clock`].

use crate::clock::{Clock, SystemClock};
use crate::codec::{BincodeCodec, KeyCodec, ValueCodec};
use crate::error::{Error, Result};
use crate::meta;
use crate::ordered::{OrderedCodec, OrderedKey};
use rocksdb::{ColumnFamilyDescriptor, CompactionDecision, IteratorMode, Options, DB};
use serde::{de::DeserializeOwned, Serialize};
use std::{marker::PhantomData, path::Path, sync::Arc, time::Duration};

const TAG_NO_TTL: u8 = 0;
const TAG_TTL: u8 = 1;

/// Wrap a payload in the TTL envelope.
fn encode_envelope(expire_at: Option<u64>, payload: &[u8]) -> Vec<u8> {
    match expire_at {
        None => {
            let mut out = Vec::with_capacity(1 + payload.len());
            out.push(TAG_NO_TTL);
            out.extend_from_slice(payload);
            out
        }
        Some(deadline) => {
            let mut out = Vec::with_capacity(1 + 8 + payload.len());
            out.push(TAG_TTL);
            out.extend_from_slice(&deadline.to_be_bytes());
            out.extend_from_slice(payload);
            out
        }
    }
}

/// Split an envelope into `(expire_at, payload)`.
fn decode_envelope(bytes: &[u8]) -> Result<(Option<u64>, &[u8])> {
    match bytes.split_first() {
        Some((&TAG_NO_TTL, payload)) => Ok((None, payload)),
        Some((&TAG_TTL, rest)) => {
            if rest.len() < 8 {
                return Err(Error::Deserialization("truncated TTL envelope".to_string()));
            }
            let (deadline, payload) = rest.split_at(8);
            let millis = u64::from_be_bytes(deadline.try_into().unwrap());
            Ok((Some(millis), payload))
        }
        _ => Err(Error::Deserialization(
            "invalid TTL envelope tag".to_string(),
        )),
    }
}

fn is_expired(expire_at: Option<u64>, now: u64) -> bool {
    matches!(expire_at, Some(deadline) if deadline <= now)
}

/// A typed map whose entries can carry per-key time-to-live.
///
/// Stored on the default column family. Distinct from [`RocksMap`](crate::RocksMap): opening
/// the same database the other way fails via the persisted format tag.
pub struct TtlRocksMap<K, V>
where
    K: Serialize + DeserializeOwned + Clone + OrderedKey,
    V: Serialize + DeserializeOwned + Clone,
{
    db: DB,
    clock: Arc<dyn Clock>,
    default_ttl: Option<Duration>,
    _marker: PhantomData<(K, V)>,
}

impl<K, V> TtlRocksMap<K, V>
where
    K: Serialize + DeserializeOwned + Clone + OrderedKey,
    V: Serialize + DeserializeOwned + Clone,
{
    /// Open a TTL map at `path` using the system clock and no default TTL.
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        Self::open_internal(path, Arc::new(SystemClock), None)
    }

    /// Open a TTL map where writes that don't specify a TTL expire after `default_ttl`.
    pub fn open_with_default_ttl<P: AsRef<Path>>(path: P, default_ttl: Duration) -> Result<Self> {
        Self::open_internal(path, Arc::new(SystemClock), Some(default_ttl))
    }

    /// Open a TTL map with an injected clock (for deterministic testing).
    pub fn open_with_clock<P: AsRef<Path>>(path: P, clock: Arc<dyn Clock>) -> Result<Self> {
        Self::open_internal(path, clock, None)
    }

    /// Open a TTL map with both an injected clock and a default TTL.
    pub fn open_with_clock_and_default_ttl<P: AsRef<Path>>(
        path: P,
        clock: Arc<dyn Clock>,
        default_ttl: Duration,
    ) -> Result<Self> {
        Self::open_internal(path, clock, Some(default_ttl))
    }

    fn open_internal<P: AsRef<Path>>(
        path: P,
        clock: Arc<dyn Clock>,
        default_ttl: Option<Duration>,
    ) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        if !path.exists() {
            std::fs::create_dir_all(&path).map_err(|_| Error::InvalidPath(path.clone()))?;
        }

        // The default column family carries the TTL compaction filter; the filter drops
        // expired envelopes during compaction and keeps anything it cannot parse as one.
        let mut data_opts = Options::default();
        data_opts.create_if_missing(true);
        data_opts.create_missing_column_families(true);
        let filter_clock = clock.clone();
        data_opts.set_compaction_filter("rocksmap.ttl", move |_level, _key, value| {
            match decode_envelope(value) {
                Ok((expire_at, _)) if is_expired(expire_at, filter_clock.now_unix_millis()) => {
                    CompactionDecision::Remove
                }
                _ => CompactionDecision::Keep,
            }
        });

        let mut db_opts = Options::default();
        db_opts.create_if_missing(true);
        db_opts.create_missing_column_families(true);

        // The metadata CF (and any others) must not carry the envelope filter; only the
        // default CF, which holds the TTL data, does. `data_opts` is moved in exactly once.
        let names = meta::all_cf_names(&db_opts, &path, &[]);
        let mut data_opts = Some(data_opts);
        let descriptors: Vec<ColumnFamilyDescriptor> = names
            .iter()
            .map(|name| {
                let opts = if name == "default" {
                    data_opts.take().expect("exactly one default column family")
                } else {
                    Options::default()
                };
                ColumnFamilyDescriptor::new(name, opts)
            })
            .collect();

        let db = DB::open_cf_descriptors(&db_opts, &path, descriptors).map_err(Error::from)?;
        meta::verify_or_write_kind(&db, meta::MapKind::Ttl)?;

        Ok(Self {
            db,
            clock,
            default_ttl,
            _marker: PhantomData,
        })
    }

    fn now(&self) -> u64 {
        self.clock.now_unix_millis()
    }

    fn store(&self, key: &K, value: &V, expire_at: Option<u64>) -> Result<()> {
        let key_bytes = <OrderedCodec<K> as KeyCodec<K>>::encode(key)?;
        let payload = <BincodeCodec<V> as ValueCodec<V>>::encode(value)?;
        let envelope = encode_envelope(expire_at, &payload);
        self.db.put(key_bytes, envelope).map_err(Error::from)
    }

    /// Store a value using the map's `default_ttl` (no expiry if none was configured).
    pub fn put(&self, key: K, value: &V) -> Result<()> {
        let expire_at = self
            .default_ttl
            .map(|ttl| self.now().saturating_add(ttl.as_millis() as u64));
        self.store(&key, value, expire_at)
    }

    /// Store a value that expires `ttl` from now.
    pub fn put_with_ttl(&self, key: K, value: &V, ttl: Duration) -> Result<()> {
        let expire_at = self.now().saturating_add(ttl.as_millis() as u64);
        self.store(&key, value, Some(expire_at))
    }

    /// Store a value that expires at an absolute UNIX-millis deadline.
    pub fn put_with_expiry(&self, key: K, value: &V, expire_at_unix_millis: u64) -> Result<()> {
        self.store(&key, value, Some(expire_at_unix_millis))
    }

    /// Retrieve a value, treating an expired entry as absent.
    pub fn get(&self, key: &K) -> Result<Option<V>> {
        let key_bytes = <OrderedCodec<K> as KeyCodec<K>>::encode(key)?;
        match self.db.get(key_bytes).map_err(Error::from)? {
            None => Ok(None),
            Some(envelope) => {
                let (expire_at, payload) = decode_envelope(&envelope)?;
                if is_expired(expire_at, self.now()) {
                    Ok(None)
                } else {
                    Ok(Some(<BincodeCodec<V> as ValueCodec<V>>::decode(payload)?))
                }
            }
        }
    }

    /// Delete a key-value pair.
    pub fn delete(&self, key: &K) -> Result<()> {
        let key_bytes = <OrderedCodec<K> as KeyCodec<K>>::encode(key)?;
        self.db.delete(key_bytes).map_err(Error::from)
    }

    /// Iterate non-expired key-value pairs in ascending key order. Expiry is evaluated against
    /// the clock at the moment iteration begins.
    pub fn iter(&self) -> TtlIterator<'_, K, V> {
        TtlIterator {
            inner: self.db.iterator(IteratorMode::Start),
            now: self.now(),
            marker: PhantomData,
        }
    }

    /// Trigger a full compaction, which physically removes already-expired entries.
    pub fn compact(&self) {
        self.db.compact_range::<&[u8], &[u8]>(None, None);
    }

    /// Access the underlying RocksDB handle.
    pub fn db(&self) -> &DB {
        &self.db
    }
}

/// Iterator over non-expired entries of a [`TtlRocksMap`].
pub struct TtlIterator<'a, K, V>
where
    K: Serialize + DeserializeOwned + OrderedKey,
    V: Serialize + DeserializeOwned,
{
    inner: rocksdb::DBIterator<'a>,
    now: u64,
    marker: PhantomData<(K, V)>,
}

impl<'a, K, V> Iterator for TtlIterator<'a, K, V>
where
    K: Serialize + DeserializeOwned + OrderedKey,
    V: Serialize + DeserializeOwned,
{
    type Item = Result<(K, V)>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let (key_bytes, value_bytes) = match self.inner.next()? {
                Ok(pair) => pair,
                Err(e) => return Some(Err(Error::from(e))),
            };

            let decoded = (|| {
                let (expire_at, payload) = decode_envelope(&value_bytes)?;
                if is_expired(expire_at, self.now) {
                    return Ok(None);
                }
                let key = <OrderedCodec<K> as KeyCodec<K>>::decode(&key_bytes)?;
                let value = <BincodeCodec<V> as ValueCodec<V>>::decode(payload)?;
                Ok(Some((key, value)))
            })();

            match decoded {
                Ok(Some(pair)) => return Some(Ok(pair)),
                Ok(None) => continue, // expired: skip
                Err(e) => return Some(Err(e)),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::clock::ManualClock;
    use crate::RocksMap;
    use tempfile::TempDir;

    fn ttl_map(clock: ManualClock) -> (TempDir, TtlRocksMap<String, String>) {
        let dir = TempDir::new().unwrap();
        let map =
            TtlRocksMap::<String, String>::open_with_clock(dir.path(), Arc::new(clock)).unwrap();
        (dir, map)
    }

    #[test]
    fn logical_expiry_is_immediate_without_compaction() {
        let clock = ManualClock::new(1_000);
        let (_dir, map) = ttl_map(clock.clone());

        map.put_with_ttl(
            "k".to_string(),
            &"v".to_string(),
            Duration::from_millis(500),
        )
        .unwrap();
        assert_eq!(map.get(&"k".to_string()).unwrap(), Some("v".to_string()));

        clock.advance(500); // now == deadline -> expired
        assert_eq!(map.get(&"k".to_string()).unwrap(), None);
    }

    #[test]
    fn non_expiring_entries_coexist() {
        let clock = ManualClock::new(0);
        let (_dir, map) = ttl_map(clock.clone());

        map.put("permanent".to_string(), &"keep".to_string())
            .unwrap();
        map.put_with_ttl(
            "temp".to_string(),
            &"drop".to_string(),
            Duration::from_millis(10),
        )
        .unwrap();

        clock.advance(1_000);
        assert_eq!(
            map.get(&"permanent".to_string()).unwrap(),
            Some("keep".to_string())
        );
        assert_eq!(map.get(&"temp".to_string()).unwrap(), None);
    }

    #[test]
    fn iter_skips_expired() {
        let clock = ManualClock::new(0);
        let (_dir, map) = ttl_map(clock.clone());

        map.put("a".to_string(), &"1".to_string()).unwrap();
        map.put_with_ttl("b".to_string(), &"2".to_string(), Duration::from_millis(5))
            .unwrap();
        map.put("c".to_string(), &"3".to_string()).unwrap();

        clock.advance(10);
        let keys: Vec<String> = map.iter().map(|r| r.unwrap().0).collect();
        assert_eq!(keys, vec!["a".to_string(), "c".to_string()]);
    }

    #[test]
    fn physical_reclamation_after_compaction() {
        let clock = ManualClock::new(0);
        let (_dir, map) = ttl_map(clock.clone());

        map.put_with_ttl("k".to_string(), &"v".to_string(), Duration::from_millis(5))
            .unwrap();
        clock.advance(10);

        // A raw scan of the default CF bypasses the read-path filter, so it counts entries that
        // are still physically present. Flush to an SST first (the compaction filter runs on
        // SST data, not the memtable).
        map.db().flush().unwrap();
        let physical_before = map.db().iterator(IteratorMode::Start).count();
        assert_eq!(
            physical_before, 1,
            "entry should be present before compaction"
        );

        map.compact();

        let physical_after = map.db().iterator(IteratorMode::Start).count();
        assert_eq!(
            physical_after, 0,
            "compaction filter should have removed it"
        );
    }

    #[test]
    fn boundary_now_is_expired() {
        let clock = ManualClock::new(100);
        let (_dir, map) = ttl_map(clock.clone());

        // expire_at == now -> expired; now+1 -> still alive.
        map.put_with_expiry("at".to_string(), &"x".to_string(), 100)
            .unwrap();
        map.put_with_expiry("after".to_string(), &"y".to_string(), 101)
            .unwrap();

        assert_eq!(map.get(&"at".to_string()).unwrap(), None);
        assert_eq!(
            map.get(&"after".to_string()).unwrap(),
            Some("y".to_string())
        );
    }

    #[test]
    fn default_ttl_applies_to_plain_put() {
        let dir = TempDir::new().unwrap();
        let clock = ManualClock::new(0);
        let map = TtlRocksMap::<String, String>::open_with_clock_and_default_ttl(
            dir.path(),
            Arc::new(clock.clone()),
            Duration::from_millis(100),
        )
        .unwrap();

        map.put("k".to_string(), &"v".to_string()).unwrap();
        assert_eq!(map.get(&"k".to_string()).unwrap(), Some("v".to_string()));
        clock.advance(100);
        assert_eq!(map.get(&"k".to_string()).unwrap(), None);
    }

    #[test]
    fn opening_ttl_db_as_plain_map_fails() {
        let dir = TempDir::new().unwrap();
        {
            let map = TtlRocksMap::<String, String>::open(dir.path()).unwrap();
            map.put("k".to_string(), &"v".to_string()).unwrap();
        }
        let err = RocksMap::<String, String>::open(dir.path());
        assert!(matches!(err, Err(Error::FormatMismatch(_))));
    }

    #[test]
    fn opening_plain_db_as_ttl_map_fails() {
        let dir = TempDir::new().unwrap();
        {
            let map = RocksMap::<String, String>::open(dir.path()).unwrap();
            map.put("k".to_string(), &"v".to_string()).unwrap();
        }
        let err = TtlRocksMap::<String, String>::open(dir.path());
        assert!(matches!(err, Err(Error::FormatMismatch(_))));
    }
}
