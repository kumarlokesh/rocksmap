//! Database metadata stored in a dedicated `__rocksmap_meta` column family.
//!
//! Records the format version, the map "kind" (plain / TTL / indexed), and (for indexed maps)
//! the declared index names and any in-progress rebuild. Reopening a database the wrong way
//! (e.g. a TTL store as a plain map, or with a different index set) fails loudly via
//! [`Error::FormatMismatch`] instead of mis-decoding values. Kept in its own column family so
//! it never appears in user iteration.

use crate::error::{Error, Result};
use rocksdb::{ColumnFamily, Options, TransactionDB, DB};
use std::collections::BTreeSet;
use std::path::Path;

/// Name of the reserved metadata column family.
pub const META_CF: &str = "__rocksmap_meta";

const SCHEMA_KEY: &[u8] = b"schema";
const INDEXES_KEY: &[u8] = b"indexes";
const REBUILD_KEY: &[u8] = b"rebuilding";
const KEY_CODEC_KEY: &[u8] = b"key_codec";
const FORMAT_VERSION: u16 = 1;

/// How a database's values are laid out on disk.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum MapKind {
    /// Plain values (bincode payload).
    Plain,
    /// TTL envelope values (expiry header + payload).
    Ttl,
    /// Indexed dataset (data CF + index CFs, transactional).
    Indexed,
}

impl MapKind {
    fn tag(self) -> u8 {
        match self {
            MapKind::Plain => 0,
            MapKind::Ttl => 1,
            MapKind::Indexed => 2,
        }
    }

    fn label(self) -> &'static str {
        label_of(self.tag())
    }

    /// Human-readable name of this kind (`"plain"` / `"ttl"` / `"indexed"`).
    pub fn as_str(self) -> &'static str {
        self.label()
    }

    fn from_tag(tag: u8) -> Result<Self> {
        match tag {
            0 => Ok(MapKind::Plain),
            1 => Ok(MapKind::Ttl),
            2 => Ok(MapKind::Indexed),
            other => Err(Error::FormatMismatch(format!(
                "unknown map kind tag {other}"
            ))),
        }
    }
}

impl std::fmt::Display for MapKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

fn label_of(tag: u8) -> &'static str {
    match tag {
        0 => "plain",
        1 => "ttl",
        2 => "indexed",
        _ => "unknown",
    }
}

/// Minimal key-value access over the metadata column family, implemented for both the plain
/// [`DB`] and the transactional [`TransactionDB`].
pub trait KvStore {
    fn cf(&self, name: &str) -> Option<&ColumnFamily>;
    fn get_raw(&self, cf: &ColumnFamily, key: &[u8]) -> Result<Option<Vec<u8>>>;
    fn put_raw(&self, cf: &ColumnFamily, key: &[u8], value: &[u8]) -> Result<()>;
    fn delete_raw(&self, cf: &ColumnFamily, key: &[u8]) -> Result<()>;
}

impl KvStore for DB {
    fn cf(&self, name: &str) -> Option<&ColumnFamily> {
        self.cf_handle(name)
    }
    fn get_raw(&self, cf: &ColumnFamily, key: &[u8]) -> Result<Option<Vec<u8>>> {
        self.get_cf(cf, key).map_err(Error::from)
    }
    fn put_raw(&self, cf: &ColumnFamily, key: &[u8], value: &[u8]) -> Result<()> {
        self.put_cf(cf, key, value).map_err(Error::from)
    }
    fn delete_raw(&self, cf: &ColumnFamily, key: &[u8]) -> Result<()> {
        self.delete_cf(cf, key).map_err(Error::from)
    }
}

impl KvStore for TransactionDB {
    fn cf(&self, name: &str) -> Option<&ColumnFamily> {
        self.cf_handle(name)
    }
    fn get_raw(&self, cf: &ColumnFamily, key: &[u8]) -> Result<Option<Vec<u8>>> {
        self.get_cf(cf, key).map_err(Error::from)
    }
    fn put_raw(&self, cf: &ColumnFamily, key: &[u8], value: &[u8]) -> Result<()> {
        self.put_cf(cf, key, value).map_err(Error::from)
    }
    fn delete_raw(&self, cf: &ColumnFamily, key: &[u8]) -> Result<()> {
        self.delete_cf(cf, key).map_err(Error::from)
    }
}

fn meta_cf<S: KvStore>(store: &S) -> Result<&ColumnFamily> {
    store
        .cf(META_CF)
        .ok_or_else(|| Error::Other(format!("missing `{META_CF}` column family")))
}

/// Existing column families for the database at `path`, or `["default"]` if it does not exist
/// yet (a fresh database).
pub fn existing_cfs(opts: &Options, path: &Path) -> Vec<String> {
    DB::list_cf(opts, path).unwrap_or_else(|_| vec!["default".to_string()])
}

/// The full set of column families to open: every existing one, plus `default`, the metadata
/// CF, and any `extra` the caller wants created.
pub fn all_cf_names(opts: &Options, path: &Path, extra: &[&str]) -> Vec<String> {
    let mut names: BTreeSet<String> = existing_cfs(opts, path).into_iter().collect();
    names.insert("default".to_string());
    names.insert(META_CF.to_string());
    for e in extra {
        names.insert((*e).to_string());
    }
    names.into_iter().collect()
}

/// Verify the stored kind matches `kind`, writing it if the database is fresh.
pub fn verify_or_write_kind<S: KvStore>(store: &S, kind: MapKind) -> Result<()> {
    let cf = meta_cf(store)?;
    match store.get_raw(cf, SCHEMA_KEY)? {
        Some(bytes) => {
            if bytes.len() < 3 {
                return Err(Error::FormatMismatch("corrupt metadata record".to_string()));
            }
            let version = u16::from_be_bytes([bytes[0], bytes[1]]);
            if version != FORMAT_VERSION {
                return Err(Error::FormatMismatch(format!(
                    "unsupported on-disk format version {version} (this build supports {FORMAT_VERSION})"
                )));
            }
            let stored_tag = bytes[2];
            if stored_tag != kind.tag() {
                return Err(Error::FormatMismatch(format!(
                    "database was created as a {} map but opened as a {} map",
                    label_of(stored_tag),
                    kind.label()
                )));
            }
            Ok(())
        }
        None => {
            let mut record = Vec::with_capacity(3);
            record.extend_from_slice(&FORMAT_VERSION.to_be_bytes());
            record.push(kind.tag());
            store.put_raw(cf, SCHEMA_KEY, &record)
        }
    }
}

/// Verify the declared index name set matches what the database was created with, or record it.
pub fn verify_or_write_indexes<S: KvStore>(store: &S, sorted_names: &[String]) -> Result<()> {
    let cf = meta_cf(store)?;
    let want = bincode::serialize(sorted_names).map_err(|e| Error::Serialization(e.to_string()))?;
    match store.get_raw(cf, INDEXES_KEY)? {
        Some(have) if have == want => Ok(()),
        Some(have) => {
            let existing: Vec<String> = bincode::deserialize(&have).unwrap_or_default();
            Err(Error::FormatMismatch(format!(
                "database was created with indexes {existing:?} but opened with {sorted_names:?}"
            )))
        }
        None => store.put_raw(cf, INDEXES_KEY, &want),
    }
}

/// Read the recorded map kind (read-only; `None` if the metadata has never been written).
pub fn read_kind<S: KvStore>(store: &S) -> Result<Option<MapKind>> {
    let cf = meta_cf(store)?;
    match store.get_raw(cf, SCHEMA_KEY)? {
        Some(bytes) if bytes.len() >= 3 => Ok(Some(MapKind::from_tag(bytes[2])?)),
        Some(_) => Err(Error::FormatMismatch("corrupt metadata record".to_string())),
        None => Ok(None),
    }
}

/// Read the recorded key-codec id (read-only; `None` if never written).
pub fn read_key_codec<S: KvStore>(store: &S) -> Result<Option<u8>> {
    let cf = meta_cf(store)?;
    Ok(store
        .get_raw(cf, KEY_CODEC_KEY)?
        .and_then(|b| b.first().copied()))
}

/// Read the declared index names (read-only; empty if none).
pub fn read_indexes<S: KvStore>(store: &S) -> Result<Vec<String>> {
    let cf = meta_cf(store)?;
    match store.get_raw(cf, INDEXES_KEY)? {
        Some(bytes) => Ok(bincode::deserialize(&bytes).unwrap_or_default()),
        None => Ok(Vec::new()),
    }
}

/// Verify the stored key-codec id matches `id`, writing it if the database is fresh.
pub fn verify_or_write_key_codec<S: KvStore>(store: &S, id: u8) -> Result<()> {
    let cf = meta_cf(store)?;
    match store.get_raw(cf, KEY_CODEC_KEY)? {
        Some(have) if have.first() == Some(&id) => Ok(()),
        Some(have) => Err(Error::FormatMismatch(format!(
            "database was created with key codec id {:?} but opened with id {id}",
            have.first()
        ))),
        None => store.put_raw(cf, KEY_CODEC_KEY, &[id]),
    }
}

/// Mark that `index_name` is being rebuilt (so a crash mid-rebuild is detectable on reopen).
pub fn set_rebuilding<S: KvStore>(store: &S, index_name: &str) -> Result<()> {
    let cf = meta_cf(store)?;
    store.put_raw(cf, REBUILD_KEY, index_name.as_bytes())
}

/// The index currently flagged as rebuilding, if any.
pub fn get_rebuilding<S: KvStore>(store: &S) -> Result<Option<String>> {
    let cf = meta_cf(store)?;
    Ok(store
        .get_raw(cf, REBUILD_KEY)?
        .map(|b| String::from_utf8_lossy(&b).into_owned()))
}

/// Clear the rebuild flag.
pub fn clear_rebuilding<S: KvStore>(store: &S) -> Result<()> {
    let cf = meta_cf(store)?;
    store.delete_raw(cf, REBUILD_KEY)
}
