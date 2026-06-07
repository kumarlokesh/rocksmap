//! Database metadata stored in a dedicated `__rocksmap_meta` column family.
//!
//! Records the format version and the map "kind" (plain vs TTL) so that reopening a database
//! the wrong way (e.g. a TTL store as a plain map) fails loudly instead of mis-decoding values.
//! Kept out of the user keyspace in its own column family so it never appears in iteration.

use crate::error::{Error, Result};
use rocksdb::{Options, DB};
use std::collections::BTreeSet;
use std::path::Path;

/// Name of the reserved metadata column family.
pub const META_CF: &str = "__rocksmap_meta";

const SCHEMA_KEY: &[u8] = b"schema";
const FORMAT_VERSION: u16 = 1;

/// How a database's values are laid out on disk.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum MapKind {
    /// Plain values (bincode payload).
    Plain,
    /// TTL envelope values (expiry header + payload).
    Ttl,
}

impl MapKind {
    fn tag(self) -> u8 {
        match self {
            MapKind::Plain => 0,
            MapKind::Ttl => 1,
        }
    }

    fn label(self) -> &'static str {
        match self {
            MapKind::Plain => "plain",
            MapKind::Ttl => "ttl",
        }
    }
}

fn label_of(tag: u8) -> &'static str {
    match tag {
        0 => "plain",
        1 => "ttl",
        _ => "unknown",
    }
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
pub fn verify_or_write_kind(db: &DB, kind: MapKind) -> Result<()> {
    let cf = db
        .cf_handle(META_CF)
        .ok_or_else(|| Error::Other(format!("missing `{META_CF}` column family")))?;

    match db.get_cf(cf, SCHEMA_KEY).map_err(Error::from)? {
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
            db.put_cf(cf, SCHEMA_KEY, record).map_err(Error::from)
        }
    }
}
