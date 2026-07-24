//! Read-only introspection of a rocksmap database's metadata.
//!
//! Lets tooling (e.g. `rocksmap-cli`) discover what a database *is* — plain, TTL, or indexed, and
//! which key codec / indexes it uses — without knowing its typed generics, and without mutating it.

use crate::error::{Error, Result};
use crate::meta::{self, MapKind};
use rocksdb::{ColumnFamilyDescriptor, Options, DB};
use std::path::Path;

/// A read-only summary of a rocksmap database.
#[derive(Debug, Clone)]
pub struct DbInfo {
    /// Plain, TTL, or indexed.
    pub kind: MapKind,
    /// Recorded key-codec id (`1` = ordered, `2` = bincode), if written.
    pub key_codec_id: Option<u8>,
    /// Declared secondary-index names (empty unless `kind` is indexed).
    pub indexes: Vec<String>,
    /// All column families present on disk, including internal ones (`__rocksmap_meta`, `__idx_*`).
    pub column_families: Vec<String>,
}

/// Open the database at `path` **read-only** and summarize its metadata.
///
/// Errors if the path is not a database, or is not a rocksmap-managed database (no metadata).
pub fn inspect<P: AsRef<Path>>(path: P) -> Result<DbInfo> {
    let path = path.as_ref();
    let opts = Options::default(); // create_if_missing defaults to false

    let column_families = meta::existing_cfs(&opts, path);
    let descriptors: Vec<ColumnFamilyDescriptor> = column_families
        .iter()
        .map(|name| ColumnFamilyDescriptor::new(name, Options::default()))
        .collect();

    let db =
        DB::open_cf_descriptors_read_only(&opts, path, descriptors, false).map_err(Error::from)?;

    if db.cf_handle(meta::META_CF).is_none() {
        return Err(Error::FormatMismatch(
            "not a rocksmap-managed database (no metadata column family)".to_string(),
        ));
    }

    let kind = meta::read_kind(&db)?.ok_or_else(|| {
        Error::FormatMismatch("metadata present but no schema record".to_string())
    })?;
    let key_codec_id = meta::read_key_codec(&db)?;
    let indexes = meta::read_indexes(&db)?;

    Ok(DbInfo {
        kind,
        key_codec_id,
        indexes,
        column_families,
    })
}
