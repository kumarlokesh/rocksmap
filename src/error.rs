use std::path::PathBuf;
use thiserror::Error;

/// Errors that can occur in RocksMap operations.
///
/// Marked `#[non_exhaustive]`: match on it with a wildcard arm, as new variants may be added in
/// future releases without a breaking change.
#[derive(Error, Debug)]
#[non_exhaustive]
pub enum Error {
    /// Error from the underlying RocksDB instance
    #[error("RocksDB error: {0}")]
    Rocks(#[from] rocksdb::Error),

    /// Error during serialization of keys or values
    #[error("Serialization error: {0}")]
    Serialization(String),

    /// Error during deserialization of keys or values
    #[error("Deserialization error: {0}")]
    Deserialization(String),

    /// Column family not found
    #[error("Column family not found: {0}")]
    ColumnFamilyNotFound(String),

    /// Path does not exist or is not a directory
    #[error("Invalid database path: {0}")]
    InvalidPath(PathBuf),

    /// The on-disk format does not match how the database is being opened
    /// (e.g. opening a TTL database as a plain map, or an unknown format version).
    #[error("Format mismatch: {0}")]
    FormatMismatch(String),

    /// A write would violate a unique secondary index constraint.
    #[error("Unique constraint violation: {0}")]
    UniqueViolation(String),

    /// Other unexpected errors
    #[error("Unexpected error: {0}")]
    Other(String),
}

/// Shorthand for Result with our error type
pub type Result<T> = std::result::Result<T, Error>;
