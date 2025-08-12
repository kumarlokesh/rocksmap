use std::path::PathBuf;
use thiserror::Error;

/// Errors that can occur in RocksMap operations
#[derive(Error, Debug)]
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

    /// Other unexpected errors
    #[error("Unexpected error: {0}")]
    Other(String),
}

/// Shorthand for Result with our error type
pub type Result<T> = std::result::Result<T, Error>;
