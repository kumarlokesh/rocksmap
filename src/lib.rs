//! RocksMap - A high-level typed abstraction over RocksDB in Rust
//!
//! `rocksmap` provides a type-safe, ergonomic interface to RocksDB with
//! map-like API and serialization/deserialization support.

mod batch;
mod clock;
mod codec;
mod error;
mod index;
mod meta;
mod ordered;
mod rocks_map;
mod ttl;

pub use crate::batch::RocksMapBatch;
pub use crate::clock::{Clock, ManualClock, SystemClock};
pub use crate::codec::{BincodeCodec, KeyCodec, ValueCodec};
pub use crate::error::{Error, Result};
pub use crate::index::{Index, IndexedRocksMap, IndexedRocksMapBuilder};
pub use crate::ordered::{
    OrderedCodec, OrderedF32, OrderedF64, OrderedKey, OrderedKeyCodec, PrefixKey,
};
pub use crate::rocks_map::{RocksMap, RocksMapIterator};
pub use crate::ttl::{TtlIterator, TtlRocksMap};

/// Re-export important RocksDB types and options for configuration
pub mod rocks {
    pub use rocksdb::{Options, WriteBatch};
}
