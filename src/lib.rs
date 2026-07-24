//! RocksMap - A high-level typed abstraction over RocksDB in Rust
//!
//! `rocksmap` provides a type-safe, ergonomic interface to RocksDB with
//! map-like API and serialization/deserialization support.
//!
//! rocksmap's own crate adds no `unsafe` code (the underlying `rocksdb` bindings are FFI);
//! this is enforced by `#![forbid(unsafe_code)]`.

#![forbid(unsafe_code)]

mod batch;
mod clock;
mod codec;
mod error;
mod index;
mod inspect;
mod meta;
mod ordered;
mod rocks_map;
mod ttl;

pub use crate::batch::RocksMapBatch;
pub use crate::clock::{Clock, ManualClock, SystemClock};
pub use crate::codec::{BincodeCodec, KeyCodec, ValueCodec};
pub use crate::error::{Error, Result};
pub use crate::index::{Index, IndexedRocksMap, IndexedRocksMapBuilder};
pub use crate::inspect::{inspect, DbInfo};
pub use crate::meta::MapKind;
pub use crate::ordered::{
    OrderedCodec, OrderedF32, OrderedF64, OrderedKey, OrderedKeyCodec, PrefixKey,
};
pub use crate::rocks_map::{RocksMap, RocksMapIterator};
pub use crate::ttl::{strip_ttl_envelope, TtlIterator, TtlRocksMap};

/// Re-export important RocksDB types and options for configuration
pub mod rocks {
    pub use rocksdb::{Options, WriteBatch};
}
