//! RocksMap - A high-level typed abstraction over RocksDB in Rust
//!
//! `rocksmap` provides a type-safe, ergonomic interface to RocksDB with
//! map-like API and serialization/deserialization support.
//!
//! rocksmap's own crate adds no `unsafe` code (the underlying `rocksdb` bindings are FFI);
//! this is enforced by `#![forbid(unsafe_code)]`.
//!
//! # Durability
//!
//! A write that returns `Ok` is recorded in RocksDB's write-ahead log and **survives a process
//! crash**. It may be lost on an **OS crash or power loss** if the WAL has not yet been fsync'd —
//! the default, matching RocksDB and every embedded-store peer. For power-loss durability, call
//! [`RocksMap::sync_wal`] (or [`TtlRocksMap::sync_wal`]) at a checkpoint; it costs one fsync.
//!
//! **Atomicity holds regardless of durability mode:** a [`RocksMapBatch`] and every
//! [`IndexedRocksMap`] operation is all-or-nothing — a partial batch, or a data row without its
//! index entries, never becomes visible, even across a crash.

#![forbid(unsafe_code)]
#![deny(missing_docs)]

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
