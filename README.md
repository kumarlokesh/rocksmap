# rocksmap

[![Crates.io](https://img.shields.io/crates/v/rocksmap.svg)](https://crates.io/crates/rocksmap)
[![Documentation](https://docs.rs/rocksmap/badge.svg)](https://docs.rs/rocksmap)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

A typed, ergonomic map over [RocksDB](https://rocksdb.org/) in Rust — store and query
strongly-typed keys and values with serde, instead of hand-rolling byte-slice plumbing on the raw
`rocksdb` crate.

> **Status: early, pre-1.0.** The features below work and are tested; APIs may change before 1.0.

## Features

- **Typed map** — `get` / `put` / `delete` / `iter`, plus `contains` / `is_empty` / `count` /
  `len_estimate`, generic over `K, V: Serialize + DeserializeOwned + Clone`.
- **Logical key ordering** — iteration, `range` / `range_rev` (any `RangeBounds`), and prefix scans
  (`scan_prefix`, `scan_prefix_fields`) follow the key's natural order via an order-preserving
  encoding. Opt out to `BincodeCodec` for unordered keys (`range`/prefix then don't compile).
- **Column families** and **atomic batch writes** (`WriteBatch`).
- **Per-key TTL** (`TtlRocksMap`) — immediate logical expiry, reclaimed at compaction, injectable clock.
- **Atomic secondary indexes** (`IndexedRocksMap`) — data and indexes updated in one transaction;
  multiple/unique indexes, typed lookups, crash-safe rebuild.
- **Durable & safe** — documented crash guarantee + `sync_wal()`; `#![forbid(unsafe_code)]` and
  `#![deny(missing_docs)]`.

## Install

```toml
[dependencies]
rocksmap = "0.1"
serde = { version = "1", features = ["derive"] }
```

A C++ toolchain is required — the `rocksdb` dependency builds RocksDB from source.

## Usage

```rust
use rocksmap::{RocksMap, Error};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Clone, Debug)]
struct User { id: u64, name: String }

fn main() -> Result<(), Error> {
    let db = RocksMap::<u64, User>::open("./users.db")?;

    let alice = User { id: 1, name: "Alice".to_string() };
    db.put(alice.id, &alice)?;         // `put` takes the key by value
    let _ = db.get(&1)?;               // `get` / `delete` take it by reference

    for entry in db.iter()? {
        let (id, user) = entry?;
        println!("{id} -> {user:?}");
    }

    db.delete(&1)?;
    Ok(())
}
```

Runnable examples in [examples/](examples/) (`cargo run --example <name>`): `basic`,
`column_families`, `batch`, `range_and_prefix`, `ttl`, `secondary_indexes`.

## CLI

`rocksmap-cli` ([rocksmap-cli/](rocksmap-cli/)) inspects and operates databases. It is safe by
default: it reads any database but *mutates only plain ones* — writing to a TTL or indexed database
would corrupt its invariants, so that is refused.

```bash
cargo run -p rocksmap-cli -- --db ./app.db info   # kind, key codec, indexes
cargo run -p rocksmap-cli -- --db ./app.db list
```

## License

MIT — see [LICENSE](LICENSE).
