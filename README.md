# rocksmap

[![Crates.io](https://img.shields.io/crates/v/rocksmap.svg)](https://crates.io/crates/rocksmap)
[![Documentation](https://docs.rs/rocksmap/badge.svg)](https://docs.rs/rocksmap)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

A typed, ergonomic map-like layer over [RocksDB](https://rocksdb.org/) in Rust. rocksmap lets
you store and query strongly-typed keys and values with serde-based serialization, instead of
hand-rolling byte-slice plumbing on top of the raw `rocksdb` crate.

> **Project status — early, pre-1.0 (0.1.x).** The typed map, column families, atomic batch
> writes, ordered iteration / range / prefix queries, per-key TTL, and atomic secondary indexes
> all work and are tested. APIs may still change before 1.0; treat anything not listed under
> **Available now** as in progress.

## Available now

- **Typed map API** — `open`, `get`, `put`, `delete`, `iter`, plus `contains`, `is_empty`,
  exact `count` (O(n)) and an O(1) `len_estimate`, generic over
  `K, V: Serialize + DeserializeOwned + Clone`.
- **Logical key ordering** — iteration and queries follow the natural order of the key type
  (integers, signed numbers, strings, tuples, `Option`, …) via an order-preserving key
  encoding, regardless of the key's byte layout.
- **Range queries** — `range`/`range_rev` accept any `RangeBounds` (`10..=20`, `10..`, `..20`,
  `..`), forward or reverse, bounded by RocksDB so they don't scan past the range.
- **Prefix scans** — `scan_prefix` for `String`/`Vec<u8>` keys, and `scan_prefix_fields` for
  the leading field(s) of a composite (tuple) key.
- **Column families** — named, isolated keyspaces within one database.
- **Atomic batch writes** — multiple puts/deletes committed together via RocksDB `WriteBatch`.
- **Per-key TTL** — `TtlRocksMap` with `put_with_ttl`/`put_with_expiry` and an optional default
  TTL. Expired entries read as absent immediately and are physically reclaimed at compaction;
  the clock is injectable for testing.
- **Atomic secondary indexes** — `IndexedRocksMap` maintains the data and every declared index
  in one transaction (`TransactionDB`), so updates/deletes keep the index consistent and a
  crash can't leave it diverged. Supports multiple indexes, unique constraints, typed lookup
  handles, and rebuild.
- **Selectable key codec** — the order-preserving `OrderedCodec` is the default; keys that don't
  need ordered queries can opt into `BincodeCodec` (or a custom codec) via
  `RocksMap<K, V, KC>`. `range`/prefix scans are gated to ordered codecs **at compile time**,
  and a reopen with a mismatched codec fails loudly. Values use bincode; `KeyCodec` /
  `ValueCodec` are public.
- **Safe Rust surface** — enforced with `#![forbid(unsafe_code)]`; rocksmap's own crate contains
  no `unsafe` (the underlying `rocksdb` bindings are FFI and are not counted here).

## In progress / planned

- **Value-codec selection, durability/crash-consistency testing, and API stabilization** toward
  a 1.0 release. `Error` is already `#[non_exhaustive]`.

## Installation

Add rocksmap to your `Cargo.toml` (also add `serde` with the `derive` feature for your types):

```toml
[dependencies]
rocksmap = "0.1"
serde = { version = "1", features = ["derive"] }
```

Note that the `rocksdb` dependency compiles RocksDB's C++ engine, so a C++ toolchain is
required to build.

## Usage

```rust
use rocksmap::{RocksMap, Error};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
struct User {
    id: u64,
    name: String,
    active: bool,
}

fn main() -> Result<(), Error> {
    let db = RocksMap::<u64, User>::open("./users.db")?;

    let alice = User { id: 1, name: "Alice".to_string(), active: true };
    db.put(alice.id, &alice)?; // `put` takes the key by value

    if let Some(found) = db.get(&1)? {
        // `get` and `delete` take the key by reference
        println!("found: {found:?}");
    }

    for entry in db.iter()? {
        let (id, user) = entry?;
        println!("{id} -> {user:?}");
    }

    db.delete(&1)?;
    Ok(())
}
```

### Examples

Runnable, compiled examples live in [examples/](examples/). Run any with
`cargo run --example <name>`:

| Example | Shows |
| --- | --- |
| [`basic`](examples/basic.rs) | typed `open`/`put`/`get`/`iter`/`delete` |
| [`column_families`](examples/column_families.rs) | named, isolated keyspaces in one database |
| [`batch`](examples/batch.rs) | atomic multi-key `WriteBatch` |
| [`range_and_prefix`](examples/range_and_prefix.rs) | `range`/`range_rev` and `scan_prefix`/`scan_prefix_fields` |
| [`ttl`](examples/ttl.rs) | per-key expiry with `TtlRocksMap` and an injectable clock |
| [`secondary_indexes`](examples/secondary_indexes.rs) | atomic indexes, unique constraints, and consistent updates with `IndexedRocksMap` |

## CLI

`rocksmap-cli` is a **separate crate** ([rocksmap-cli/](rocksmap-cli/)) — an operational inspector
and operator for rocksmap databases.

It is **safe by default**: it reads and inspects any rocksmap database (plain / TTL / indexed), but
only *mutates* plain databases — a raw write into a TTL or indexed database would bypass
envelope/index maintenance and corrupt invariants, so those are refused.

```bash
# From source (workspace):  cargo run -p rocksmap-cli -- --db ./app.db <command>
# Once published:           cargo install rocksmap-cli

rocksmap-cli --db ./app.db info                 # what is this database? (kind, key codec, indexes)
rocksmap-cli --db ./app.db put mykey "hello"    # plain databases only
rocksmap-cli --db ./app.db get mykey
rocksmap-cli --db ./app.db list --limit 20
rocksmap-cli --db ./app.db scan a m             # inclusive key range
rocksmap-cli --db ./app.db export json dump.json
rocksmap-cli --db ./app.db import json dump.json
rocksmap-cli --db ./app.db admin stats          # also: compact, backup, list-cf
```

Keys are UTF-8 strings by default; `--key-type u64|i64` decodes ordered scalar keys. Values are
shown as text when they decode and as hex otherwise (they are opaque without the value type).

## Benchmarks

Criterion-based benchmarks live in [benches/](benches/). See [benches/README.md](benches/README.md)
for what each benchmark measures and how to run them:

```bash
cargo bench
```

## License

Licensed under the [MIT License](LICENSE).
