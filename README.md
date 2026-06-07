# rocksmap

[![Crates.io](https://img.shields.io/crates/v/rocksmap.svg)](https://crates.io/crates/rocksmap)
[![Documentation](https://docs.rs/rocksmap/badge.svg)](https://docs.rs/rocksmap)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

A typed, ergonomic map-like layer over [RocksDB](https://rocksdb.org/) in Rust. rocksmap lets
you store and query strongly-typed keys and values with serde-based serialization, instead of
hand-rolling byte-slice plumbing on top of the raw `rocksdb` crate.

> **Project status — early, pre-1.0 (0.1.x).** The core typed map, column families, atomic
> batch writes, ordered iteration / range / prefix queries, and per-key TTL work and are tested.
> Secondary indexes are still being reworked before 1.0. Treat anything not listed under
> **Available now** as in progress and subject to change.

## Available now

- **Typed map API** — `open`, `get`, `put`, `delete`, `iter`, generic over
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
- **Serialization codecs** — order-preserving encoding for keys and bincode for values by
  default; the `KeyCodec` / `ValueCodec` traits are public.
- **Safe Rust surface** — rocksmap's own crate contains no `unsafe` code (the underlying
  `rocksdb` bindings are FFI and are not counted here).

## In progress / planned

- **Atomic, consistent secondary indexes.** The current index helper is experimental and does
  not guarantee atomicity across the data and index on updates.
- **Selectable key codec.** Keys use the order-preserving codec; an opt-out (e.g. for
  unordered/bincode keys) is planned.

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
| [`cli_tool_demo`](examples/cli_tool_demo.rs) | driving the `rocksmap-cli` binary |

## CLI

rocksmap ships a command-line tool, `rocksmap-cli`, as a binary target of this crate (it is
not a separate crate). It operates on a database of UTF-8 string keys and string values. It is
under active development.

Build and run it from source:

```bash
cargo build --release --bin rocksmap-cli

./target/release/rocksmap-cli put mykey "hello world"
./target/release/rocksmap-cli get mykey
./target/release/rocksmap-cli list
./target/release/rocksmap-cli delete mykey

# Additional command groups
./target/release/rocksmap-cli admin   --help   # stats, compact, backup, column families
./target/release/rocksmap-cli import  --help   # json / csv import
./target/release/rocksmap-cli export  --help   # json / csv export
./target/release/rocksmap-cli diag    --help   # analysis, integrity checks, benchmarking
./target/release/rocksmap-cli shell             # interactive shell
```

## Benchmarks

Criterion-based benchmarks live in [benches/](benches/). See [benches/README.md](benches/README.md)
for what each benchmark measures and how to run them:

```bash
cargo bench
```

## License

Licensed under the [MIT License](LICENSE).
