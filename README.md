# rocksmap

[![Crates.io](https://img.shields.io/crates/v/rocksmap.svg)](https://crates.io/crates/rocksmap)
[![Documentation](https://docs.rs/rocksmap/badge.svg)](https://docs.rs/rocksmap)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

A typed, ergonomic map-like layer over [RocksDB](https://rocksdb.org/) in Rust. rocksmap lets
you store and query strongly-typed keys and values with serde-based serialization, instead of
hand-rolling byte-slice plumbing on top of the raw `rocksdb` crate.

> **Project status — early, pre-1.0 (0.1.x).** The core typed map, column families, and atomic
> batch writes work and are tested. Several advanced areas — key ordering for range/prefix
> scans, TTL, and secondary indexes — are still being reworked before 1.0. Treat anything not
> listed under **Available now** as in progress and subject to change.

## Available now

- **Typed map API** — `open`, `get`, `put`, `delete`, `iter`, generic over
  `K, V: Serialize + DeserializeOwned + Clone`.
- **Column families** — named, isolated keyspaces within one database.
- **Atomic batch writes** — multiple puts/deletes committed together via RocksDB `WriteBatch`.
- **Pluggable codecs** — `KeyCodec` / `ValueCodec` traits, with bincode as the default.
- **Safe Rust surface** — rocksmap's own crate contains no `unsafe` code (the underlying
  `rocksdb` bindings are FFI and are not counted here).

## In progress / planned

- **Order-preserving key encoding** so iteration and ranges follow logical key order. Until
  this lands, `range` only orders correctly for keys whose byte encoding already matches their
  logical order, and `prefix_scan` is limited to string-like keys.
- **Correct, seek-based range and prefix scans.**
- **TTL / expiration.** *Not currently functional;* the existing TTL hooks are placeholders
  and do not expire keys.
- **Atomic, consistent secondary indexes.** The current index helper is experimental and does
  not guarantee atomicity across the data and index on updates.

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

### Column families

A column family is a named keyspace that shares the same key/value types as its parent map
(`User` is the type defined in the example above):

```rust
let mut db = RocksMap::<u64, User>::open("./app.db")?;

let admins = db.column_family("admins")?;
let alice = User { id: 1, name: "Alice".to_string(), active: true };
admins.put(&1, &alice)?; // on a column family, `put` takes the key by reference
let _ = admins.get(&1)?;
```

### Atomic batch writes

```rust
let db = RocksMap::<u64, User>::open("./app.db")?;
let alice = User { id: 1, name: "Alice".to_string(), active: true };
let bob = User { id: 2, name: "Bob".to_string(), active: false };

let mut batch = db.batch();
batch.put(&1, &alice)?;
batch.put(&2, &bob)?;
batch.delete(&3)?;
batch.commit()?; // all operations apply atomically, or none do
```

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
