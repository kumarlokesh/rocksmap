# RocksMap

[![Crates.io](https://img.shields.io/crates/v/rocksmap.svg)](https://crates.io/crates/rocksmap)
[![Documentation](https://docs.rs/rocksmap/badge.svg)](https://docs.rs/rocksmap)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

A high-level, type-safe abstraction over RocksDB in Rust, offering ergonomic map-like APIs with zero unsafe code and clean serialization defaults.

## ✨ Features

### Core Features

- **Type-safe**: Full generics support with compile-time type checking
- **Zero unsafe code**: Built entirely on safe Rust abstractions
- **Ergonomic API**: Map-like interface (`get`, `put`, `delete`, `iter`)
- **Serialization**: Pluggable codec system with bincode/serde defaults
- **Column families**: Namespaced data organization
- **Comprehensive error handling**: Rich error types with context

### Advanced Features Overview

- **Batch operations**: Atomic multi-key transactions
- **TTL support**: Automatic key expiration with compaction
- **Range queries**: Efficient key range and prefix scanning
- **Secondary indexes**: Optional indexing layer for complex queries
- **CLI tooling**: Full-featured command-line interface
- **Diagnostics**: Database analysis, integrity checks, and benchmarking

## Usage

```rust
use rocksmap::{RocksMap, Error};
use serde::{Serialize, Deserialize};

#[derive(Serialize, Deserialize, Debug, PartialEq)]
struct User {
    id: u64,
    name: String,
    active: bool,
}

fn main() -> Result<(), Error> {
    let temp_dir = tempfile::tempdir()?;
    let path = temp_dir.path();

    let mut user_db = RocksMap::<u64, User>::open(path)?;

    let user = User {
        id: 1,
        name: String::from("Alice"),
        active: true,
    };
    user_db.put(user.id, &user)?;

    if let Some(retrieved_user) = user_db.get(&1)? {
        println!("Found user: {:?}", retrieved_user);
    }

    user_db.delete(&1)?;

    let mut user_settings = user_db.column_family("settings")?;
    user_settings.put(1, &"dark-mode")?;

    Ok(())
}
```

## 🛠️ CLI Tool

RocksMap includes a powerful CLI for database management and diagnostics:

```bash
# Install the CLI
cargo install rocksmap-cli

# Basic operations
rocksmap-cli put mykey "hello world"
rocksmap-cli get mykey
rocksmap-cli list
rocksmap-cli delete mykey

# Database administration
rocksmap-cli admin stats
rocksmap-cli admin compact
rocksmap-cli admin backup /path/to/backup

# Data import/export
rocksmap-cli export json data.json
rocksmap-cli import csv data.csv

# Diagnostics and analysis
rocksmap-cli diag analyze    # Key distribution analysis
rocksmap-cli diag check      # Integrity verification
rocksmap-cli diag stats      # Detailed RocksDB statistics
rocksmap-cli diag benchmark  # Performance benchmarking

# Interactive shell
rocksmap-cli shell
```

## 📚 API Documentation

### Basic Operations

```rust
use rocksmap::RocksMap;

// Open a database
let mut db = RocksMap::<String, String>::open("./my.db")?;

// Put/Get/Delete
db.put("key1", &"value1")?;
let value = db.get(&"key1")?;
db.delete(&"key1")?;

// Iteration
for result in db.iter() {
    let (key, value) = result?;
    println!("{}: {}", key, value);
}
```

### Advanced Features Code Examples

```rust
// Batch operations
let mut batch = db.batch();
batch.put("key1", &"value1")?;
batch.put("key2", &"value2")?;
batch.delete("old_key")?;
batch.write()?;

// TTL support
db.put_with_ttl("temp_key", &"temp_value", Duration::from_secs(3600))?;

// Range queries
for result in db.range(&"start_key", &"end_key") {
    let (key, value) = result?;
    // Process range results
}

// Prefix scanning
for result in db.prefix_scan(&"prefix_") {
    let (key, value) = result?;
    // Process prefix matches
}

// Column families
let mut cf = db.column_family("namespace")?;
cf.put("key", &"value")?;
```

## License

This project is licensed under the terms of the [MIT License](LICENSE).
