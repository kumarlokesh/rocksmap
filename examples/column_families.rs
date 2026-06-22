//! Column families: named, isolated keyspaces within one database.
//!
//! Run with: `cargo run --example column_families`

use rocksmap::RocksMap;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
struct User {
    id: u64,
    name: String,
    active: bool,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let mut db = RocksMap::<u64, User>::open(dir.path())?;

    // `column_family` creates the family if needed; `with_cf` opens a handle to an
    // existing one with a shared borrow, so several handles can be held at once.
    db.column_family("users")?;
    db.column_family("admins")?;

    let users = db.with_cf("users");
    let admins = db.with_cf("admins");

    let alice = User {
        id: 1,
        name: "Alice".to_string(),
        active: true,
    };
    let root = User {
        id: 1,
        name: "root".to_string(),
        active: true,
    };

    // The same key (1) lives independently in each column family.
    users.put(1, &alice)?;
    admins.put(1, &root)?;

    println!("users/1  = {:?}", users.get(&1)?);
    println!("admins/1 = {:?}", admins.get(&1)?);

    Ok(())
}
