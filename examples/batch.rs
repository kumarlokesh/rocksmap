//! Atomic batch writes: multiple puts/deletes applied together (all or nothing).
//!
//! Run with: `cargo run --example batch`

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
    let db = RocksMap::<u64, User>::open(dir.path())?;

    db.put(
        1,
        &User {
            id: 1,
            name: "Alice".to_string(),
            active: true,
        },
    )?;

    let mut batch = db.batch();
    batch.put(
        &2,
        &User {
            id: 2,
            name: "Bob".to_string(),
            active: false,
        },
    )?;
    batch.put(
        &3,
        &User {
            id: 3,
            name: "Carol".to_string(),
            active: true,
        },
    )?;
    batch.delete(&1)?;
    batch.commit()?; // the two puts and the delete apply atomically, or none do

    println!("after batch:");
    for entry in db.iter()? {
        let (id, user) = entry?;
        println!("  {id}: {}", user.name);
    }

    Ok(())
}
