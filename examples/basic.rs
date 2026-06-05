//! Typed key/value usage: open, put, get, iterate, delete.
//!
//! Run with: `cargo run --example basic`

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

    for user in [
        User {
            id: 1,
            name: "Alice".to_string(),
            active: true,
        },
        User {
            id: 2,
            name: "Bob".to_string(),
            active: false,
        },
        User {
            id: 3,
            name: "Carol".to_string(),
            active: true,
        },
    ] {
        db.put(user.id, &user)?;
    }

    if let Some(user) = db.get(&2)? {
        println!("get(2) = {user:?}");
    }

    println!("all users (ascending by id):");
    for entry in db.iter()? {
        let (id, user) = entry?;
        let status = if user.active { "active" } else { "inactive" };
        println!("  {id}: {} ({status})", user.name);
    }

    db.delete(&2)?;
    println!("after delete(2), get(2) = {:?}", db.get(&2)?);

    Ok(())
}
