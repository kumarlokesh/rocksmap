//! Atomic secondary indexes with `IndexedRocksMap`: lookups by non-key fields, a unique
//! constraint, and consistent updates/deletes.
//!
//! Run with: `cargo run --example secondary_indexes`

use rocksmap::IndexedRocksMap;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Clone, Debug)]
struct User {
    id: u64,
    email: String,
    org: String,
}

fn user(id: u64, email: &str, org: &str) -> User {
    User {
        id,
        email: email.to_string(),
        org: org.to_string(),
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;

    // Declare indexes up front; each returns a typed handle used for lookups.
    let mut builder = IndexedRocksMap::<u64, User>::builder(dir.path());
    let by_org = builder.index("by_org", |u: &User| Some(u.org.clone()));
    let by_email = builder.unique_index("by_email", |u: &User| Some(u.email.clone()));
    let users = builder.open()?;

    users.put(1, &user(1, "a@x.com", "x"))?;
    users.put(2, &user(2, "b@x.com", "x"))?;
    users.put(3, &user(3, "c@y.com", "y"))?;

    println!(
        "org x members: {:?}",
        users.find_keys_by(&by_org, &"x".to_string())?
    );
    println!(
        "by email b@x.com: {:?}",
        users
            .find_by(&by_email, &"b@x.com".to_string())?
            .iter()
            .map(|u| u.id)
            .collect::<Vec<_>>()
    );

    // A unique index rejects a second row with the same secondary key (atomically rolled back).
    match users.put(4, &user(4, "a@x.com", "z")) {
        Err(e) => println!("rejected duplicate email: {e}"),
        Ok(()) => println!("unexpected success"),
    }

    // Updating an indexed field moves the entry across indexes atomically.
    users.put(1, &user(1, "a2@x.com", "y"))?;
    println!(
        "after moving user 1 to org y: org x = {:?}, org y = {:?}",
        users.find_keys_by(&by_org, &"x".to_string())?,
        users.find_keys_by(&by_org, &"y".to_string())?,
    );
    println!(
        "old email a@x.com now maps to: {:?}",
        users.find_keys_by(&by_email, &"a@x.com".to_string())?,
    );

    users.delete(&2)?;
    println!(
        "after delete(2), org x = {:?}",
        users.find_keys_by(&by_org, &"x".to_string())?
    );

    Ok(())
}
