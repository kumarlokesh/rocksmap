//! Range and prefix queries over the order-preserving key encoding.
//!
//! Run with: `cargo run --example range_and_prefix`

use rocksmap::RocksMap;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Range queries follow logical key order, not byte layout.
    let dir = tempfile::tempdir()?;
    let nums = RocksMap::<u64, String>::open(dir.path())?;
    for k in [1u64, 2, 10, 256, 1000] {
        nums.put(k, &format!("n{k}"))?;
    }

    print!("range(10..=256)  =");
    for entry in nums.range(10..=256)? {
        print!(" {}", entry?.0);
    }
    println!(); // 10 256

    print!("range_rev(..)    =");
    for entry in nums.range_rev(..)? {
        print!(" {}", entry?.0);
    }
    println!(); // 1000 256 10 2 1

    // Prefix scan over string keys.
    let dir = tempfile::tempdir()?;
    let logs = RocksMap::<String, String>::open(dir.path())?;
    for (k, v) in [
        ("2026-06-04:a", "x"),
        ("2026-06-05:a", "y"),
        ("2026-06-05:b", "z"),
        ("2026-07-01:a", "w"),
    ] {
        logs.put(k.to_string(), &v.to_string())?;
    }
    println!("scan_prefix(\"2026-06-05\"):");
    for entry in logs.scan_prefix("2026-06-05")? {
        let (k, v) = entry?;
        println!("  {k} = {v}");
    }

    // Composite-key prefix: fix the leading field of a `(user_id, timestamp)` key.
    let dir = tempfile::tempdir()?;
    let events = RocksMap::<(u64, u64), String>::open(dir.path())?;
    for user in [1u64, 2] {
        for ts in [10u64, 20, 30] {
            events.put((user, ts), &format!("u{user}@{ts}"))?;
        }
    }
    println!("scan_prefix_fields((1,)) — all events for user 1, by timestamp:");
    for entry in events.scan_prefix_fields(&(1u64,))? {
        let ((user, ts), data) = entry?;
        println!("  ({user}, {ts}) = {data}");
    }

    Ok(())
}
