//! Per-key TTL with `TtlRocksMap`. Uses a `ManualClock` so the example is deterministic
//! (production code would use the default system clock via `TtlRocksMap::open`).
//!
//! Run with: `cargo run --example ttl`

use rocksmap::{ManualClock, TtlRocksMap};
use std::sync::Arc;
use std::time::Duration;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let clock = ManualClock::new(0);
    let cache =
        TtlRocksMap::<String, String>::open_with_clock(dir.path(), Arc::new(clock.clone()))?;

    cache.put_with_ttl(
        "session:abc".to_string(),
        &"alice".to_string(),
        Duration::from_secs(60),
    )?;
    cache.put("config:theme".to_string(), &"dark".to_string())?; // no expiry

    println!("at t=0:");
    println!(
        "  session:abc  = {:?}",
        cache.get(&"session:abc".to_string())?
    );
    println!(
        "  config:theme = {:?}",
        cache.get(&"config:theme".to_string())?
    );

    clock.advance(60_000); // advance 60s: the session deadline passes

    println!("after 60s:");
    println!(
        "  session:abc  = {:?} (expired, read as absent immediately)",
        cache.get(&"session:abc".to_string())?
    );
    println!(
        "  config:theme = {:?} (no TTL, still present)",
        cache.get(&"config:theme".to_string())?
    );

    cache.compact(); // physically reclaim the expired entry
    println!("live entries after compaction: {}", cache.iter().count());

    Ok(())
}
