//! Durability: data and invariants survive a clean close + reopen. (Crash-kill durability is
//! tested by the `durability-tests` workspace member.)

use rocksmap::{IndexedRocksMap, RocksMap, TtlRocksMap};
use std::time::Duration;
use tempfile::TempDir;

#[test]
fn plain_reopen_durability() {
    let dir = TempDir::new().unwrap();
    {
        let db = RocksMap::<u64, String>::open(dir.path()).unwrap();
        for i in 0..100u64 {
            db.put(i, &format!("v{i}")).unwrap();
        }
        db.sync_wal().unwrap();
    } // drop = clean close

    let db = RocksMap::<u64, String>::open(dir.path()).unwrap();
    assert_eq!(db.count().unwrap(), 100);
    assert_eq!(db.get(&42).unwrap(), Some("v42".to_string()));
}

#[test]
fn batch_atomic_across_reopen() {
    let dir = TempDir::new().unwrap();
    {
        let db = RocksMap::<u64, u64>::open(dir.path()).unwrap();
        let mut batch = db.batch();
        batch.put(&1, &10).unwrap();
        batch.put(&2, &20).unwrap();
        batch.put(&3, &30).unwrap();
        batch.commit().unwrap();
        db.sync_wal().unwrap();
    }

    let db = RocksMap::<u64, u64>::open(dir.path()).unwrap();
    // The whole batch survived as a unit.
    assert_eq!(db.count().unwrap(), 3);
    assert_eq!(db.get(&2).unwrap(), Some(20));
}

#[test]
fn ttl_reopen_durability() {
    let dir = TempDir::new().unwrap();
    {
        let db = TtlRocksMap::<String, String>::open(dir.path()).unwrap();
        db.put("perm".into(), &"keep".into()).unwrap();
        db.put_with_ttl("temp".into(), &"live".into(), Duration::from_secs(3600))
            .unwrap();
        db.sync_wal().unwrap();
    }

    let db = TtlRocksMap::<String, String>::open(dir.path()).unwrap();
    assert_eq!(
        db.get(&"perm".to_string()).unwrap(),
        Some("keep".to_string())
    );
    assert_eq!(
        db.get(&"temp".to_string()).unwrap(),
        Some("live".to_string())
    );
}

#[test]
fn indexed_reopen_keeps_index_consistent() {
    let dir = TempDir::new().unwrap();
    {
        let mut b = IndexedRocksMap::<u64, String>::builder(dir.path());
        let _by_val = b.index("by_val", |v: &String| Some(v.clone()));
        let map = b.open().unwrap();
        map.put(1, &"x".into()).unwrap();
        map.put(2, &"y".into()).unwrap();
    }

    // Reopen re-declaring the same index; the index must still match the data.
    let mut b = IndexedRocksMap::<u64, String>::builder(dir.path());
    let by_val = b.index("by_val", |v: &String| Some(v.clone()));
    let map = b.open().unwrap();
    assert_eq!(map.get(&1).unwrap(), Some("x".to_string()));
    assert_eq!(map.count().unwrap(), 2);
    assert_eq!(
        map.find_keys_by(&by_val, &"y".to_string()).unwrap(),
        vec![2]
    );
}
