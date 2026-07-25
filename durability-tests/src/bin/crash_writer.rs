//! Test helper: writes synced `u64` entries `0, 1, 2, …` to the RocksMap at `argv[1]`, forever,
//! until the process is killed. Used by `tests/crash.rs` to exercise crash recovery.

use rocksmap::RocksMap;

fn main() {
    let path = std::env::args()
        .nth(1)
        .expect("usage: crash_writer <db_path>");
    let db = RocksMap::<u64, u64>::open(&path).expect("open");

    let mut i = 0u64;
    loop {
        db.put(i, &i).expect("put");
        db.sync_wal().expect("sync_wal");
        i += 1;
    }
}
