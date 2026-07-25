//! Kill-mid-write crash test: SIGKILL the writer at a random point, then reopen and assert the
//! database recovers to a *consistent* state — it opens cleanly and contains a contiguous prefix
//! `0..=max` of the keys the writer was producing. A gap or a failed reopen is a durability bug.

use rocksmap::RocksMap;
use std::process::Command;
use std::time::Duration;
use tempfile::TempDir;

#[test]
fn sigkill_mid_write_recovers_a_contiguous_prefix() {
    let writer = env!("CARGO_BIN_EXE_crash_writer");

    for iter in 0..3u64 {
        let dir = TempDir::new().unwrap();
        let path = dir.path().to_path_buf();

        let mut child = Command::new(writer)
            .arg(&path)
            .spawn()
            .expect("spawn writer");

        // Let it get past process load + DB open and write a meaningful amount (varied per
        // iteration), then kill it abruptly.
        std::thread::sleep(Duration::from_millis(1000 + iter * 250));
        child.kill().expect("kill writer"); // SIGKILL on unix
        let _ = child.wait();
        // Give the OS a moment to release the DB lock held by the dead process.
        std::thread::sleep(Duration::from_millis(50));

        // Must open cleanly (WAL recovers) and hold a contiguous prefix with no gaps.
        let db = RocksMap::<u64, u64>::open(&path).expect("reopen after crash");
        let mut keys: Vec<u64> = db.iter().unwrap().map(|r| r.unwrap().0).collect();
        keys.sort_unstable();

        assert!(
            !keys.is_empty(),
            "iteration {iter}: expected some acknowledged writes to survive the crash"
        );
        for (idx, key) in keys.iter().enumerate() {
            assert_eq!(
                *key,
                idx as u64,
                "iteration {iter}: torn/gapped recovery at index {idx} (len={})",
                keys.len()
            );
        }
    }
}
