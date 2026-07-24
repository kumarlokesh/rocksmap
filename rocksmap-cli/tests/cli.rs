//! Integration tests for `rocksmap-cli`, driving the built binary.

use assert_cmd::Command;
use predicates::prelude::*;
use predicates::str::contains;
use rocksmap::{IndexedRocksMap, RocksMap, TtlRocksMap};
use std::time::Duration;
use tempfile::TempDir;

fn cli(db: &std::path::Path) -> Command {
    let mut c = Command::cargo_bin("rocksmap-cli").unwrap();
    c.arg("--db").arg(db);
    c
}

#[test]
fn plain_put_get_list_info_roundtrip() {
    let dir = TempDir::new().unwrap();
    let db = dir.path();

    cli(db).args(["put", "hello", "world"]).assert().success();
    cli(db)
        .args(["get", "hello"])
        .assert()
        .success()
        .stdout(contains("hello").and(contains("world")));
    cli(db)
        .arg("info")
        .assert()
        .success()
        .stdout(contains("kind:      plain"));
    cli(db)
        .arg("list")
        .assert()
        .success()
        .stdout(contains("hello\tworld"));
    // missing key exits non-zero
    cli(db).args(["get", "nope"]).assert().failure();
}

#[test]
fn refuses_writes_on_ttl_database() {
    let dir = TempDir::new().unwrap();
    let db = dir.path();
    {
        let ttl = TtlRocksMap::<String, String>::open(db).unwrap();
        ttl.put_with_ttl("k".into(), &"v".into(), Duration::from_secs(3600))
            .unwrap();
    } // drop the handle so the CLI can open it

    cli(db)
        .arg("info")
        .assert()
        .success()
        .stdout(contains("kind:      ttl"));

    // A raw write must be refused (it would bypass envelope maintenance).
    cli(db)
        .args(["put", "k2", "v2"])
        .assert()
        .failure()
        .stderr(contains("read-only on TTL"));

    // ...and reads still work, showing the payload (not envelope bytes).
    cli(db)
        .args(["get", "k"])
        .assert()
        .success()
        .stdout(contains("v"));
}

#[test]
fn refuses_writes_on_indexed_database_and_reports_indexes() {
    let dir = TempDir::new().unwrap();
    let db = dir.path();
    {
        let mut builder = IndexedRocksMap::<String, String>::builder(db);
        let _ = builder.index("by_value", |v: &String| Some(v.clone()));
        let map = builder.open().unwrap();
        map.put("a".into(), &"x".into()).unwrap();
    }

    cli(db)
        .arg("info")
        .assert()
        .success()
        .stdout(contains("kind:      indexed").and(contains("by_value")));

    cli(db)
        .args(["put", "b", "y"])
        .assert()
        .failure()
        .stderr(contains("indexed"));
}

#[test]
fn list_cf_hides_internal_by_default() {
    let dir = TempDir::new().unwrap();
    let db = dir.path();
    {
        let _ = RocksMap::<String, String>::open(db).unwrap();
    }

    // default: no internal CFs
    cli(db)
        .args(["admin", "list-cf"])
        .assert()
        .success()
        .stdout(contains("__rocksmap_meta").not());

    // --internal reveals them
    cli(db)
        .args(["admin", "list-cf", "--internal"])
        .assert()
        .success()
        .stdout(contains("__rocksmap_meta"));
}

#[test]
fn export_then_import_roundtrip() {
    let dir = TempDir::new().unwrap();
    let src = dir.path().join("src");
    let dst = dir.path().join("dst");
    let file = dir.path().join("dump.json");

    cli(&src).args(["put", "k1", "v1"]).assert().success();
    cli(&src).args(["put", "k2", "v2"]).assert().success();
    cli(&src)
        .args(["export", "json", file.to_str().unwrap()])
        .assert()
        .success();

    cli(&dst)
        .args(["import", "json", file.to_str().unwrap()])
        .assert()
        .success();
    cli(&dst)
        .args(["get", "k2"])
        .assert()
        .success()
        .stdout(contains("v2"));
}
