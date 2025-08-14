use criterion::{black_box, criterion_group, criterion_main, Criterion};
use rocksmap::RocksMap;
use tempfile::TempDir;

/// Quick benchmarks for basic CRUD operations
/// These are designed to run fast and provide immediate feedback during development
fn benchmark_put_operations(c: &mut Criterion) {
    let temp_dir = TempDir::new().unwrap();
    let db = RocksMap::<String, String>::open(temp_dir.path()).unwrap();

    c.bench_function("put_string", |b| {
        let mut counter = 0;
        b.iter(|| {
            let key = format!("key_{}", counter);
            let value = format!("value_{}", counter);
            counter += 1;
            db.put(black_box(key), black_box(&value)).unwrap();
        })
    });
}

fn benchmark_get_operations(c: &mut Criterion) {
    let temp_dir = TempDir::new().unwrap();
    let db = RocksMap::<String, String>::open(temp_dir.path()).unwrap();

    for i in 0..1000 {
        db.put(format!("get_key_{}", i), &format!("get_value_{}", i))
            .unwrap();
    }

    c.bench_function("get_string", |b| {
        let mut counter = 0;
        b.iter(|| {
            let key = format!("get_key_{}", counter % 1000);
            counter += 1;
            black_box(db.get(black_box(&key)).unwrap());
        })
    });
}

fn benchmark_delete_operations(c: &mut Criterion) {
    let temp_dir = TempDir::new().unwrap();
    let db = RocksMap::<String, String>::open(temp_dir.path()).unwrap();

    c.bench_function("delete_string", |b| {
        let mut counter = 0;
        b.iter(|| {
            let key = format!("delete_key_{}", counter);
            let value = format!("delete_value_{}", counter);
            db.put(key.clone(), &value).unwrap();
            db.delete(black_box(&key)).unwrap();
            counter += 1;
        })
    });
}

criterion_group!(
    basic_ops,
    benchmark_put_operations,
    benchmark_get_operations,
    benchmark_delete_operations
);
criterion_main!(basic_ops);
