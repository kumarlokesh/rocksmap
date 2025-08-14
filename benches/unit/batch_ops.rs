use criterion::{criterion_group, criterion_main, Criterion};
use rocksmap::RocksMap;
use tempfile::TempDir;

/// Quick benchmarks for batch operations
/// Tests small batch sizes for immediate feedback
fn benchmark_small_batch_operations(c: &mut Criterion) {
    let temp_dir = TempDir::new().unwrap();
    let db = RocksMap::<String, String>::open(temp_dir.path()).unwrap();

    c.bench_function("batch_put_10_items", |b| {
        let mut counter = 0;
        b.iter(|| {
            let mut batch = db.batch();
            for i in 0..10 {
                let key = format!("batch_{}_{}", counter, i);
                let value = format!("batch_value_{}", i);
                batch.put(&key, &value).unwrap();
            }
            counter += 1;
            batch.commit().unwrap();
        })
    });

    c.bench_function("batch_put_50_items", |b| {
        let mut counter = 0;
        b.iter(|| {
            let mut batch = db.batch();
            for i in 0..50 {
                let key = format!("batch_{}_{}", counter, i);
                let value = format!("batch_value_{}", i);
                batch.put(&key, &value).unwrap();
            }
            counter += 1;
            batch.commit().unwrap();
        })
    });
}

fn benchmark_mixed_batch_operations(c: &mut Criterion) {
    let temp_dir = TempDir::new().unwrap();
    let db = RocksMap::<String, String>::open(temp_dir.path()).unwrap();

    for i in 0..100 {
        db.put(format!("existing_{}", i), &format!("value_{}", i))
            .unwrap();
    }

    c.bench_function("batch_mixed_operations", |b| {
        let mut counter = 0;
        b.iter(|| {
            let mut batch = db.batch();
            for i in 0..10 {
                let key = format!("new_{}_{}", counter, i);
                let value = format!("new_value_{}", i);
                batch.put(&key, &value).unwrap();
                let existing_key = format!("existing_{}", i % 100);
                let updated_value = format!("updated_{}_{}", counter, i);
                batch.put(&existing_key, &updated_value).unwrap();
            }
            counter += 1;
            batch.commit().unwrap();
        })
    });
}

criterion_group!(
    batch_ops,
    benchmark_small_batch_operations,
    benchmark_mixed_batch_operations
);
criterion_main!(batch_ops);
