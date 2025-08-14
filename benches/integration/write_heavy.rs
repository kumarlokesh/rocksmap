use criterion::{black_box, criterion_group, criterion_main, Criterion};
use rocksmap::RocksMap;
use std::sync::Arc;
use std::thread;
use tempfile::TempDir;

/// Comprehensive benchmarks for write-heavy workloads
/// Tests realistic scenarios with large datasets and high throughput
fn benchmark_sequential_writes(c: &mut Criterion) {
    let temp_dir = TempDir::new().unwrap();
    let db = RocksMap::<String, String>::open(temp_dir.path()).unwrap();

    c.bench_function("sequential_writes_10k", |b| {
        let mut counter = 0;
        b.iter(|| {
            for i in 0..1000 {
                let key = format!("seq_write_{}_{:06}", counter, i);
                let value = format!("seq_value_{}_{}", counter, i);
                db.put(black_box(key), black_box(&value)).unwrap();
            }
            counter += 1;
        })
    });
}

fn benchmark_large_batch_writes(c: &mut Criterion) {
    let temp_dir = TempDir::new().unwrap();
    let db = RocksMap::<String, String>::open(temp_dir.path()).unwrap();

    c.bench_function("large_batch_1000_items", |b| {
        let mut counter = 0;
        b.iter(|| {
            let mut batch = db.batch();
            for i in 0..1000 {
                let key = format!("batch_{}_{:06}", counter, i);
                let value = format!("batch_value_{}_{}", counter, i);
                batch.put(&key, &value).unwrap();
            }
            counter += 1;
            batch.commit().unwrap();
        })
    });

    c.bench_function("large_batch_5000_items", |b| {
        let mut counter = 0;
        b.iter(|| {
            let mut batch = db.batch();
            for i in 0..5000 {
                let key = format!("large_batch_{}_{:06}", counter, i);
                let value = format!("large_batch_value_{}_{}", counter, i);
                batch.put(&key, &value).unwrap();
            }
            counter += 1;
            batch.commit().unwrap();
        })
    });
}

fn benchmark_concurrent_writes(c: &mut Criterion) {
    let temp_dir = TempDir::new().unwrap();
    let db = Arc::new(RocksMap::<String, String>::open(temp_dir.path()).unwrap());

    c.bench_function("concurrent_writes_4_threads", |b| {
        let mut counter = 0;
        b.iter(|| {
            let batch_id = counter;
            let handles: Vec<_> = (0..4)
                .map(|thread_id| {
                    let db_clone = Arc::clone(&db);
                    thread::spawn(move || {
                        for i in 0..250 {
                            let key =
                                format!("concurrent_{}_{}_{}_{:06}", batch_id, thread_id, i, i);
                            let value =
                                format!("concurrent_value_{}_{}_{}", batch_id, thread_id, i);
                            db_clone.put(key, &value).unwrap();
                        }
                    })
                })
                .collect();

            for handle in handles {
                handle.join().unwrap();
            }
            counter += 1;
        })
    });
}

fn benchmark_update_heavy_workload(c: &mut Criterion) {
    let temp_dir = TempDir::new().unwrap();
    let db = RocksMap::<String, String>::open(temp_dir.path()).unwrap();

    for i in 0..10000 {
        db.put(
            format!("update_key_{:06}", i),
            &format!("initial_value_{}", i),
        )
        .unwrap();
    }

    c.bench_function("update_heavy_1000_updates", |b| {
        let mut counter = 0;
        b.iter(|| {
            for i in 0..1000 {
                let key_id = (counter + i) % 10000;
                let key = format!("update_key_{:06}", key_id);
                let value = format!("updated_value_{}_{}", counter, i);
                db.put(black_box(key), black_box(&value)).unwrap();
            }
            counter += 1000;
        })
    });
}

fn benchmark_mixed_write_operations(c: &mut Criterion) {
    let temp_dir = TempDir::new().unwrap();
    let db = RocksMap::<String, String>::open(temp_dir.path()).unwrap();

    for i in 0..5000 {
        db.put(
            format!("existing_{:06}", i),
            &format!("existing_value_{}", i),
        )
        .unwrap();
    }

    c.bench_function("mixed_write_operations", |b| {
        let mut counter = 0;
        b.iter(|| {
            let mut batch = db.batch();

            // Mix of new inserts, updates, and deletes
            for i in 0..500 {
                // 60% new inserts
                if i < 300 {
                    let key = format!("new_{}_{:06}", counter, i);
                    let value = format!("new_value_{}_{}", counter, i);
                    batch.put(&key, &value).unwrap();
                }
                // 30% updates
                else if i < 450 {
                    let key_id = (counter + i) % 5000;
                    let key = format!("existing_{:06}", key_id);
                    let value = format!("updated_{}_{}", counter, i);
                    batch.put(&key, &value).unwrap();
                }
                // 10% deletes
                else {
                    let key_id = (counter + i) % 5000;
                    let key = format!("existing_{:06}", key_id);
                    batch.delete(&key).unwrap();
                }
            }

            counter += 1;
            batch.commit().unwrap();
        })
    });
}

criterion_group!(
    write_heavy,
    benchmark_sequential_writes,
    benchmark_large_batch_writes,
    benchmark_concurrent_writes,
    benchmark_update_heavy_workload,
    benchmark_mixed_write_operations
);
criterion_main!(write_heavy);
