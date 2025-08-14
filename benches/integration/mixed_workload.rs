use criterion::{black_box, criterion_group, criterion_main, Criterion};
use rocksmap::RocksMap;
use std::sync::Arc;
use std::thread;
use tempfile::TempDir;

/// Comprehensive benchmarks for mixed read/write workloads
/// Tests realistic production scenarios with balanced operations
fn benchmark_balanced_workload(c: &mut Criterion) {
    let temp_dir = TempDir::new().unwrap();
    let db = RocksMap::<String, String>::open(temp_dir.path()).unwrap();

    for i in 0..20000 {
        db.put(format!("mixed_key_{:06}", i), &format!("mixed_value_{}", i))
            .unwrap();
    }

    c.bench_function("balanced_read_write_70_30", |b| {
        let mut counter = 0;
        b.iter(|| {
            for i in 0..1000 {
                if i < 700 {
                    // 70% reads
                    let key_id = (counter + i) % 20000;
                    let key = format!("mixed_key_{:06}", key_id);
                    black_box(db.get(&key).unwrap());
                } else {
                    // 30% writes
                    let key = format!("new_mixed_{}_{:06}", counter, i);
                    let value = format!("new_mixed_value_{}_{}", counter, i);
                    db.put(black_box(key), black_box(&value)).unwrap();
                }
            }
            counter += 1;
        })
    });
}

fn benchmark_read_heavy_workload(c: &mut Criterion) {
    let temp_dir = TempDir::new().unwrap();
    let db = RocksMap::<String, String>::open(temp_dir.path()).unwrap();

    for i in 0..50000 {
        db.put(
            format!("read_heavy_key_{:06}", i),
            &format!("read_heavy_value_{}", i),
        )
        .unwrap();
    }

    c.bench_function("read_heavy_90_10", |b| {
        let mut counter = 0;
        b.iter(|| {
            for i in 0..1000 {
                if i < 900 {
                    // 90% reads
                    let key_id = (counter * 7919 + i) % 50000; // Pseudo-random access
                    let key = format!("read_heavy_key_{:06}", key_id);
                    black_box(db.get(&key).unwrap());
                } else {
                    // 10% writes
                    let key = format!("new_read_heavy_{}_{:06}", counter, i);
                    let value = format!("new_read_heavy_value_{}_{}", counter, i);
                    db.put(black_box(key), black_box(&value)).unwrap();
                }
            }
            counter += 1;
        })
    });
}

fn benchmark_write_heavy_workload(c: &mut Criterion) {
    let temp_dir = TempDir::new().unwrap();
    let db = RocksMap::<String, String>::open(temp_dir.path()).unwrap();

    for i in 0..10000 {
        db.put(
            format!("write_heavy_key_{:06}", i),
            &format!("write_heavy_value_{}", i),
        )
        .unwrap();
    }

    c.bench_function("write_heavy_30_70", |b| {
        let mut counter = 0;
        b.iter(|| {
            for i in 0..1000 {
                if i < 300 {
                    // 30% reads
                    let key_id = (counter + i) % 10000;
                    let key = format!("write_heavy_key_{:06}", key_id);
                    black_box(db.get(&key).unwrap());
                } else {
                    // 70% writes (mix of inserts and updates)
                    if i < 650 {
                        // New inserts
                        let key = format!("new_write_heavy_{}_{:06}", counter, i);
                        let value = format!("new_write_heavy_value_{}_{}", counter, i);
                        db.put(black_box(key), black_box(&value)).unwrap();
                    } else {
                        // Updates
                        let key_id = (counter + i) % 10000;
                        let key = format!("write_heavy_key_{:06}", key_id);
                        let value = format!("updated_write_heavy_{}_{}", counter, i);
                        db.put(black_box(key), black_box(&value)).unwrap();
                    }
                }
            }
            counter += 1;
        })
    });
}

fn benchmark_concurrent_mixed_workload(c: &mut Criterion) {
    let temp_dir = TempDir::new().unwrap();
    let db = Arc::new(RocksMap::<String, String>::open(temp_dir.path()).unwrap());

    for i in 0..30000 {
        db.put(
            format!("concurrent_mixed_{:06}", i),
            &format!("concurrent_mixed_value_{}", i),
        )
        .unwrap();
    }

    c.bench_function("concurrent_mixed_4_threads", |b| {
        let mut counter = 0;
        b.iter(|| {
            let batch_id = counter;
            let handles: Vec<_> = (0..4)
                .map(|thread_id| {
                    let db_clone = Arc::clone(&db);
                    thread::spawn(move || {
                        for i in 0..250 {
                            if i < 175 {
                                // 70% reads
                                let key_id = (thread_id * 7500 + i * 4) % 30000;
                                let key = format!("concurrent_mixed_{:06}", key_id);
                                black_box(db_clone.get(&key).unwrap());
                            } else {
                                // 30% writes
                                let key = format!(
                                    "new_concurrent_{}_{}_{}_{:06}",
                                    batch_id, thread_id, i, i
                                );
                                let value = format!(
                                    "new_concurrent_value_{}_{}_{}",
                                    batch_id, thread_id, i
                                );
                                db_clone.put(key, &value).unwrap();
                            }
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

fn benchmark_realistic_application_pattern(c: &mut Criterion) {
    let temp_dir = TempDir::new().unwrap();
    let db = RocksMap::<String, String>::open(temp_dir.path()).unwrap();

    for category in ["user", "session", "product", "order"] {
        for i in 0..5000 {
            let key = format!("{}:{:06}", category, i);
            let value = format!("{}_data_{}", category, i);
            db.put(key, &value).unwrap();
        }
    }

    c.bench_function("realistic_app_pattern", |b| {
        let mut counter = 0;
        b.iter(|| {
            for i in 0..100 {
                match i % 10 {
                    // 40% user lookups
                    0..=3 => {
                        let user_id = (counter + i) % 5000;
                        let key = format!("user:{:06}", user_id);
                        black_box(db.get(&key).unwrap());
                    }
                    // 20% session operations
                    4..=5 => {
                        let session_id = (counter + i) % 5000;
                        let key = format!("session:{:06}", session_id);
                        if i % 2 == 0 {
                            black_box(db.get(&key).unwrap());
                        } else {
                            let value = format!("session_data_{}_{}", counter, i);
                            db.put(key, &value).unwrap();
                        }
                    }
                    // 20% product lookups
                    6..=7 => {
                        let product_id = (counter + i) % 5000;
                        let key = format!("product:{:06}", product_id);
                        black_box(db.get(&key).unwrap());
                    }
                    // 20% order operations
                    8..=9 => {
                        let order_id = counter * 100 + i;
                        let key = format!("order:{:06}", order_id);
                        let value = format!("order_data_{}_{}", counter, i);
                        db.put(key, &value).unwrap();
                    }
                    _ => unreachable!(),
                }
            }
            counter += 1;
        })
    });
}

criterion_group!(
    mixed_workload,
    benchmark_balanced_workload,
    benchmark_read_heavy_workload,
    benchmark_write_heavy_workload,
    benchmark_concurrent_mixed_workload,
    benchmark_realistic_application_pattern
);
criterion_main!(mixed_workload);
