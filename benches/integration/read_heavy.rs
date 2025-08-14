use criterion::{black_box, criterion_group, criterion_main, Criterion};
use rocksmap::RocksMap;
use std::sync::Arc;
use std::thread;
use tempfile::TempDir;

/// Comprehensive benchmarks for read-heavy workloads
/// Tests realistic scenarios with larger datasets
fn benchmark_sequential_reads(c: &mut Criterion) {
    let temp_dir = TempDir::new().unwrap();
    let db = RocksMap::<String, String>::open(temp_dir.path()).unwrap();

    for i in 0..10000 {
        db.put(format!("seq_key_{:06}", i), &format!("seq_value_{}", i))
            .unwrap();
    }

    c.bench_function("sequential_reads_10k", |b| {
        b.iter(|| {
            for i in 0..1000 {
                let key = format!("seq_key_{:06}", i);
                black_box(db.get(&key).unwrap());
            }
        })
    });
}

fn benchmark_random_reads(c: &mut Criterion) {
    let temp_dir = TempDir::new().unwrap();
    let db = RocksMap::<String, String>::open(temp_dir.path()).unwrap();

    for i in 0..50000 {
        db.put(format!("rand_key_{:06}", i), &format!("rand_value_{}", i))
            .unwrap();
    }

    c.bench_function("random_reads_50k_dataset", |b| {
        let mut counter = 0;
        b.iter(|| {
            for _ in 0..1000 {
                let key_id = (counter * 7919) % 50000;
                let key = format!("rand_key_{:06}", key_id);
                black_box(db.get(&key).unwrap());
                counter += 1;
            }
        })
    });
}

fn benchmark_range_scans(c: &mut Criterion) {
    let temp_dir = TempDir::new().unwrap();
    let db = RocksMap::<String, String>::open(temp_dir.path()).unwrap();

    for category in 0..100 {
        for item in 0..1000 {
            let key = format!("cat_{:03}_item_{:06}", category, item);
            let value = format!("category_{}_item_data_{}", category, item);
            db.put(key, &value).unwrap();
        }
    }

    c.bench_function("range_scan_1000_items", |b| {
        let mut category = 0;
        b.iter(|| {
            let start_key = format!("cat_{:03}_item_000000", category);
            let end_key = format!("cat_{:03}_item_999999", category);
            let mut count = 0;

            for result in db.iter().unwrap() {
                let (key, _value) = result.unwrap();
                if key >= start_key && key <= end_key {
                    count += 1;
                    black_box(count);
                } else if key > end_key {
                    break;
                }
            }
            category = (category + 1) % 100;
        })
    });
}

fn benchmark_concurrent_reads(c: &mut Criterion) {
    let temp_dir = TempDir::new().unwrap();
    let db = Arc::new(RocksMap::<String, String>::open(temp_dir.path()).unwrap());

    for i in 0..20000 {
        db.put(
            format!("concurrent_key_{:06}", i),
            &format!("concurrent_value_{}", i),
        )
        .unwrap();
    }

    c.bench_function("concurrent_reads_4_threads", |b| {
        b.iter(|| {
            let handles: Vec<_> = (0..4)
                .map(|thread_id| {
                    let db_clone = Arc::clone(&db);
                    thread::spawn(move || {
                        for i in 0..250 {
                            let key_id = (thread_id * 5000) + (i * 4);
                            let key = format!("concurrent_key_{:06}", key_id);
                            black_box(db_clone.get(&key).unwrap());
                        }
                    })
                })
                .collect();

            for handle in handles {
                handle.join().unwrap();
            }
        })
    });
}

criterion_group!(
    read_heavy,
    benchmark_sequential_reads,
    benchmark_random_reads,
    benchmark_range_scans,
    benchmark_concurrent_reads
);
criterion_main!(read_heavy);
