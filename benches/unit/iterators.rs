use criterion::{black_box, criterion_group, criterion_main, Criterion};
use rocksmap::RocksMap;
use tempfile::TempDir;

/// Quick benchmarks for iterator operations
/// Tests small datasets for fast feedback
fn benchmark_basic_iteration(c: &mut Criterion) {
    let temp_dir = TempDir::new().unwrap();
    let db = RocksMap::<String, String>::open(temp_dir.path()).unwrap();

    for i in 0..1000 {
        db.put(format!("iter_key_{:04}", i), &format!("iter_value_{}", i))
            .unwrap();
    }

    c.bench_function("iterate_1000_items", |b| {
        b.iter(|| {
            let mut count = 0;
            for result in db.iter().unwrap() {
                let (_key, _value) = result.unwrap();
                count += 1;
                black_box(count);
            }
        })
    });
}

fn benchmark_prefix_iteration(c: &mut Criterion) {
    let temp_dir = TempDir::new().unwrap();
    let db = RocksMap::<String, String>::open(temp_dir.path()).unwrap();

    for prefix in ["user", "order", "product"] {
        for i in 0..100 {
            db.put(
                format!("{}_{:04}", prefix, i),
                &format!("{}_value_{}", prefix, i),
            )
            .unwrap();
        }
    }

    c.bench_function("iterate_with_prefix", |b| {
        b.iter(|| {
            let mut count = 0;
            for result in db.iter().unwrap() {
                let (key, _value) = result.unwrap();
                if key.starts_with("user_") {
                    count += 1;
                    black_box(count);
                }
            }
        })
    });
}

fn benchmark_range_iteration(c: &mut Criterion) {
    let temp_dir = TempDir::new().unwrap();
    let db = RocksMap::<String, String>::open(temp_dir.path()).unwrap();

    for i in 0..1000 {
        db.put(format!("range_key_{:04}", i), &format!("range_value_{}", i))
            .unwrap();
    }

    c.bench_function("iterate_range_100_items", |b| {
        b.iter(|| {
            let mut count = 0;
            let start_key = "range_key_0100".to_string();
            let end_key = "range_key_0200".to_string();

            for result in db.iter().unwrap() {
                let (key, _value) = result.unwrap();
                if key >= start_key && key < end_key {
                    count += 1;
                    black_box(count);
                } else if key >= end_key {
                    break;
                }
            }
        })
    });
}

criterion_group!(
    iterators,
    benchmark_basic_iteration,
    benchmark_prefix_iteration,
    benchmark_range_iteration
);
criterion_main!(iterators);
