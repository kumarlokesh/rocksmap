use criterion::{black_box, criterion_group, criterion_main, Criterion};
use rocksmap::RocksMap;

use tempfile::TempDir;

fn benchmark_basic_operations(c: &mut Criterion) {
    let temp_dir = TempDir::new().unwrap();
    let db = RocksMap::<String, String>::open(temp_dir.path()).unwrap();

    c.bench_function("put_operation", |b| {
        let mut counter = 0;
        b.iter(|| {
            let key = format!("key_{}", counter);
            let value = format!("value_{}", counter);
            counter += 1;
            db.put(black_box(key), black_box(&value)).unwrap();
        })
    });

    for i in 0..1000 {
        db.put(format!("bench_key_{}", i), &format!("bench_value_{}", i))
            .unwrap();
    }

    c.bench_function("get_operation", |b| {
        let mut counter = 0;
        b.iter(|| {
            let key = format!("bench_key_{}", counter % 1000);
            counter += 1;
            black_box(db.get(black_box(&key)).unwrap());
        })
    });

    c.bench_function("delete_operation", |b| {
        let mut counter = 0;
        b.iter(|| {
            let key = format!("delete_key_{}", counter);
            counter += 1;
            db.put(key.clone(), &"temp_value".to_string()).unwrap();
            db.delete(black_box(&key)).unwrap();
        })
    });
}

fn benchmark_batch_operations(c: &mut Criterion) {
    let temp_dir = TempDir::new().unwrap();
    let db = RocksMap::<String, String>::open(temp_dir.path()).unwrap();

    c.bench_function("batch_write_100", |b| {
        let mut counter = 0;
        b.iter(|| {
            let mut batch = db.batch();
            for i in 0..100 {
                let key = format!("batch_key_{}_{}", counter, i);
                let value = format!("batch_value_{}_{}", counter, i);
                batch.put(&key, &value).unwrap();
            }
            counter += 1;
            batch.commit().unwrap();
        })
    });
}

fn benchmark_iteration(c: &mut Criterion) {
    let temp_dir = TempDir::new().unwrap();
    let db = RocksMap::<String, String>::open(temp_dir.path()).unwrap();

    for i in 0..10000 {
        db.put(format!("iter_key_{:06}", i), &format!("iter_value_{}", i))
            .unwrap();
    }

    c.bench_function("full_iteration", |b| {
        b.iter(|| {
            let mut count = 0;
            for result in db.iter().unwrap() {
                match result {
                    Ok((_key, _value)) => {}
                    Err(_) => break,
                }
                count += 1;
            }
            black_box(count);
        })
    });

    c.bench_function("prefix_scan", |b| {
        b.iter(|| {
            let mut count = 0;
            for result in db.prefix_scan(&"key_".to_string()).unwrap() {
                match result {
                    Ok((_key, _value)) => {}
                    Err(_) => break,
                }
                count += 1;
            }
            black_box(count);
        })
    });

    c.bench_function("range_query", |b| {
        b.iter(|| {
            let mut count = 0;
            for result in db
                .range(&"key_100".to_string(), &"key_200".to_string())
                .unwrap()
            {
                match result {
                    Ok((_key, _value)) => {}
                    Err(_) => break,
                }
                count += 1;
            }
            black_box(count);
        })
    });
}

fn benchmark_column_families(c: &mut Criterion) {
    let temp_dir = TempDir::new().unwrap();
    let mut db = RocksMap::<String, String>::open(temp_dir.path()).unwrap();

    c.bench_function("column_family_operations", |b| {
        let mut counter = 0;
        b.iter(|| {
            let cf_name = format!("cf_{}", counter % 10);
            let cf = db.column_family(&cf_name).unwrap();
            let key = format!("cf_key_{}", counter);
            let value = format!("cf_value_{}", counter);

            cf.put(&key, &value).unwrap();
            let _retrieved = cf.get(&key).unwrap();
            cf.delete(&key).unwrap();

            counter += 1;
        })
    });
}

criterion_group!(
    benches,
    benchmark_basic_operations,
    benchmark_batch_operations,
    benchmark_iteration,
    benchmark_column_families
);
criterion_main!(benches);
