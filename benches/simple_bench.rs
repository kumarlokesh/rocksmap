use criterion::{black_box, criterion_group, criterion_main, Criterion};
use rocksmap::RocksMap;
use tempfile::TempDir;

fn benchmark_operations(c: &mut Criterion) {
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

    c.bench_function("batch_operations", |b| {
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
}

criterion_group!(benches, benchmark_operations);
criterion_main!(benches);
