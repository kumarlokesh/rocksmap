/// Common utilities for RocksMap benchmarks
///
/// This module provides shared functionality for both unit and integration benchmarks,
/// including test data generation, database setup helpers, and common patterns.
use rocksmap::RocksMap;
use tempfile::TempDir;

/// Creates a temporary database with the specified number of pre-populated entries
pub fn create_test_db_with_data(count: usize) -> (TempDir, RocksMap<String, String>) {
    let temp_dir = TempDir::new().unwrap();
    let db = RocksMap::<String, String>::open(temp_dir.path()).unwrap();

    for i in 0..count {
        let key = format!("test_key_{:06}", i);
        let value = format!("test_value_{}", i);
        db.put(key, &value).unwrap();
    }

    (temp_dir, db)
}

/// Creates a temporary database with structured test data (categories and items)
pub fn create_structured_test_db(
    categories: usize,
    items_per_category: usize,
) -> (TempDir, RocksMap<String, String>) {
    let temp_dir = TempDir::new().unwrap();
    let db = RocksMap::<String, String>::open(temp_dir.path()).unwrap();

    for cat in 0..categories {
        for item in 0..items_per_category {
            let key = format!("cat_{:03}_item_{:06}", cat, item);
            let value = format!("category_{}_item_data_{}", cat, item);
            db.put(key, &value).unwrap();
        }
    }

    (temp_dir, db)
}

/// Generates test data with realistic key patterns
pub fn generate_realistic_keys(prefix: &str, count: usize) -> Vec<(String, String)> {
    (0..count)
        .map(|i| {
            let key = format!("{}:{:06}", prefix, i);
            let value = format!("{}_data_{}", prefix, i);
            (key, value)
        })
        .collect()
}

/// Common benchmark configuration constants
pub mod config {
    /// Small dataset size for unit tests
    pub const UNIT_TEST_SIZE: usize = 1_000;

    /// Medium dataset size for integration tests
    pub const INTEGRATION_TEST_SIZE: usize = 10_000;

    /// Large dataset size for stress tests
    pub const STRESS_TEST_SIZE: usize = 100_000;

    /// Default batch size for batch operations
    pub const DEFAULT_BATCH_SIZE: usize = 100;

    /// Number of concurrent threads for concurrent tests
    pub const CONCURRENT_THREADS: usize = 4;
}
