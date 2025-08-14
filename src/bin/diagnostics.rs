use anyhow::Result;
use clap::{Parser, Subcommand};
use rocksdb::{Options, DB};
use rocksmap::RocksMap;
use std::collections::HashMap;
use std::path::PathBuf;
use std::time::Instant;

#[derive(Parser)]
#[command(name = "rocksmap-diag")]
#[command(about = "Diagnostic utilities for RocksMap databases")]
#[command(version = "0.1.0")]
struct Cli {
    /// Path to the RocksDB database
    #[arg(short, long, default_value = "./rocksmap.db")]
    database: PathBuf,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Analyze key distribution and patterns
    Analyze,
    /// Check database integrity
    Check,
    /// Show detailed database statistics
    Stats,
    /// Scan keyspace for patterns
    Scan {
        /// Pattern to search for (regex)
        #[arg(short, long)]
        pattern: Option<String>,
        /// Show key size distribution
        #[arg(long)]
        key_sizes: bool,
        /// Show value size distribution
        #[arg(long)]
        value_sizes: bool,
    },
    /// Benchmark database performance
    Benchmark {
        /// Number of operations to perform
        #[arg(short, long, default_value = "1000")]
        operations: usize,
        /// Operation type (read, write, mixed)
        #[arg(short, long, default_value = "mixed")]
        op_type: String,
    },
}

fn main() -> Result<()> {
    let cli = Cli::parse();

    match &cli.command {
        Commands::Analyze => analyze_database(&cli.database),
        Commands::Check => check_integrity(&cli.database),
        Commands::Stats => show_detailed_stats(&cli.database),
        Commands::Scan {
            pattern,
            key_sizes,
            value_sizes,
        } => scan_keyspace(&cli.database, pattern.as_deref(), *key_sizes, *value_sizes),
        Commands::Benchmark {
            operations,
            op_type,
        } => benchmark_database(&cli.database, *operations, op_type),
    }
}

fn analyze_database(db_path: &PathBuf) -> Result<()> {
    println!("Analyzing RocksMap Database");
    println!("==========================");
    println!("Database: {:?}\n", db_path);

    let db = RocksMap::<String, String>::open(db_path)?;

    let mut total_keys = 0;
    let mut total_key_bytes = 0;
    let mut total_value_bytes = 0;
    let mut key_prefixes = HashMap::new();
    let mut key_lengths = Vec::new();
    let mut value_lengths = Vec::new();

    for result in db.iter()? {
        let (key, value) = result?;
        total_keys += 1;
        total_key_bytes += key.len();
        total_value_bytes += value.len();
        key_lengths.push(key.len());
        value_lengths.push(value.len());

        let prefix = if let Some(pos) = key.find(':') {
            &key[..pos]
        } else if let Some(pos) = key.find('_') {
            &key[..pos]
        } else {
            "no_prefix"
        };
        *key_prefixes.entry(prefix.to_string()).or_insert(0) += 1;
    }

    key_lengths.sort();
    value_lengths.sort();

    println!("📊 General Statistics:");
    println!("  Total keys: {}", total_keys);
    println!(
        "  Total key bytes: {} ({:.2} KB)",
        total_key_bytes,
        total_key_bytes as f64 / 1024.0
    );
    println!(
        "  Total value bytes: {} ({:.2} KB)",
        total_value_bytes,
        total_value_bytes as f64 / 1024.0
    );
    println!(
        "  Average key length: {:.1}",
        if total_keys > 0 {
            total_key_bytes as f64 / total_keys as f64
        } else {
            0.0
        }
    );
    println!(
        "  Average value length: {:.1}",
        if total_keys > 0 {
            total_value_bytes as f64 / total_keys as f64
        } else {
            0.0
        }
    );

    if !key_lengths.is_empty() {
        println!("\n📏 Key Length Distribution:");
        println!("  Min: {}", key_lengths[0]);
        println!("  Max: {}", key_lengths[key_lengths.len() - 1]);
        println!("  Median: {}", key_lengths[key_lengths.len() / 2]);
    }

    if !value_lengths.is_empty() {
        println!("\n📏 Value Length Distribution:");
        println!("  Min: {}", value_lengths[0]);
        println!("  Max: {}", value_lengths[value_lengths.len() - 1]);
        println!("  Median: {}", value_lengths[value_lengths.len() / 2]);
    }

    if !key_prefixes.is_empty() {
        println!("\n🏷️  Key Prefix Analysis:");
        let mut sorted_prefixes: Vec<_> = key_prefixes.iter().collect();
        sorted_prefixes.sort_by(|a, b| b.1.cmp(a.1));

        for (prefix, count) in sorted_prefixes.iter().take(10) {
            let percentage = (**count as f64 / total_keys as f64) * 100.0;
            println!("  {}: {} keys ({:.1}%)", prefix, count, percentage);
        }
    }

    Ok(())
}

fn check_integrity(db_path: &PathBuf) -> Result<()> {
    println!("Database Integrity Check");
    println!("=======================");
    println!("Database: {:?}\n", db_path);

    let db = RocksMap::<String, String>::open(db_path)?;

    let mut total_keys = 0;
    let mut errors = Vec::new();
    let mut empty_keys = 0;
    let mut empty_values = 0;

    println!("🔍 Scanning database entries...");

    for result in db.iter()? {
        match result {
            Ok((key, value)) => {
                total_keys += 1;

                if key.is_empty() {
                    empty_keys += 1;
                    errors.push("Found empty key".to_string());
                }

                if value.is_empty() {
                    empty_values += 1;
                }
            }
            Err(e) => {
                errors.push(format!("Iterator error: {}", e));
            }
        }
    }

    println!("✅ Integrity Check Results:");
    println!("  Total keys scanned: {}", total_keys);
    println!("  Empty keys: {}", empty_keys);
    println!("  Empty values: {}", empty_values);
    println!("  Errors found: {}", errors.len());

    if errors.is_empty() {
        println!("  Status: ✅ Database appears healthy");
    } else {
        println!("  Status: ⚠️  Issues detected");
        println!("\n🚨 Issues found:");
        for error in &errors {
            println!("  - {}", error);
        }
    }

    Ok(())
}

fn show_detailed_stats(db_path: &PathBuf) -> Result<()> {
    println!("Detailed Database Statistics");
    println!("===========================");
    println!("Database: {:?}\n", db_path);

    let db = DB::open_default(db_path)?;

    let properties = [
        ("rocksdb.estimate-num-keys", "Estimated Keys"),
        ("rocksdb.total-sst-files-size", "SST Files Size (bytes)"),
        ("rocksdb.cur-size-all-mem-tables", "Memtable Size (bytes)"),
        ("rocksdb.num-files-at-level0", "Level 0 Files"),
        ("rocksdb.num-files-at-level1", "Level 1 Files"),
        ("rocksdb.num-files-at-level2", "Level 2 Files"),
        ("rocksdb.compaction-pending", "Compaction Pending"),
        ("rocksdb.background-errors", "Background Errors"),
        ("rocksdb.num-running-compactions", "Running Compactions"),
        ("rocksdb.num-running-flushes", "Running Flushes"),
    ];

    println!("🗄️  RocksDB Internal Statistics:");
    for (property, description) in &properties {
        if let Some(value) = db.property_value(*property)? {
            println!("  {}: {}", description, value);
        }
    }

    let cf_names = DB::list_cf(&Options::default(), db_path)?;
    println!("\n📁 Column Families:");
    for (i, name) in cf_names.iter().enumerate() {
        println!("  {}. {}", i + 1, name);
    }

    Ok(())
}

fn scan_keyspace(
    db_path: &PathBuf,
    pattern: Option<&str>,
    show_key_sizes: bool,
    show_value_sizes: bool,
) -> Result<()> {
    println!("Keyspace Scan");
    println!("=============");
    println!("Database: {:?}", db_path);
    if let Some(p) = pattern {
        println!("Pattern: {}", p);
    }
    println!();

    let db = RocksMap::<String, String>::open(db_path)?;

    let mut matched_keys = 0;
    let mut total_keys = 0;
    let mut key_size_buckets = HashMap::new();
    let mut value_size_buckets = HashMap::new();

    for result in db.iter()? {
        let (key, value) = result?;
        total_keys += 1;

        let matches = if let Some(pat) = pattern {
            key.contains(pat)
        } else {
            true
        };

        if matches {
            matched_keys += 1;
            println!(
                "🔑 {}: {}",
                key,
                if value.len() > 50 {
                    format!("{}...", &value[..47])
                } else {
                    value.clone()
                }
            );

            if show_key_sizes {
                let bucket = (key.len() / 10) * 10;
                *key_size_buckets.entry(bucket).or_insert(0) += 1;
            }

            if show_value_sizes {
                let bucket = (value.len() / 100) * 100;
                *value_size_buckets.entry(bucket).or_insert(0) += 1;
            }
        }
    }

    println!("\n📊 Scan Results:");
    println!("  Total keys: {}", total_keys);
    println!("  Matched keys: {}", matched_keys);

    if show_key_sizes && !key_size_buckets.is_empty() {
        println!("\n📏 Key Size Distribution:");
        let mut sorted: Vec<_> = key_size_buckets.iter().collect();
        sorted.sort_by_key(|&(size, _)| size);
        for (size, count) in sorted {
            println!("  {}-{} chars: {} keys", size, size + 9, count);
        }
    }

    if show_value_sizes && !value_size_buckets.is_empty() {
        println!("\n📏 Value Size Distribution:");
        let mut sorted: Vec<_> = value_size_buckets.iter().collect();
        sorted.sort_by_key(|&(size, _)| size);
        for (size, count) in sorted {
            println!("  {}-{} chars: {} values", size, size + 99, count);
        }
    }

    Ok(())
}

fn benchmark_database(db_path: &PathBuf, operations: usize, op_type: &str) -> Result<()> {
    println!("Database Benchmark");
    println!("==================");
    println!("Database: {:?}", db_path);
    println!("Operations: {}", operations);
    println!("Operation type: {}", op_type);
    println!();

    let start = Instant::now();
    let db = RocksMap::<String, String>::open(db_path)?;

    let mut read_times = Vec::new();
    let mut write_times = Vec::new();
    let mut delete_times = Vec::new();

    match op_type {
        "read" => {
            println!("📖 Performing read benchmark...");
            let mut existing_keys = Vec::new();

            for result in db.iter()? {
                let (key, _) = result?;
                existing_keys.push(key);
            }

            if existing_keys.is_empty() {
                println!("No existing keys found, adding test data...");
                for i in 0..100 {
                    let key = format!("bench_key_{}", i);
                    let value = format!("bench_value_{}", i);
                    db.put(key.clone(), &value)?;
                    existing_keys.push(key);
                }
            }

            for _ in 0..operations {
                if existing_keys.is_empty() {
                    break;
                }

                let key_idx = rand::random::<usize>() % existing_keys.len();
                let key = &existing_keys[key_idx];

                let read_start = Instant::now();
                let _ = db.get(key)?;
                read_times.push(read_start.elapsed().as_micros());
            }
        }
        "write" => {
            println!("✏️ Performing write benchmark...");
            for i in 0..operations {
                let key = format!("bench_key_{}", i);
                let value = format!("bench_value_{}", i);

                let write_start = Instant::now();
                db.put(key, &value)?;
                write_times.push(write_start.elapsed().as_micros());
            }
        }
        "delete" => {
            println!("🗑️ Performing delete benchmark...");
            let keys: Vec<_> = (0..operations)
                .map(|i| format!("bench_delete_key_{}", i))
                .collect();

            for i in 0..operations {
                let key = &keys[i];
                let value = format!("bench_value_{}", i);
                db.put(key.clone(), &value)?;
            }

            for key in &keys {
                let delete_start = Instant::now();
                db.delete(key)?;
                delete_times.push(delete_start.elapsed().as_micros());
            }
        }
        _ => {
            // "mixed"
            println!("🔄 Performing mixed benchmark...");
            let mut keys = Vec::new();

            // Do 50% writes first
            let write_ops = operations / 2;
            for i in 0..write_ops {
                let key = format!("bench_mixed_key_{}", i);
                let value = format!("bench_value_{}", i);

                let write_start = Instant::now();
                db.put(key.clone(), &value)?;
                write_times.push(write_start.elapsed().as_micros());
                keys.push(key);
            }

            // Then 30% reads
            let read_ops = operations * 3 / 10;
            for _ in 0..read_ops {
                if keys.is_empty() {
                    break;
                }

                let key_idx = rand::random::<usize>() % keys.len();
                let key = &keys[key_idx];

                let read_start = Instant::now();
                let _ = db.get(key)?;
                read_times.push(read_start.elapsed().as_micros());
            }

            // Finally 20% deletes
            let delete_ops = operations - write_ops - read_ops;
            for _ in 0..delete_ops {
                if keys.is_empty() {
                    break;
                }

                let key_idx = rand::random::<usize>() % keys.len();
                let key = keys.swap_remove(key_idx);

                let delete_start = Instant::now();
                db.delete(&key)?;
                delete_times.push(delete_start.elapsed().as_micros());
            }
        }
    }

    let elapsed = start.elapsed();
    println!("\n⏱️ Benchmark Results:");
    println!("  Total time: {:.2} seconds", elapsed.as_secs_f64());
    println!(
        "  Operations per second: {:.2}",
        operations as f64 / elapsed.as_secs_f64()
    );

    fn print_latency_stats(times: &[u128], op_name: &str) {
        if times.is_empty() {
            return;
        }

        let mut times_sorted = times.to_vec();
        times_sorted.sort();

        let total: u128 = times.iter().sum();
        let avg = total as f64 / times.len() as f64;
        let p50 = times_sorted[times.len() / 2];
        let p95 = times_sorted[(times.len() * 95) / 100];
        let p99 = times_sorted[(times.len() * 99) / 100];

        println!("\n  {} Latencies (microseconds):", op_name);
        println!("    Avg: {:.2}", avg);
        println!("    p50: {}", p50);
        println!("    p95: {}", p95);
        println!("    p99: {}", p99);
    }

    print_latency_stats(&read_times, "Read");
    print_latency_stats(&write_times, "Write");
    print_latency_stats(&delete_times, "Delete");

    Ok(())
}
