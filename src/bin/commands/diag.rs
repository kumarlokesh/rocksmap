use anyhow::{Context, Result};
use rocksdb::{Options, DB};
use rocksmap::RocksMap;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::time::Instant;

use crate::{DiagCommands, OutputFormat};

/// Handle diagnostic commands
pub fn diag_command(db_path: &Path, command: DiagCommands, format: &OutputFormat) -> Result<()> {
    match command {
        DiagCommands::Analyze => analyze_database(db_path, format),
        DiagCommands::Check => check_integrity(db_path, format),
        DiagCommands::Stats => show_detailed_stats(db_path, format),
        DiagCommands::Scan {
            pattern,
            key_sizes,
            value_sizes,
        } => scan_keyspace(db_path, pattern.as_deref(), key_sizes, value_sizes, format),
        DiagCommands::Benchmark {
            operations,
            op_type,
        } => benchmark_database(db_path, operations, &op_type, format),
    }
}

/// Analyze key distribution and patterns
fn analyze_database(db_path: &Path, format: &OutputFormat) -> Result<()> {
    let db = RocksMap::<String, String>::open(db_path).context("Failed to open database")?;

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

    match format {
        OutputFormat::Json => {
            let mut prefix_analysis = Vec::new();
            let mut sorted_prefixes: Vec<_> = key_prefixes.iter().collect();
            sorted_prefixes.sort_by(|a, b| b.1.cmp(a.1));

            for (prefix, count) in sorted_prefixes.iter().take(10) {
                let percentage = (**count as f64 / total_keys as f64) * 100.0;
                prefix_analysis.push(serde_json::json!({
                    "prefix": prefix,
                    "count": count,
                    "percentage": percentage
                }));
            }

            let result = serde_json::json!({
                "statistics": {
                    "total_keys": total_keys,
                    "total_key_bytes": total_key_bytes,
                    "total_value_bytes": total_value_bytes,
                    "average_key_length": if total_keys > 0 { total_key_bytes as f64 / total_keys as f64 } else { 0.0 },
                    "average_value_length": if total_keys > 0 { total_value_bytes as f64 / total_keys as f64 } else { 0.0 }
                },
                "key_length_distribution": {
                    "min": key_lengths.first(),
                    "max": key_lengths.last(),
                    "median": key_lengths.get(key_lengths.len() / 2)
                },
                "value_length_distribution": {
                    "min": value_lengths.first(),
                    "max": value_lengths.last(),
                    "median": value_lengths.get(value_lengths.len() / 2)
                },
                "prefix_analysis": prefix_analysis
            });
            println!("{}", serde_json::to_string_pretty(&result)?);
        }
        OutputFormat::Csv => {
            println!("metric,value");
            println!("total_keys,{}", total_keys);
            println!("total_key_bytes,{}", total_key_bytes);
            println!("total_value_bytes,{}", total_value_bytes);
            println!(
                "avg_key_length,{:.1}",
                if total_keys > 0 {
                    total_key_bytes as f64 / total_keys as f64
                } else {
                    0.0
                }
            );
            println!(
                "avg_value_length,{:.1}",
                if total_keys > 0 {
                    total_value_bytes as f64 / total_keys as f64
                } else {
                    0.0
                }
            );

            if !key_lengths.is_empty() {
                println!("min_key_length,{}", key_lengths[0]);
                println!("max_key_length,{}", key_lengths[key_lengths.len() - 1]);
                println!("median_key_length,{}", key_lengths[key_lengths.len() / 2]);
            }

            if !key_prefixes.is_empty() {
                println!("\nprefix,count,percentage");
                let mut sorted_prefixes: Vec<_> = key_prefixes.iter().collect();
                sorted_prefixes.sort_by(|a, b| b.1.cmp(a.1));

                for (prefix, count) in sorted_prefixes.iter().take(10) {
                    let percentage = (**count as f64 / total_keys as f64) * 100.0;
                    println!("{},{},{:.1}", prefix, count, percentage);
                }
            }
        }
        OutputFormat::Table => {
            println!("Analyzing RocksMap Database");
            println!("==========================");
            println!("Database: {:?}\n", db_path);

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
        }
    }

    Ok(())
}

/// Check database integrity
fn check_integrity(db_path: &Path, format: &OutputFormat) -> Result<()> {
    let db = RocksMap::<String, String>::open(db_path).context("Failed to open database")?;

    let mut total_keys = 0;
    let mut errors = Vec::new();
    let mut empty_keys = 0;
    let mut empty_values = 0;

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

    match format {
        OutputFormat::Json => {
            let result = serde_json::json!({
                "integrity_check": {
                    "total_keys": total_keys,
                    "empty_keys": empty_keys,
                    "empty_values": empty_values,
                    "errors_count": errors.len(),
                    "status": if errors.is_empty() { "healthy" } else { "issues_detected" },
                    "issues": errors
                }
            });
            println!("{}", serde_json::to_string_pretty(&result)?);
        }
        OutputFormat::Csv => {
            println!("metric,value");
            println!("total_keys,{}", total_keys);
            println!("empty_keys,{}", empty_keys);
            println!("empty_values,{}", empty_values);
            println!("errors_count,{}", errors.len());
            println!(
                "status,{}",
                if errors.is_empty() {
                    "healthy"
                } else {
                    "issues_detected"
                }
            );

            if !errors.is_empty() {
                println!("\nissue");
                for error in &errors {
                    println!("{}", error.replace(",", ";")); // Escape commas for CSV
                }
            }
        }
        OutputFormat::Table => {
            println!("Database Integrity Check");
            println!("=======================");
            println!("Database: {:?}\n", db_path);

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
        }
    }

    Ok(())
}

/// Show detailed database statistics
fn show_detailed_stats(db_path: &Path, format: &OutputFormat) -> Result<()> {
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

    let mut stats = HashMap::new();
    for (property, description) in &properties {
        if let Some(value) = db.property_value(*property)? {
            stats.insert(*description, value);
        }
    }

    let cf_names = DB::list_cf(&Options::default(), db_path)?;

    match format {
        OutputFormat::Json => {
            let result = serde_json::json!({
                "database_stats": stats,
                "column_families": cf_names,
            });
            println!("{}", serde_json::to_string_pretty(&result)?);
        }
        OutputFormat::Csv => {
            println!("stat,value");
            for (description, value) in stats {
                println!("{},{}", description, value);
            }

            println!("\ncolumn_family");
            for name in cf_names {
                println!("{}", name);
            }
        }
        OutputFormat::Table => {
            println!("Detailed Database Statistics");
            println!("===========================");
            println!("Database: {:?}\n", db_path);

            println!("🗄️  RocksDB Internal Statistics:");
            for (description, value) in stats {
                println!("  {}: {}", description, value);
            }

            println!("\n📁 Column Families:");
            for (i, name) in cf_names.iter().enumerate() {
                println!("  {}. {}", i + 1, name);
            }
        }
    }

    Ok(())
}

/// Scan keyspace for patterns
fn scan_keyspace(
    db_path: &Path,
    pattern: Option<&str>,
    show_key_sizes: bool,
    show_value_sizes: bool,
    format: &OutputFormat,
) -> Result<()> {
    let db = RocksMap::<String, String>::open(db_path).context("Failed to open database")?;

    let mut matched_keys = 0;
    let mut total_keys = 0;
    let mut key_size_buckets = HashMap::new();
    let mut value_size_buckets = HashMap::new();
    let mut results = Vec::new();

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
            results.push((key.clone(), value.clone()));

            if show_key_sizes {
                let bucket = (key.len() / 10) * 10; // Group by 10s
                *key_size_buckets.entry(bucket).or_insert(0) += 1;
            }

            if show_value_sizes {
                let bucket = (value.len() / 100) * 100; // Group by 100s
                *value_size_buckets.entry(bucket).or_insert(0) += 1;
            }
        }
    }

    match format {
        OutputFormat::Json => {
            let mut key_sizes = Vec::new();
            if show_key_sizes {
                let mut sorted: Vec<_> = key_size_buckets.iter().collect();
                sorted.sort_by_key(|&(size, _)| size);
                for (size, count) in sorted {
                    key_sizes.push(serde_json::json!({
                        "range": {
                            "start": size,
                            "end": size + 9
                        },
                        "count": count
                    }));
                }
            }

            let mut value_sizes = Vec::new();
            if show_value_sizes {
                let mut sorted: Vec<_> = value_size_buckets.iter().collect();
                sorted.sort_by_key(|&(size, _)| size);
                for (size, count) in sorted {
                    value_sizes.push(serde_json::json!({
                        "range": {
                            "start": size,
                            "end": size + 99
                        },
                        "count": count
                    }));
                }
            }

            let json_results: Vec<_> = results
                .iter()
                .map(|(k, v)| {
                    serde_json::json!({
                        "key": k,
                        "value": v,
                        "key_length": k.len(),
                        "value_length": v.len()
                    })
                })
                .collect();

            let result = serde_json::json!({
                "scan_results": {
                    "total_keys": total_keys,
                    "matched_keys": matched_keys,
                    "pattern": pattern,
                },
                "keys": json_results,
                "key_size_distribution": key_sizes,
                "value_size_distribution": value_sizes
            });
            println!("{}", serde_json::to_string_pretty(&result)?);
        }
        OutputFormat::Csv => {
            println!("key,value");
            for (key, value) in &results {
                let escaped_key = key.replace("\"", "\"\"");
                let escaped_value = value.replace("\"", "\"\"");
                println!("\"{}\",\"{}\"", escaped_key, escaped_value);
            }

            if show_key_sizes && !key_size_buckets.is_empty() {
                println!("\nkey_size_range,count");
                let mut sorted: Vec<_> = key_size_buckets.iter().collect();
                sorted.sort_by_key(|&(size, _)| size);
                for (size, count) in sorted {
                    println!("{}-{},{}", size, size + 9, count);
                }
            }

            if show_value_sizes && !value_size_buckets.is_empty() {
                println!("\nvalue_size_range,count");
                let mut sorted: Vec<_> = value_size_buckets.iter().collect();
                sorted.sort_by_key(|&(size, _)| size);
                for (size, count) in sorted {
                    println!("{}-{},{}", size, size + 99, count);
                }
            }
        }
        OutputFormat::Table => {
            println!("Keyspace Scan");
            println!("=============");
            println!("Database: {:?}", db_path);
            if let Some(p) = pattern {
                println!("Pattern: {}", p);
            }
            println!();

            for (key, value) in &results {
                println!(
                    "🔑 {}: {}",
                    key,
                    if value.len() > 50 {
                        format!("{}...", &value[..47])
                    } else {
                        value.clone()
                    }
                );
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
        }
    }

    Ok(())
}

/// Benchmark database performance
fn benchmark_database(
    db_path: &Path,
    operations: usize,
    op_type: &str,
    format: &OutputFormat,
) -> Result<()> {
    let start = Instant::now();
    let db = RocksMap::<String, String>::open(db_path).context("Failed to open database")?;

    let mut read_times = Vec::new();
    let mut write_times = Vec::new();
    let mut delete_times = Vec::new();

    match op_type {
        "read" => {
            let mut existing_keys = Vec::new();

            for result in db.iter()? {
                let (key, _) = result?;
                existing_keys.push(key);
            }

            if existing_keys.is_empty() {
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
            for i in 0..operations {
                let key = format!("bench_key_{}", i);
                let value = format!("bench_value_{}", i);

                let write_start = Instant::now();
                db.put(key, &value)?;
                write_times.push(write_start.elapsed().as_micros());
            }
        }
        "delete" => {
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
    let ops_per_sec = operations as f64 / elapsed.as_secs_f64();

    let get_latency_stats = |times: &[u128]| {
        if times.is_empty() {
            return None;
        }

        let mut times_sorted = times.to_vec();
        times_sorted.sort();

        let total: u128 = times.iter().sum();
        let avg = total as f64 / times.len() as f64;

        let p50 = if !times_sorted.is_empty() {
            times_sorted[times.len() / 2]
        } else {
            0
        };
        let p95 = if !times_sorted.is_empty() {
            times_sorted[(times.len() * 95) / 100]
        } else {
            0
        };
        let p99 = if !times_sorted.is_empty() {
            times_sorted[(times.len() * 99) / 100]
        } else {
            0
        };

        Some((avg, p50, p95, p99))
    };

    match format {
        OutputFormat::Json => {
            let mut result = serde_json::json!({
                "benchmark": {
                    "operation_type": op_type,
                    "operations": operations,
                    "total_time_seconds": elapsed.as_secs_f64(),
                    "operations_per_second": ops_per_sec,
                }
            });

            if let Some((avg, p50, p95, p99)) = get_latency_stats(&read_times) {
                result["benchmark"]["read_latency"] = serde_json::json!({
                    "avg_micros": avg,
                    "p50_micros": p50,
                    "p95_micros": p95,
                    "p99_micros": p99
                });
            }

            if let Some((avg, p50, p95, p99)) = get_latency_stats(&write_times) {
                result["benchmark"]["write_latency"] = serde_json::json!({
                    "avg_micros": avg,
                    "p50_micros": p50,
                    "p95_micros": p95,
                    "p99_micros": p99
                });
            }

            if let Some((avg, p50, p95, p99)) = get_latency_stats(&delete_times) {
                result["benchmark"]["delete_latency"] = serde_json::json!({
                    "avg_micros": avg,
                    "p50_micros": p50,
                    "p95_micros": p95,
                    "p99_micros": p99
                });
            }

            println!("{}", serde_json::to_string_pretty(&result)?);
        }
        OutputFormat::Csv => {
            println!("metric,value");
            println!("operation_type,{}", op_type);
            println!("operations,{}", operations);
            println!("total_time_seconds,{:.4}", elapsed.as_secs_f64());
            println!("operations_per_second,{:.2}", ops_per_sec);

            if let Some((avg, p50, p95, p99)) = get_latency_stats(&read_times) {
                println!("read_avg_micros,{:.2}", avg);
                println!("read_p50_micros,{}", p50);
                println!("read_p95_micros,{}", p95);
                println!("read_p99_micros,{}", p99);
            }

            if let Some((avg, p50, p95, p99)) = get_latency_stats(&write_times) {
                println!("write_avg_micros,{:.2}", avg);
                println!("write_p50_micros,{}", p50);
                println!("write_p95_micros,{}", p95);
                println!("write_p99_micros,{}", p99);
            }

            if let Some((avg, p50, p95, p99)) = get_latency_stats(&delete_times) {
                println!("delete_avg_micros,{:.2}", avg);
                println!("delete_p50_micros,{}", p50);
                println!("delete_p95_micros,{}", p95);
                println!("delete_p99_micros,{}", p99);
            }
        }
        OutputFormat::Table => {
            println!("Database Benchmark");
            println!("==================");
            println!("Database: {:?}", db_path);
            println!("Operations: {}", operations);
            println!("Operation type: {}", op_type);
            println!();

            println!("\n⏱️ Benchmark Results:");
            println!("  Total time: {:.2} seconds", elapsed.as_secs_f64());
            println!("  Operations per second: {:.2}", ops_per_sec);

            if let Some((avg, p50, p95, p99)) = get_latency_stats(&read_times) {
                println!("\n  Read Latencies (microseconds):");
                println!("    Avg: {:.2}", avg);
                println!("    p50: {}", p50);
                println!("    p95: {}", p95);
                println!("    p99: {}", p99);
            }

            if let Some((avg, p50, p95, p99)) = get_latency_stats(&write_times) {
                println!("\n  Write Latencies (microseconds):");
                println!("    Avg: {:.2}", avg);
                println!("    p50: {}", p50);
                println!("    p95: {}", p95);
                println!("    p99: {}", p99);
            }

            if let Some((avg, p50, p95, p99)) = get_latency_stats(&delete_times) {
                println!("\n  Delete Latencies (microseconds):");
                println!("    Avg: {:.2}", avg);
                println!("    p50: {}", p50);
                println!("    p95: {}", p95);
                println!("    p99: {}", p99);
            }
        }
    }

    Ok(())
}
