use anyhow::{Context, Result};
use rocksmap::RocksMap;
use std::path::Path;

use crate::OutputFormat;

mod admin;
mod diag;
mod export;
mod import;
mod shell;

pub use admin::*;
pub use diag::*;
pub use export::*;
pub use import::*;
pub use shell::*;

/// Put a key-value pair into the database
pub fn put_command(
    db_path: &Path,
    key: &str,
    value: &str,
    cf: Option<&str>,
    format: &OutputFormat,
) -> Result<()> {
    let mut db = RocksMap::<String, String>::open(db_path).context("Failed to open database")?;

    match cf {
        Some(cf_name) => {
            let cf_ref = db
                .column_family(cf_name)
                .context("Failed to get column family")?;
            cf_ref
                .put(&key.to_string(), &value.to_string())
                .context("Failed to put value")?;
        }
        None => {
            db.put(key.to_string(), &value.to_string())
                .context("Failed to put value")?;
        }
    }

    match format {
        OutputFormat::Json => {
            let result = serde_json::json!({
                "status": "success",
                "operation": "put",
                "key": key,
                "value": value,
                "column_family": cf
            });
            println!("{}", serde_json::to_string_pretty(&result)?);
        }
        OutputFormat::Table => {
            println!("✓ Successfully stored key '{}' with value '{}'", key, value);
            if let Some(cf_name) = cf {
                println!("  Column family: {}", cf_name);
            }
        }
        OutputFormat::Csv => {
            println!("operation,key,value,column_family,status");
            println!("put,{},{},{},success", key, value, cf.unwrap_or("default"));
        }
    }

    Ok(())
}

/// Get a value by key from the database
pub fn get_command(
    db_path: &Path,
    key: &str,
    cf: Option<&str>,
    format: &OutputFormat,
) -> Result<()> {
    let mut db = RocksMap::<String, String>::open(db_path).context("Failed to open database")?;

    let value = match cf {
        Some(cf_name) => {
            let cf_ref = db
                .column_family(cf_name)
                .context("Failed to get column family")?;
            cf_ref
                .get(&key.to_string())
                .context("Failed to get value")?
        }
        None => db.get(&key.to_string()).context("Failed to get value")?,
    };

    match format {
        OutputFormat::Json => {
            let result = serde_json::json!({
                "key": key,
                "value": value,
                "column_family": cf,
                "found": value.is_some()
            });
            println!("{}", serde_json::to_string_pretty(&result)?);
        }
        OutputFormat::Table => match value {
            Some(v) => {
                println!("Key: {}", key);
                println!("Value: {}", v);
                if let Some(cf_name) = cf {
                    println!("Column family: {}", cf_name);
                }
            }
            None => {
                println!("Key '{}' not found", key);
            }
        },
        OutputFormat::Csv => {
            println!("key,value,column_family,found");
            let found = value.is_some();
            println!(
                "{},{},{},{}",
                key,
                value.unwrap_or_default(),
                cf.unwrap_or("default"),
                found
            );
        }
    }

    Ok(())
}

/// Delete a key from the database
pub fn delete_command(
    db_path: &Path,
    key: &str,
    cf: Option<&str>,
    format: &OutputFormat,
) -> Result<()> {
    let mut db = RocksMap::<String, String>::open(db_path).context("Failed to open database")?;

    match cf {
        Some(cf_name) => {
            let cf_ref = db
                .column_family(cf_name)
                .context("Failed to get column family")?;
            cf_ref
                .delete(&key.to_string())
                .context("Failed to delete key")?;
        }
        None => {
            db.delete(&key.to_string())
                .context("Failed to delete key")?;
        }
    }

    match format {
        OutputFormat::Json => {
            let result = serde_json::json!({
                "status": "success",
                "operation": "delete",
                "key": key,
                "column_family": cf
            });
            println!("{}", serde_json::to_string_pretty(&result)?);
        }
        OutputFormat::Table => {
            println!("✓ Successfully deleted key '{}'", key);
            if let Some(cf_name) = cf {
                println!("  Column family: {}", cf_name);
            }
        }
        OutputFormat::Csv => {
            println!("operation,key,column_family,status");
            println!("delete,{},{},success", key, cf.unwrap_or("default"));
        }
    }

    Ok(())
}

/// List all keys in the database
pub fn list_command(
    db_path: &Path,
    cf: Option<&str>,
    limit: Option<usize>,
    prefix: Option<&str>,
    format: &OutputFormat,
) -> Result<()> {
    let mut db = RocksMap::<String, String>::open(db_path).context("Failed to open database")?;

    let mut results = Vec::new();
    let mut count = 0;

    match (cf, prefix) {
        (Some(cf_name), Some(prefix_str)) => {
            let cf_ref = db
                .column_family(cf_name)
                .context("Failed to get column family")?;
            let iterator = cf_ref
                .prefix_scan(&prefix_str.to_string())
                .context("Failed to create prefix iterator")?;

            for result in iterator {
                let (key, value) = result?;
                results.push((key, value));
                count += 1;
            }
        }
        (Some(cf_name), None) => {
            let cf_ref = db
                .column_family(cf_name)
                .context("Failed to get column family")?;
            let iterator = cf_ref.iter().context("Failed to create iterator")?;

            for result in iterator {
                let (key, value) = result?;
                results.push((key, value));
                count += 1;
            }
        }
        (None, Some(prefix_str)) => {
            let iterator = db
                .prefix_scan(&prefix_str.to_string())
                .context("Failed to create prefix iterator")?;

            for result in iterator {
                let (key, value) = result?;
                results.push((key, value));
                count += 1;
            }
        }
        (None, None) => {
            let iterator = db.iter().context("Failed to create iterator")?;

            for result in iterator {
                let (key, value) = result?;
                results.push((key, value));
                count += 1;
            }
        }
    }

    match format {
        OutputFormat::Json => {
            let json_results: Vec<_> = results
                .iter()
                .map(|(k, v)| {
                    serde_json::json!({
                        "key": k,
                        "value": v
                    })
                })
                .collect();

            let output = serde_json::json!({
                "results": json_results,
                "count": count,
                "column_family": cf,
                "prefix": prefix,
                "limit": limit
            });
            println!("{}", serde_json::to_string_pretty(&output)?);
        }
        OutputFormat::Table => {
            if results.is_empty() {
                println!("No keys found");
            } else {
                println!("Found {} key(s):", count);
                println!("{:<20} | {}", "Key", "Value");
                println!("{:-<20}-+-{:-<40}", "", "");
                for (key, value) in &results {
                    let truncated_value = if value.len() > 40 {
                        format!("{}...", &value[..37])
                    } else {
                        value.clone()
                    };
                    println!("{:<20} | {}", key, truncated_value);
                }
            }

            if let Some(cf_name) = cf {
                println!("Column family: {}", cf_name);
            }
            if let Some(prefix_str) = prefix {
                println!("Prefix filter: {}", prefix_str);
            }
        }
        OutputFormat::Csv => {
            println!("key,value");
            for (key, value) in &results {
                println!("{},{}", key, value);
            }
        }
    }

    Ok(())
}

/// Scan a range of keys
pub fn scan_command(
    db_path: &Path,
    from: &str,
    to: &str,
    cf: Option<&str>,
    format: &OutputFormat,
) -> Result<()> {
    let mut db = RocksMap::<String, String>::open(db_path).context("Failed to open database")?;

    let mut results = Vec::new();
    let mut count = 0;

    match cf {
        Some(cf_name) => {
            let cf_ref = db
                .column_family(cf_name)
                .context("Failed to get column family")?;
            let iterator = cf_ref
                .range(&from.to_string(), &to.to_string())
                .context("Failed to create range iterator")?;

            for result in iterator {
                let (key, value) = result?;
                results.push((key, value));
                count += 1;
            }
        }
        None => {
            let iterator = db
                .range(&from.to_string(), &to.to_string())
                .context("Failed to create range iterator")?;

            for result in iterator {
                let (key, value) = result?;
                results.push((key, value));
                count += 1;
            }
        }
    }

    match format {
        OutputFormat::Json => {
            let json_results: Vec<_> = results
                .iter()
                .map(|(k, v)| {
                    serde_json::json!({
                        "key": k,
                        "value": v
                    })
                })
                .collect();

            let output = serde_json::json!({
                "results": json_results,
                "count": count,
                "range": {
                    "from": from,
                    "to": to
                },
                "column_family": cf
            });
            println!("{}", serde_json::to_string_pretty(&output)?);
        }
        OutputFormat::Table => {
            if results.is_empty() {
                println!("No keys found in range [{}, {}]", from, to);
            } else {
                println!("Found {} key(s) in range [{}, {}]:", count, from, to);
                println!("{:<20} | {}", "Key", "Value");
                println!("{:-<20}-+-{:-<40}", "", "");
                for (key, value) in &results {
                    let truncated_value = if value.len() > 40 {
                        format!("{}...", &value[..37])
                    } else {
                        value.clone()
                    };
                    println!("{:<20} | {}", key, truncated_value);
                }
            }

            if let Some(cf_name) = cf {
                println!("Column family: {}", cf_name);
            }
        }
        OutputFormat::Csv => {
            println!("key,value");
            for (key, value) in &results {
                println!("{},{}", key, value);
            }
        }
    }

    Ok(())
}
