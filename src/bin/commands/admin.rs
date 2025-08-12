use anyhow::{Context, Result};
use rocksdb::{Options, DB};
use rocksmap::RocksMap;
use std::fs;
use std::path::Path;

use crate::{AdminCommands, OutputFormat};

/// Execute admin commands
pub fn admin_command(db_path: &Path, command: AdminCommands, format: &OutputFormat) -> Result<()> {
    match command {
        AdminCommands::Compact { cf } => compact_database(db_path, cf.as_deref(), format),
        AdminCommands::Stats => show_database_stats(db_path, format),
        AdminCommands::Check => check_database_integrity(db_path, format),
        AdminCommands::Backup { path } => backup_database(db_path, &path, format),
        AdminCommands::CreateCf { name } => create_column_family(db_path, &name, format),
        AdminCommands::ListCf => list_column_families(db_path, format),
    }
}

/// Compact the database
fn compact_database(db_path: &Path, cf: Option<&str>, format: &OutputFormat) -> Result<()> {
    let db = DB::open_default(db_path).context("Failed to open database")?;

    match cf {
        Some(cf_name) => {
            let cf_handle = db
                .cf_handle(cf_name)
                .ok_or_else(|| anyhow::anyhow!("Column family '{}' not found", cf_name))?;
            db.compact_range_cf(cf_handle, None::<&[u8]>, None::<&[u8]>);
        }
        None => {
            db.compact_range(None::<&[u8]>, None::<&[u8]>);
        }
    }

    match format {
        OutputFormat::Json => {
            let result = serde_json::json!({
                "status": "success",
                "operation": "compact",
                "column_family": cf,
                "message": "Database compaction completed"
            });
            println!("{}", serde_json::to_string_pretty(&result)?);
        }
        OutputFormat::Table => {
            println!("✓ Database compaction completed successfully");
            if let Some(cf_name) = cf {
                println!("  Column family: {}", cf_name);
            }
        }
        OutputFormat::Csv => {
            println!("operation,column_family,status");
            println!("compact,{},success", cf.unwrap_or("default"));
        }
    }

    Ok(())
}

/// Show database statistics
fn show_database_stats(db_path: &Path, format: &OutputFormat) -> Result<()> {
    let db = DB::open_default(db_path).context("Failed to open database")?;

    let num_keys = db
        .property_value("rocksdb.estimate-num-keys")
        .unwrap_or_else(|_| Some("unknown".to_string()))
        .unwrap_or_else(|| "unknown".to_string());

    let db_size = db
        .property_value("rocksdb.total-sst-files-size")
        .unwrap_or_else(|_| Some("unknown".to_string()))
        .unwrap_or_else(|| "unknown".to_string());

    let mem_usage = db
        .property_value("rocksdb.cur-size-all-mem-tables")
        .unwrap_or_else(|_| Some("unknown".to_string()))
        .unwrap_or_else(|| "unknown".to_string());

    let num_files = db
        .property_value("rocksdb.num-files-at-level0")
        .unwrap_or_else(|_| Some("unknown".to_string()))
        .unwrap_or_else(|| "unknown".to_string());

    let disk_usage = match fs::metadata(db_path) {
        Ok(metadata) => metadata.len().to_string(),
        Err(_) => "unknown".to_string(),
    };

    match format {
        OutputFormat::Json => {
            let stats = serde_json::json!({
                "database_path": db_path,
                "estimated_keys": num_keys,
                "sst_files_size_bytes": db_size,
                "memtable_size_bytes": mem_usage,
                "level0_files": num_files,
                "disk_usage_bytes": disk_usage
            });
            println!("{}", serde_json::to_string_pretty(&stats)?);
        }
        OutputFormat::Table => {
            println!("Database Statistics");
            println!("==================");
            println!("Path: {:?}", db_path);
            println!("Estimated keys: {}", num_keys);
            println!("SST files size: {} bytes", db_size);
            println!("Memtable size: {} bytes", mem_usage);
            println!("Level 0 files: {}", num_files);
            println!("Disk usage: {} bytes", disk_usage);
        }
        OutputFormat::Csv => {
            println!("metric,value");
            println!("database_path,{:?}", db_path);
            println!("estimated_keys,{}", num_keys);
            println!("sst_files_size_bytes,{}", db_size);
            println!("memtable_size_bytes,{}", mem_usage);
            println!("level0_files,{}", num_files);
            println!("disk_usage_bytes,{}", disk_usage);
        }
    }

    Ok(())
}

/// Check database integrity
fn check_database_integrity(db_path: &Path, format: &OutputFormat) -> Result<()> {
    let db = RocksMap::<String, String>::open(db_path)
        .context("Failed to open database for integrity check")?;

    let mut key_count = 0;
    let mut errors = Vec::new();

    match db.iter() {
        Ok(iterator) => {
            for result in iterator {
                match result {
                    Ok(_) => key_count += 1,
                    Err(e) => errors.push(format!("Iterator error: {}", e)),
                }
            }
        }
        Err(e) => errors.push(format!("Failed to create iterator: {}", e)),
    }

    let is_healthy = errors.is_empty();

    match format {
        OutputFormat::Json => {
            let result = serde_json::json!({
                "database_path": db_path,
                "healthy": is_healthy,
                "keys_checked": key_count,
                "errors": errors
            });
            println!("{}", serde_json::to_string_pretty(&result)?);
        }
        OutputFormat::Table => {
            println!("Database Integrity Check");
            println!("=======================");
            println!("Path: {:?}", db_path);
            println!(
                "Status: {}",
                if is_healthy {
                    "✓ Healthy"
                } else {
                    "✗ Issues found"
                }
            );
            println!("Keys checked: {}", key_count);

            if !errors.is_empty() {
                println!("\nErrors found:");
                for error in &errors {
                    println!("  - {}", error);
                }
            }
        }
        OutputFormat::Csv => {
            println!("database_path,healthy,keys_checked,error_count");
            println!(
                "{:?},{},{},{}",
                db_path,
                is_healthy,
                key_count,
                errors.len()
            );
        }
    }

    Ok(())
}

/// Backup the database
fn backup_database(db_path: &Path, backup_path: &Path, format: &OutputFormat) -> Result<()> {
    if backup_path.exists() {
        return Err(anyhow::anyhow!(
            "Backup path already exists: {:?}",
            backup_path
        ));
    }
    fs::create_dir_all(backup_path.parent().unwrap_or(backup_path))
        .context("Failed to create backup directory")?;

    copy_dir_recursive(db_path, backup_path).context("Failed to copy database files")?;

    match format {
        OutputFormat::Json => {
            let result = serde_json::json!({
                "status": "success",
                "operation": "backup",
                "source": db_path,
                "destination": backup_path,
                "message": "Database backup completed"
            });
            println!("{}", serde_json::to_string_pretty(&result)?);
        }
        OutputFormat::Table => {
            println!("✓ Database backup completed successfully");
            println!("  Source: {:?}", db_path);
            println!("  Destination: {:?}", backup_path);
        }
        OutputFormat::Csv => {
            println!("operation,source,destination,status");
            println!("backup,{:?},{:?},success", db_path, backup_path);
        }
    }

    Ok(())
}

/// Create a column family
fn create_column_family(db_path: &Path, name: &str, format: &OutputFormat) -> Result<()> {
    let mut db = RocksMap::<String, String>::open(db_path).context("Failed to open database")?;

    db.column_family(name)
        .context("Failed to create column family")?;

    match format {
        OutputFormat::Json => {
            let result = serde_json::json!({
                "status": "success",
                "operation": "create_column_family",
                "name": name,
                "message": "Column family created successfully"
            });
            println!("{}", serde_json::to_string_pretty(&result)?);
        }
        OutputFormat::Table => {
            println!("✓ Column family '{}' created successfully", name);
        }
        OutputFormat::Csv => {
            println!("operation,name,status");
            println!("create_column_family,{},success", name);
        }
    }

    Ok(())
}

/// List column families
fn list_column_families(db_path: &Path, format: &OutputFormat) -> Result<()> {
    let cf_names =
        DB::list_cf(&Options::default(), db_path).context("Failed to list column families")?;

    match format {
        OutputFormat::Json => {
            let result = serde_json::json!({
                "column_families": cf_names,
                "count": cf_names.len()
            });
            println!("{}", serde_json::to_string_pretty(&result)?);
        }
        OutputFormat::Table => {
            if cf_names.is_empty() {
                println!("No column families found");
            } else {
                println!("Column Families ({}):", cf_names.len());
                for (i, name) in cf_names.iter().enumerate() {
                    println!("  {}. {}", i + 1, name);
                }
            }
        }
        OutputFormat::Csv => {
            println!("name");
            for name in &cf_names {
                println!("{}", name);
            }
        }
    }

    Ok(())
}

/// Recursively copy a directory
fn copy_dir_recursive(src: &Path, dst: &Path) -> Result<()> {
    if !src.is_dir() {
        return Err(anyhow::anyhow!("Source is not a directory: {:?}", src));
    }

    fs::create_dir_all(dst)?;

    for entry in fs::read_dir(src)? {
        let entry = entry?;
        let src_path = entry.path();
        let dst_path = dst.join(entry.file_name());

        if src_path.is_dir() {
            copy_dir_recursive(&src_path, &dst_path)?;
        } else {
            fs::copy(&src_path, &dst_path)?;
        }
    }

    Ok(())
}
