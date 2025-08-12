use anyhow::{Context, Result};
use rocksmap::RocksMap;
use serde_json::Value;
use std::collections::HashMap;
use std::fs::File;
use std::io::Write;
use std::path::Path;

use crate::{ExportCommands, OutputFormat};

/// Execute export commands
pub fn export_command(
    db_path: &Path,
    command: ExportCommands,
    format: &OutputFormat,
) -> Result<()> {
    match command {
        ExportCommands::Json { file, cf } => export_to_json(db_path, &file, cf.as_deref(), format),
        ExportCommands::Csv { file, cf } => export_to_csv(db_path, &file, cf.as_deref(), format),
    }
}

/// Export data to JSON file
fn export_to_json(
    db_path: &Path,
    file_path: &Path,
    cf: Option<&str>,
    format: &OutputFormat,
) -> Result<()> {
    let mut db = RocksMap::<String, String>::open(db_path).context("Failed to open database")?;

    let mut data = HashMap::new();
    let mut exported_count = 0;
    let mut errors = Vec::new();

    match cf {
        Some(cf_name) => {
            let cf_ref = db
                .column_family(cf_name)
                .context("Failed to get column family")?;
            let iterator = cf_ref.iter().context("Failed to create iterator")?;

            for result in iterator {
                match result {
                    Ok((key, value)) => {
                        data.insert(key, value);
                        exported_count += 1;
                    }
                    Err(e) => {
                        errors.push(format!("Failed to read key-value pair: {}", e));
                    }
                }
            }
        }
        None => {
            let iterator = db.iter().context("Failed to create iterator")?;

            for result in iterator {
                match result {
                    Ok((key, value)) => {
                        data.insert(key, value);
                        exported_count += 1;
                    }
                    Err(e) => {
                        errors.push(format!("Failed to read key-value pair: {}", e));
                    }
                }
            }
        }
    }

    let file = File::create(file_path).context("Failed to create JSON file")?;

    serde_json::to_writer_pretty(file, &data).context("Failed to write JSON data")?;

    match format {
        OutputFormat::Json => {
            let result = serde_json::json!({
                "status": if errors.is_empty() { "success" } else { "partial_success" },
                "operation": "export_json",
                "file": file_path,
                "column_family": cf,
                "exported_count": exported_count,
                "error_count": errors.len(),
                "errors": errors
            });
            println!("{}", serde_json::to_string_pretty(&result)?);
        }
        OutputFormat::Table => {
            println!("JSON Export Results");
            println!("==================");
            println!("File: {:?}", file_path);
            println!("Exported: {} records", exported_count);

            if let Some(cf_name) = cf {
                println!("Column family: {}", cf_name);
            }

            if !errors.is_empty() {
                println!("Errors: {}", errors.len());
                for error in &errors {
                    println!("  - {}", error);
                }
            } else {
                println!("✓ All records exported successfully");
            }
        }
        OutputFormat::Csv => {
            println!("operation,file,column_family,exported_count,error_count");
            println!(
                "export_json,{:?},{},{},{}",
                file_path,
                cf.unwrap_or("default"),
                exported_count,
                errors.len()
            );
        }
    }

    Ok(())
}

/// Export data to CSV file
fn export_to_csv(
    db_path: &Path,
    file_path: &Path,
    cf: Option<&str>,
    format: &OutputFormat,
) -> Result<()> {
    let mut db = RocksMap::<String, String>::open(db_path).context("Failed to open database")?;

    let file = File::create(file_path).context("Failed to create CSV file")?;

    let mut writer = csv::Writer::from_writer(file);

    writer
        .write_record(&["key", "value"])
        .context("Failed to write CSV header")?;

    let mut exported_count = 0;
    let mut errors = Vec::new();

    match cf {
        Some(cf_name) => {
            let cf_ref = db
                .column_family(cf_name)
                .context("Failed to get column family")?;
            let iterator = cf_ref.iter().context("Failed to create iterator")?;

            for result in iterator {
                match result {
                    Ok((key, value)) => match writer.write_record(&[&key, &value]) {
                        Ok(_) => exported_count += 1,
                        Err(e) => {
                            errors.push(format!("Failed to write record for key '{}': {}", key, e))
                        }
                    },
                    Err(e) => {
                        errors.push(format!("Failed to read key-value pair: {}", e));
                    }
                }
            }
        }
        None => {
            let iterator = db.iter().context("Failed to create iterator")?;

            for result in iterator {
                match result {
                    Ok((key, value)) => match writer.write_record(&[&key, &value]) {
                        Ok(_) => exported_count += 1,
                        Err(e) => {
                            errors.push(format!("Failed to write record for key '{}': {}", key, e))
                        }
                    },
                    Err(e) => {
                        errors.push(format!("Failed to read key-value pair: {}", e));
                    }
                }
            }
        }
    }

    writer.flush().context("Failed to flush CSV writer")?;

    match format {
        OutputFormat::Json => {
            let result = serde_json::json!({
                "status": if errors.is_empty() { "success" } else { "partial_success" },
                "operation": "export_csv",
                "file": file_path,
                "column_family": cf,
                "exported_count": exported_count,
                "error_count": errors.len(),
                "errors": errors
            });
            println!("{}", serde_json::to_string_pretty(&result)?);
        }
        OutputFormat::Table => {
            println!("CSV Export Results");
            println!("=================");
            println!("File: {:?}", file_path);
            println!("Exported: {} records", exported_count);

            if let Some(cf_name) = cf {
                println!("Column family: {}", cf_name);
            }

            if !errors.is_empty() {
                println!("Errors: {}", errors.len());
                for error in &errors {
                    println!("  - {}", error);
                }
            } else {
                println!("✓ All records exported successfully");
            }
        }
        OutputFormat::Csv => {
            println!("operation,file,column_family,exported_count,error_count");
            println!(
                "export_csv,{:?},{},{},{}",
                file_path,
                cf.unwrap_or("default"),
                exported_count,
                errors.len()
            );
        }
    }

    Ok(())
}
