use anyhow::{Context, Result};
use rocksmap::RocksMap;
use serde_json::Value;
use std::collections::HashMap;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::Path;

use crate::{ImportCommands, OutputFormat};

/// Execute import commands
pub fn import_command(
    db_path: &Path,
    command: ImportCommands,
    format: &OutputFormat,
) -> Result<()> {
    match command {
        ImportCommands::Json { file, cf } => {
            import_from_json(db_path, &file, cf.as_deref(), format)
        }
        ImportCommands::Csv {
            file,
            cf,
            key_column,
            value_column,
        } => import_from_csv(
            db_path,
            &file,
            cf.as_deref(),
            &key_column,
            &value_column,
            format,
        ),
    }
}

/// Import data from JSON file
fn import_from_json(
    db_path: &Path,
    file_path: &Path,
    cf: Option<&str>,
    format: &OutputFormat,
) -> Result<()> {
    let mut db = RocksMap::<String, String>::open(db_path).context("Failed to open database")?;

    let file = File::open(file_path).context("Failed to open JSON file")?;

    let reader = BufReader::new(file);
    let json_data: Value = serde_json::from_reader(reader).context("Failed to parse JSON file")?;

    let mut imported_count = 0;
    let mut errors = Vec::new();

    match json_data {
        Value::Object(map) => {
            for (key, value) in map {
                let value_str = match value {
                    Value::String(s) => s,
                    _ => value.to_string(),
                };

                let result = match cf {
                    Some(cf_name) => {
                        let cf_ref = db
                            .column_family(cf_name)
                            .context("Failed to get column family")?;
                        cf_ref.put(&key.to_string(), &value_str)
                    }
                    None => db.put(key.clone(), &value_str),
                };

                match result {
                    Ok(_) => imported_count += 1,
                    Err(e) => errors.push(format!("Failed to import key '{}': {}", key, e)),
                }
            }
        }
        Value::Array(arr) => {
            for (index, item) in arr.iter().enumerate() {
                if let Value::Object(obj) = item {
                    let key = obj
                        .get("key")
                        .and_then(|v| v.as_str())
                        .ok_or_else(|| anyhow::anyhow!("Missing 'key' field in item {}", index))?;

                    let value = obj
                        .get("value")
                        .map(|v| match v {
                            Value::String(s) => s.clone(),
                            _ => v.to_string(),
                        })
                        .ok_or_else(|| {
                            anyhow::anyhow!("Missing 'value' field in item {}", index)
                        })?;

                    let result = match cf {
                        Some(cf_name) => {
                            let cf_ref = db
                                .column_family(cf_name)
                                .context("Failed to get column family")?;
                            cf_ref.put(&key.to_string(), &value)
                        }
                        None => db.put(key.to_string(), &value),
                    };

                    match result {
                        Ok(_) => imported_count += 1,
                        Err(e) => errors.push(format!("Failed to import key '{}': {}", key, e)),
                    }
                } else {
                    errors.push(format!("Invalid item format at index {}", index));
                }
            }
        }
        _ => {
            return Err(anyhow::anyhow!(
                "JSON must be an object or array of objects"
            ));
        }
    }

    match format {
        OutputFormat::Json => {
            let result = serde_json::json!({
                "status": if errors.is_empty() { "success" } else { "partial_success" },
                "operation": "import_json",
                "file": file_path,
                "column_family": cf,
                "imported_count": imported_count,
                "error_count": errors.len(),
                "errors": errors
            });
            println!("{}", serde_json::to_string_pretty(&result)?);
        }
        OutputFormat::Table => {
            println!("JSON Import Results");
            println!("==================");
            println!("File: {:?}", file_path);
            println!("Imported: {} records", imported_count);

            if let Some(cf_name) = cf {
                println!("Column family: {}", cf_name);
            }

            if !errors.is_empty() {
                println!("Errors: {}", errors.len());
                for error in &errors {
                    println!("  - {}", error);
                }
            } else {
                println!("✓ All records imported successfully");
            }
        }
        OutputFormat::Csv => {
            println!("operation,file,column_family,imported_count,error_count");
            println!(
                "import_json,{:?},{},{},{}",
                file_path,
                cf.unwrap_or("default"),
                imported_count,
                errors.len()
            );
        }
    }

    Ok(())
}

/// Import data from CSV file
fn import_from_csv(
    db_path: &Path,
    file_path: &Path,
    cf: Option<&str>,
    key_column: &str,
    value_column: &str,
    format: &OutputFormat,
) -> Result<()> {
    let mut db = RocksMap::<String, String>::open(db_path).context("Failed to open database")?;

    let file = File::open(file_path).context("Failed to open CSV file")?;

    let mut reader = csv::Reader::from_reader(file);
    let headers = reader.headers()?.clone();

    let key_index = headers
        .iter()
        .position(|h| h == key_column)
        .ok_or_else(|| anyhow::anyhow!("Key column '{}' not found in CSV", key_column))?;

    let value_index = headers
        .iter()
        .position(|h| h == value_column)
        .ok_or_else(|| anyhow::anyhow!("Value column '{}' not found in CSV", value_column))?;

    let mut imported_count = 0;
    let mut errors = Vec::new();

    for (row_num, result) in reader.records().enumerate() {
        match result {
            Ok(record) => {
                let key = record
                    .get(key_index)
                    .ok_or_else(|| anyhow::anyhow!("Missing key in row {}", row_num + 1))?;

                let value = record
                    .get(value_index)
                    .ok_or_else(|| anyhow::anyhow!("Missing value in row {}", row_num + 1))?;

                let result = match cf {
                    Some(cf_name) => {
                        let cf_ref = db
                            .column_family(cf_name)
                            .context("Failed to get column family")?;
                        cf_ref.put(&key.to_string(), &value.to_string())
                    }
                    None => db.put(key.to_string(), &value.to_string()),
                };

                match result {
                    Ok(_) => imported_count += 1,
                    Err(e) => errors.push(format!(
                        "Row {}: Failed to import key '{}': {}",
                        row_num + 1,
                        key,
                        e
                    )),
                }
            }
            Err(e) => {
                errors.push(format!("Row {}: CSV parsing error: {}", row_num + 1, e));
            }
        }
    }

    match format {
        OutputFormat::Json => {
            let result = serde_json::json!({
                "status": if errors.is_empty() { "success" } else { "partial_success" },
                "operation": "import_csv",
                "file": file_path,
                "column_family": cf,
                "key_column": key_column,
                "value_column": value_column,
                "imported_count": imported_count,
                "error_count": errors.len(),
                "errors": errors
            });
            println!("{}", serde_json::to_string_pretty(&result)?);
        }
        OutputFormat::Table => {
            println!("CSV Import Results");
            println!("=================");
            println!("File: {:?}", file_path);
            println!("Key column: {}", key_column);
            println!("Value column: {}", value_column);
            println!("Imported: {} records", imported_count);

            if let Some(cf_name) = cf {
                println!("Column family: {}", cf_name);
            }

            if !errors.is_empty() {
                println!("Errors: {}", errors.len());
                for error in &errors {
                    println!("  - {}", error);
                }
            } else {
                println!("✓ All records imported successfully");
            }
        }
        OutputFormat::Csv => {
            println!(
                "operation,file,column_family,key_column,value_column,imported_count,error_count"
            );
            println!(
                "import_csv,{:?},{},{},{},{},{}",
                file_path,
                cf.unwrap_or("default"),
                key_column,
                value_column,
                imported_count,
                errors.len()
            );
        }
    }

    Ok(())
}
