use anyhow::{Context, Result};
use rocksmap::RocksMap;
use std::io::{self, Write};
use std::path::Path;

use crate::OutputFormat;

/// Interactive shell command
pub fn shell_command(db_path: &Path, format: &OutputFormat) -> Result<()> {
    println!("RocksMap Interactive Shell");
    println!("=========================");
    println!("Database: {:?}", db_path);
    println!("Type 'help' for available commands, 'exit' to quit.\n");

    let mut db = RocksMap::<String, String>::open(db_path).context("Failed to open database")?;

    loop {
        print!("rocksmap> ");
        io::stdout().flush()?;

        let mut input = String::new();
        io::stdin().read_line(&mut input)?;

        let input = input.trim();
        if input.is_empty() {
            continue;
        }

        let parts: Vec<&str> = input.split_whitespace().collect();
        if parts.is_empty() {
            continue;
        }

        match parts[0].to_lowercase().as_str() {
            "help" => show_help(),
            "exit" | "quit" => {
                println!("Goodbye!");
                break;
            }
            "put" => {
                if parts.len() >= 3 {
                    let key = parts[1];
                    let value = parts[2..].join(" ");
                    match db.put(key.to_string(), &value) {
                        Ok(_) => println!("✓ Stored '{}' = '{}'", key, value),
                        Err(e) => println!("✗ Error: {}", e),
                    }
                } else {
                    println!("Usage: put <key> <value>");
                }
            }
            "get" => {
                if parts.len() >= 2 {
                    let key = parts[1];
                    match db.get(&key.to_string()) {
                        Ok(Some(value)) => println!("'{}' = '{}'", key, value),
                        Ok(None) => println!("Key '{}' not found", key),
                        Err(e) => println!("✗ Error: {}", e),
                    }
                } else {
                    println!("Usage: get <key>");
                }
            }
            "delete" | "del" => {
                if parts.len() >= 2 {
                    let key = parts[1];
                    match db.delete(&key.to_string()) {
                        Ok(_) => println!("✓ Deleted key '{}'", key),
                        Err(e) => println!("✗ Error: {}", e),
                    }
                } else {
                    println!("Usage: delete <key>");
                }
            }
            "list" => {
                let limit = if parts.len() >= 2 {
                    parts[1].parse().unwrap_or(10)
                } else {
                    10
                };

                match db.iter() {
                    Ok(iterator) => {
                        let mut count = 0;
                        println!("{:<20} | {}", "Key", "Value");
                        println!("{:-<20}-+-{:-<40}", "", "");

                        for result in iterator {
                            if count >= limit {
                                break;
                            }

                            match result {
                                Ok((key, value)) => {
                                    let truncated_value = if value.len() > 40 {
                                        format!("{}...", &value[..37])
                                    } else {
                                        value
                                    };
                                    println!("{:<20} | {}", key, truncated_value);
                                    count += 1;
                                }
                                Err(e) => {
                                    println!("✗ Error reading entry: {}", e);
                                    break;
                                }
                            }
                        }

                        if count == 0 {
                            println!("No entries found");
                        } else {
                            println!(
                                "\nShowing {} entries (use 'list <number>' to show more)",
                                count
                            );
                        }
                    }
                    Err(e) => println!("✗ Error creating iterator: {}", e),
                }
            }
            "scan" => {
                if parts.len() >= 3 {
                    let from = parts[1];
                    let to = parts[2];

                    match db.range(&from.to_string(), &to.to_string()) {
                        Ok(iterator) => {
                            let mut count = 0;
                            println!("Range scan [{}, {}]:", from, to);
                            println!("{:<20} | {}", "Key", "Value");
                            println!("{:-<20}-+-{:-<40}", "", "");

                            for result in iterator {
                                match result {
                                    Ok((key, value)) => {
                                        let truncated_value = if value.len() > 40 {
                                            format!("{}...", &value[..37])
                                        } else {
                                            value
                                        };
                                        println!("{:<20} | {}", key, truncated_value);
                                        count += 1;
                                    }
                                    Err(e) => {
                                        println!("✗ Error reading entry: {}", e);
                                        break;
                                    }
                                }
                            }

                            if count == 0 {
                                println!("No entries found in range");
                            } else {
                                println!("\nFound {} entries in range", count);
                            }
                        }
                        Err(e) => println!("✗ Error creating range iterator: {}", e),
                    }
                } else {
                    println!("Usage: scan <from_key> <to_key>");
                }
            }
            "prefix" => {
                if parts.len() >= 2 {
                    let prefix = parts[1];

                    match db.prefix_scan(&prefix.to_string()) {
                        Ok(iterator) => {
                            let mut count = 0;
                            println!("Prefix scan for '{}':", prefix);
                            println!("{:<20} | {}", "Key", "Value");
                            println!("{:-<20}-+-{:-<40}", "", "");

                            for result in iterator {
                                match result {
                                    Ok((key, value)) => {
                                        let truncated_value = if value.len() > 40 {
                                            format!("{}...", &value[..37])
                                        } else {
                                            value
                                        };
                                        println!("{:<20} | {}", key, truncated_value);
                                        count += 1;
                                    }
                                    Err(e) => {
                                        println!("✗ Error reading entry: {}", e);
                                        break;
                                    }
                                }
                            }

                            if count == 0 {
                                println!("No entries found with prefix '{}'", prefix);
                            } else {
                                println!("\nFound {} entries with prefix", count);
                            }
                        }
                        Err(e) => println!("✗ Error creating prefix iterator: {}", e),
                    }
                } else {
                    println!("Usage: prefix <prefix>");
                }
            }
            "count" => match db.iter() {
                Ok(iterator) => {
                    let mut count = 0;
                    for result in iterator {
                        match result {
                            Ok(_) => count += 1,
                            Err(e) => {
                                println!("✗ Error counting entries: {}", e);
                                return Ok(());
                            }
                        }
                    }
                    println!("Total entries: {}", count);
                }
                Err(e) => println!("✗ Error creating iterator: {}", e),
            },
            "clear" => {
                print!("Are you sure you want to delete ALL entries? (yes/no): ");
                io::stdout().flush()?;

                let mut confirmation = String::new();
                io::stdin().read_line(&mut confirmation)?;

                if confirmation.trim().to_lowercase() == "yes" {
                    match db.iter() {
                        Ok(iterator) => {
                            let keys: Result<Vec<_>, _> =
                                iterator.map(|result| result.map(|(key, _)| key)).collect();

                            match keys {
                                Ok(keys) => {
                                    let mut deleted = 0;
                                    for key in keys {
                                        match db.delete(&key) {
                                            Ok(_) => deleted += 1,
                                            Err(e) => {
                                                println!("✗ Error deleting key '{}': {}", key, e)
                                            }
                                        }
                                    }
                                    println!("✓ Deleted {} entries", deleted);
                                }
                                Err(e) => println!("✗ Error reading keys: {}", e),
                            }
                        }
                        Err(e) => println!("✗ Error creating iterator: {}", e),
                    }
                } else {
                    println!("Operation cancelled");
                }
            }
            _ => {
                println!(
                    "Unknown command: '{}'. Type 'help' for available commands.",
                    parts[0]
                );
            }
        }
    }

    Ok(())
}

fn show_help() {
    println!("Available commands:");
    println!("  help                    - Show this help message");
    println!("  put <key> <value>       - Store a key-value pair");
    println!("  get <key>               - Retrieve value for a key");
    println!("  delete <key>            - Delete a key");
    println!("  list [limit]            - List entries (default limit: 10)");
    println!("  scan <from> <to>        - Scan range of keys [from, to]");
    println!("  prefix <prefix>         - Find keys starting with prefix");
    println!("  count                   - Count total number of entries");
    println!("  clear                   - Delete all entries (with confirmation)");
    println!("  exit                    - Exit the shell");
    println!();
}
