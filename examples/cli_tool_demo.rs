use rocksmap::RocksMap;
use std::process::Command;
use tempfile::TempDir;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🛠️ RocksMap CLI Usage Example");

    let temp_dir = TempDir::new()?;
    let db_path = temp_dir.path();

    let db = RocksMap::<String, String>::open(db_path)?;
    db.put("user:1".to_string(), &"Alice Johnson".to_string())?;
    db.put("user:2".to_string(), &"Bob Smith".to_string())?;
    db.put("user:3".to_string(), &"Carol Davis".to_string())?;
    db.put("config:theme".to_string(), &"dark".to_string())?;
    db.put("config:lang".to_string(), &"en".to_string())?;
    drop(db);

    let db_path_str = db_path.to_str().unwrap();

    println!("\n📋 CLI Commands Demonstration:");

    println!("\n1. List all keys:");
    let output = Command::new("cargo")
        .args(&[
            "run",
            "--bin",
            "rocksmap-cli",
            "--",
            "-d",
            db_path_str,
            "list",
        ])
        .output()?;
    println!("{}", String::from_utf8_lossy(&output.stdout));

    println!("2. Get specific value:");
    let output = Command::new("cargo")
        .args(&[
            "run",
            "--bin",
            "rocksmap-cli",
            "--",
            "-d",
            db_path_str,
            "get",
            "user:1",
        ])
        .output()?;
    println!("{}", String::from_utf8_lossy(&output.stdout));

    println!("3. Database statistics:");
    let output = Command::new("cargo")
        .args(&[
            "run",
            "--bin",
            "rocksmap-cli",
            "--",
            "-d",
            db_path_str,
            "admin",
            "stats",
        ])
        .output()?;
    println!("{}", String::from_utf8_lossy(&output.stdout));

    println!("4. Database analysis:");
    let output = Command::new("cargo")
        .args(&[
            "run",
            "--bin",
            "rocksmap-cli",
            "--",
            "-d",
            db_path_str,
            "diag",
            "analyze",
        ])
        .output()?;
    println!("{}", String::from_utf8_lossy(&output.stdout));

    println!("5. Integrity check:");
    let output = Command::new("cargo")
        .args(&[
            "run",
            "--bin",
            "rocksmap-cli",
            "--",
            "-d",
            db_path_str,
            "diag",
            "check",
        ])
        .output()?;
    println!("{}", String::from_utf8_lossy(&output.stdout));

    println!("✅ CLI demonstration completed!");
    Ok(())
}
