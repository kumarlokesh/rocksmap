use rocksmap::RocksMap;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🚀 RocksMap Simple Example");

    let temp_dir = tempfile::tempdir()?;
    let db_path = temp_dir.path();

    println!("Database path: {:?}", db_path);

    let db = RocksMap::<String, String>::open(db_path)?;

    println!("\n📝 Storing data...");
    db.put("user:1".to_string(), &"Alice Johnson".to_string())?;
    db.put("user:2".to_string(), &"Bob Smith".to_string())?;
    db.put("user:3".to_string(), &"Carol Davis".to_string())?;
    db.put("config:theme".to_string(), &"dark".to_string())?;
    db.put("config:language".to_string(), &"en".to_string())?;
    println!("  ✓ Stored 5 key-value pairs");

    println!("\n🔍 Retrieving data...");
    if let Some(user) = db.get(&"user:1".to_string())? {
        println!("  Found user:1 = {}", user);
    }

    if let Some(theme) = db.get(&"config:theme".to_string())? {
        println!("  Found config:theme = {}", theme);
    }

    println!("\n📋 All user keys:");
    for result in db.prefix_scan(&"user:".to_string()).unwrap() {
        match result {
            Ok((key, value)) => println!("  {}: {}", key, value),
            Err(e) => eprintln!("Error reading key: {}", e),
        }
    }

    println!("\n📦 Batch operations...");
    let mut batch = db.batch();
    batch.put(&"user:4".to_string(), &"David Wilson".to_string())?;
    batch.put(&"user:5".to_string(), &"Eva Brown".to_string())?;
    batch.commit()?;
    println!("  ✓ Batch committed successfully");

    let mut total_keys = 0;
    for result in db.iter().unwrap() {
        match result {
            Ok(_entry) => total_keys += 1,
            Err(e) => eprintln!("Error reading entry: {}", e),
        }
    }
    println!("\n📊 Total keys in database: {}", total_keys);

    println!("\n🗑️  Deleting user:2...");
    db.delete(&"user:2".to_string())?;
    println!("  ✓ Key deleted");

    let mut final_count = 0;
    for result in db.iter().unwrap() {
        match result {
            Ok(_entry) => final_count += 1,
            Err(e) => eprintln!("Error reading entry: {}", e),
        }
    }
    println!("📊 Final key count: {}", final_count);

    println!("\n✅ Example completed successfully!");
    Ok(())
}
