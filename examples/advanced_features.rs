use rocksmap::RocksMap;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, PartialEq, Clone)]
struct User {
    id: u64,
    name: String,
    email: String,
    active: bool,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = tempfile::tempdir()?;
    let db_path = temp_dir.path();

    println!("🚀 RocksMap Basic Usage Example");
    println!("Database path: {:?}", db_path);

    let mut user_db = RocksMap::<u64, User>::open(db_path)?;

    let users = vec![
        User {
            id: 1,
            name: "Alice Johnson".to_string(),
            email: "alice@example.com".to_string(),
            active: true,
        },
        User {
            id: 2,
            name: "Bob Smith".to_string(),
            email: "bob@example.com".to_string(),
            active: false,
        },
        User {
            id: 3,
            name: "Carol Davis".to_string(),
            email: "carol@example.com".to_string(),
            active: true,
        },
    ];

    println!("\n📝 Storing users...");
    for user in &users {
        user_db.put(user.id, user)?;
        println!("  ✓ Stored user: {}", user.name);
    }

    println!("\n🔍 Retrieving user with ID 2...");
    if let Some(user) = user_db.get(&2)? {
        println!("  Found user: {:?}", user);
    }

    println!("\n📋 All users in database:");
    for result in user_db.iter().unwrap() {
        match result {
            Ok((id, user)) => println!(
                "  ID {}: {} ({})",
                id,
                user.name,
                if user.active { "active" } else { "inactive" }
            ),
            Err(e) => eprintln!("Error reading user: {}", e),
        }
    }

    println!("\n🗂️  Using column families...");
    let settings_cf = user_db.column_family("settings")?;

    let theme_setting = User {
        id: 1,
        name: "dark_theme".to_string(),
        email: "setting@rocksmap.com".to_string(),
        active: true,
    };

    let notification_setting = User {
        id: 2,
        name: "notifications_enabled".to_string(),
        email: "setting@rocksmap.com".to_string(),
        active: true,
    };

    settings_cf.put(&1, &theme_setting)?;
    settings_cf.put(&2, &notification_setting)?;

    if let Some(setting) = settings_cf.get(&1)? {
        println!("  User 1 setting: {}", setting.name);
    }

    println!("\n📦 Batch operations...");
    let mut batch = user_db.batch();
    batch.put(
        &4,
        &User {
            id: 4,
            name: "David Wilson".to_string(),
            email: "david@example.com".to_string(),
            active: true,
        },
    )?;
    batch.put(
        &5,
        &User {
            id: 5,
            name: "Eva Brown".to_string(),
            email: "eva@example.com".to_string(),
            active: false,
        },
    )?;
    batch.commit()?;
    println!("  ✓ Batch committed successfully");

    let mut total_users = 0;
    for result in user_db.iter().unwrap() {
        match result {
            Ok(_entry) => total_users += 1,
            Err(e) => eprintln!("Error counting users: {}", e),
        }
    }
    println!("\n📊 Total users in database: {}", total_users);

    println!("\n🗑️  Deleting user 2...");
    user_db.delete(&2)?;
    println!("  ✓ User deleted");

    let mut final_count = 0;
    for result in user_db.iter().unwrap() {
        match result {
            Ok(_entry) => final_count += 1,
            Err(e) => eprintln!("Error counting final users: {}", e),
        }
    }
    println!("📊 Final user count: {}", final_count);

    println!("\n✅ Example completed successfully!");
    Ok(())
}
