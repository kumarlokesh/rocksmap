//! `rocksmap-cli` — inspect and operate rocksmap databases.
//!
//! Safe by default: it reads/inspects any rocksmap database (plain / TTL / indexed) but only
//! *mutates* plain databases — writing into a TTL or indexed database via the CLI would bypass
//! envelope/index maintenance and corrupt invariants, so those are refused.

use anyhow::{anyhow, bail, Context, Result};
use clap::{Parser, Subcommand, ValueEnum};
use rocksdb::{
    checkpoint::Checkpoint, ColumnFamilyDescriptor, IteratorMode, Options, ReadOptions, DB,
};
use rocksmap::{
    inspect, strip_ttl_envelope, BincodeCodec, KeyCodec, MapKind, OrderedCodec, RocksMap,
    ValueCodec,
};
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Parser)]
#[command(
    name = "rocksmap-cli",
    version,
    about = "Inspect and operate rocksmap databases"
)]
struct Cli {
    /// Path to the database directory
    #[arg(short, long)]
    db: PathBuf,

    /// Output format
    #[arg(short, long, value_enum, default_value = "table", global = true)]
    format: Format,

    /// How to interpret keys (ordered-keyed databases only)
    #[arg(long, value_enum, default_value = "string", global = true)]
    key_type: KeyType,

    #[command(subcommand)]
    command: Command,
}

#[derive(Clone, Copy, ValueEnum)]
enum Format {
    Table,
    Json,
    Csv,
}

#[derive(Clone, Copy, PartialEq, Eq, ValueEnum)]
enum KeyType {
    String,
    U64,
    I64,
}

#[derive(Subcommand)]
enum Command {
    /// Report what the database is (kind, key codec, indexes)
    Info,
    /// Get a value by key
    Get { key: String },
    /// Store a key/value (plain databases only)
    Put { key: String, value: String },
    /// Delete a key (plain databases only)
    Delete { key: String },
    /// List entries (of the data column family)
    List {
        /// Only keys starting with this prefix
        #[arg(long)]
        prefix: Option<String>,
        /// Stop after this many entries
        #[arg(long)]
        limit: Option<usize>,
    },
    /// Scan the inclusive key range [from, to]
    Scan { from: String, to: String },
    /// Export all entries to a file
    Export {
        #[command(subcommand)]
        target: IoFormat,
    },
    /// Import entries from a file (plain databases only)
    Import {
        #[command(subcommand)]
        source: IoFormat,
    },
    /// Administrative operations
    Admin {
        #[command(subcommand)]
        cmd: AdminCmd,
    },
}

#[derive(Subcommand)]
enum IoFormat {
    /// JSON array of {key, value}
    Json { file: PathBuf },
    /// CSV with a key,value header
    Csv { file: PathBuf },
}

#[derive(Subcommand)]
enum AdminCmd {
    /// Approximate key count and column families
    Stats,
    /// Compact the whole database
    Compact,
    /// Create a consistent backup (checkpoint) at the given path
    Backup { path: PathBuf },
    /// List column families
    ListCf {
        /// Include internal column families (`__rocksmap_meta`, `__idx_*`)
        #[arg(long)]
        internal: bool,
    },
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    match &cli.command {
        Command::Info => cmd_info(&cli.db),
        Command::Get { key } => cmd_get(&cli, key),
        Command::Put { key, value } => cmd_put(&cli, key, value),
        Command::Delete { key } => cmd_delete(&cli, key),
        Command::List { prefix, limit } => cmd_list(&cli, prefix.as_deref(), *limit),
        Command::Scan { from, to } => cmd_scan(&cli, from, to),
        Command::Export { target } => cmd_export(&cli, target),
        Command::Import { source } => cmd_import(&cli, source),
        Command::Admin { cmd } => cmd_admin(&cli, cmd),
    }
}

// --- helpers ---

fn anyerr(e: rocksmap::Error) -> anyhow::Error {
    anyhow!(e.to_string())
}

fn now_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

fn hex(bytes: &[u8]) -> String {
    let mut s = String::with_capacity(2 + bytes.len() * 2);
    s.push_str("0x");
    for b in bytes {
        s.push_str(&format!("{b:02x}"));
    }
    s
}

fn codec_label(id: Option<u8>) -> String {
    match id {
        Some(1) => "ordered".to_string(),
        Some(2) => "bincode".to_string(),
        Some(other) => format!("unknown({other})"),
        None => "unknown".to_string(),
    }
}

fn is_internal_cf(name: &str) -> bool {
    name == "__rocksmap_meta" || name.starts_with("__idx_")
}

/// Encode a CLI key string to its stored (ordered) bytes.
fn encode_key(kt: KeyType, s: &str) -> Result<Vec<u8>> {
    Ok(match kt {
        KeyType::String => {
            <OrderedCodec<String> as KeyCodec<String>>::encode(&s.to_string()).map_err(anyerr)?
        }
        KeyType::U64 => {
            let v: u64 = s.parse().context("key is not a u64")?;
            <OrderedCodec<u64> as KeyCodec<u64>>::encode(&v).map_err(anyerr)?
        }
        KeyType::I64 => {
            let v: i64 = s.parse().context("key is not an i64")?;
            <OrderedCodec<i64> as KeyCodec<i64>>::encode(&v).map_err(anyerr)?
        }
    })
}

/// Decode stored key bytes back to a display string (falls back to hex).
fn decode_key(kt: KeyType, bytes: &[u8]) -> String {
    match kt {
        KeyType::String => {
            <OrderedCodec<String> as KeyCodec<String>>::decode(bytes).unwrap_or_else(|_| hex(bytes))
        }
        KeyType::U64 => <OrderedCodec<u64> as KeyCodec<u64>>::decode(bytes)
            .map(|v| v.to_string())
            .unwrap_or_else(|_| hex(bytes)),
        KeyType::I64 => <OrderedCodec<i64> as KeyCodec<i64>>::decode(bytes)
            .map(|v| v.to_string())
            .unwrap_or_else(|_| hex(bytes)),
    }
}

/// Render a stored value: strip the TTL envelope (skipping expired entries) then show the payload
/// as a string if it decodes, else as hex. Returns `None` for an expired entry.
fn render_value(kind: MapKind, raw: &[u8], now: u64) -> Result<Option<String>> {
    let payload = match kind {
        MapKind::Ttl => match strip_ttl_envelope(raw, now).map_err(anyerr)? {
            None => return Ok(None),
            Some(p) => p,
        },
        _ => raw.to_vec(),
    };
    let shown = <BincodeCodec<String> as ValueCodec<String>>::decode(&payload)
        .unwrap_or_else(|_| hex(&payload));
    Ok(Some(shown))
}

fn open_raw_read_only(path: &Path) -> Result<DB> {
    let opts = Options::default();
    let cfs = DB::list_cf(&opts, path).unwrap_or_else(|_| vec!["default".to_string()]);
    let descriptors: Vec<ColumnFamilyDescriptor> = cfs
        .iter()
        .map(|name| ColumnFamilyDescriptor::new(name, Options::default()))
        .collect();
    DB::open_cf_descriptors_read_only(&opts, path, descriptors, false)
        .map_err(|e| anyhow!("failed to open database read-only: {e}"))
}

fn require_plain(db: &Path) -> Result<()> {
    // A fresh (not-yet-created) database will be opened as plain by `RocksMap::open`.
    if !db.join("CURRENT").exists() {
        return Ok(());
    }
    let info = inspect(db).map_err(anyerr)?;
    if info.kind != MapKind::Plain {
        bail!(
            "this is a `{}` database; the CLI is read-only on TTL and indexed databases \
             (a raw write would bypass envelope/index maintenance and corrupt invariants)",
            info.kind
        );
    }
    Ok(())
}

fn print_rows(rows: &[(String, String)], format: Format) -> Result<()> {
    match format {
        Format::Table => {
            for (k, v) in rows {
                println!("{k}\t{v}");
            }
        }
        Format::Json => {
            let arr: Vec<_> = rows
                .iter()
                .map(|(k, v)| serde_json::json!({ "key": k, "value": v }))
                .collect();
            println!("{}", serde_json::to_string_pretty(&arr)?);
        }
        Format::Csv => {
            let mut w = csv::Writer::from_writer(std::io::stdout());
            w.write_record(["key", "value"])?;
            for (k, v) in rows {
                w.write_record([k, v])?;
            }
            w.flush()?;
        }
    }
    Ok(())
}

// --- commands ---

fn cmd_info(db: &Path) -> Result<()> {
    let info = inspect(db).map_err(anyerr)?;
    println!("kind:      {}", info.kind);
    println!("key-codec: {}", codec_label(info.key_codec_id));
    if info.kind == MapKind::Indexed {
        println!("indexes:   {:?}", info.indexes);
    }
    let user_cfs: Vec<&String> = info
        .column_families
        .iter()
        .filter(|c| !is_internal_cf(c))
        .collect();
    let internal = info.column_families.len() - user_cfs.len();
    println!("column-families: {user_cfs:?} (+{internal} internal)");
    Ok(())
}

fn cmd_get(cli: &Cli, key: &str) -> Result<()> {
    let info = inspect(&cli.db).map_err(anyerr)?;
    let raw = open_raw_read_only(&cli.db)?;
    let key_bytes = encode_key(cli.key_type, key)?;
    match raw.get(key_bytes).map_err(|e| anyhow!("{e}"))? {
        Some(value_bytes) => match render_value(info.kind, &value_bytes, now_millis())? {
            Some(value) => print_rows(&[(key.to_string(), value)], cli.format),
            None => bail!("key not found (expired)"),
        },
        None => bail!("key not found"),
    }
}

fn cmd_put(cli: &Cli, key: &str, value: &str) -> Result<()> {
    require_plain(&cli.db)?;
    if cli.key_type != KeyType::String {
        bail!("writes currently support only --key-type string");
    }
    let db = RocksMap::<String, String>::open(&cli.db).map_err(anyerr)?;
    db.put(key.to_string(), &value.to_string())
        .map_err(anyerr)?;
    eprintln!("stored `{key}`");
    Ok(())
}

fn cmd_delete(cli: &Cli, key: &str) -> Result<()> {
    require_plain(&cli.db)?;
    if cli.key_type != KeyType::String {
        bail!("deletes currently support only --key-type string");
    }
    let db = RocksMap::<String, String>::open(&cli.db).map_err(anyerr)?;
    db.delete(&key.to_string()).map_err(anyerr)?;
    eprintln!("deleted `{key}`");
    Ok(())
}

/// Collect (key, value) rows from the default column family, applying an optional inclusive
/// range and limit, skipping expired TTL entries.
fn collect_rows(
    cli: &Cli,
    kind: MapKind,
    range: Option<(&str, &str)>,
    prefix: Option<&str>,
    limit: Option<usize>,
) -> Result<Vec<(String, String)>> {
    let raw = open_raw_read_only(&cli.db)?;
    let now = now_millis();

    let mut readopts = ReadOptions::default();
    if let Some((from, to)) = range {
        readopts.set_iterate_lower_bound(encode_key(cli.key_type, from)?);
        let mut upper = encode_key(cli.key_type, to)?;
        upper.push(0x00); // inclusive of `to` (prefix-free successor)
        readopts.set_iterate_upper_bound(upper);
    }

    let mut rows = Vec::new();
    for item in raw.iterator_opt(IteratorMode::Start, readopts) {
        let (kb, vb) = item.map_err(|e| anyhow!("{e}"))?;
        let key = decode_key(cli.key_type, &kb);
        if let Some(p) = prefix {
            if !key.starts_with(p) {
                continue;
            }
        }
        if let Some(value) = render_value(kind, &vb, now)? {
            rows.push((key, value));
            if let Some(n) = limit {
                if rows.len() >= n {
                    break;
                }
            }
        }
    }
    Ok(rows)
}

fn cmd_list(cli: &Cli, prefix: Option<&str>, limit: Option<usize>) -> Result<()> {
    let info = inspect(&cli.db).map_err(anyerr)?;
    let rows = collect_rows(cli, info.kind, None, prefix, limit)?;
    print_rows(&rows, cli.format)
}

fn cmd_scan(cli: &Cli, from: &str, to: &str) -> Result<()> {
    let info = inspect(&cli.db).map_err(anyerr)?;
    let rows = collect_rows(cli, info.kind, Some((from, to)), None, None)?;
    print_rows(&rows, cli.format)
}

fn cmd_export(cli: &Cli, target: &IoFormat) -> Result<()> {
    let info = inspect(&cli.db).map_err(anyerr)?;
    let rows = collect_rows(cli, info.kind, None, None, None)?;
    match target {
        IoFormat::Json { file } => {
            let arr: Vec<_> = rows
                .iter()
                .map(|(k, v)| serde_json::json!({ "key": k, "value": v }))
                .collect();
            std::fs::write(file, serde_json::to_string_pretty(&arr)?)?;
        }
        IoFormat::Csv { file } => {
            let mut w = csv::Writer::from_path(file)?;
            w.write_record(["key", "value"])?;
            for (k, v) in &rows {
                w.write_record([k, v])?;
            }
            w.flush()?;
        }
    }
    eprintln!("exported {} entries", rows.len());
    Ok(())
}

fn cmd_import(cli: &Cli, source: &IoFormat) -> Result<()> {
    require_plain(&cli.db)?;
    if cli.key_type != KeyType::String {
        bail!("import currently supports only --key-type string");
    }
    let db = RocksMap::<String, String>::open(&cli.db).map_err(anyerr)?;
    let mut count = 0usize;
    match source {
        IoFormat::Json { file } => {
            let text = std::fs::read_to_string(file)?;
            let entries: Vec<serde_json::Value> = serde_json::from_str(&text)?;
            for e in entries {
                let key = e
                    .get("key")
                    .and_then(|v| v.as_str())
                    .context("entry missing string `key`")?;
                let value = e
                    .get("value")
                    .and_then(|v| v.as_str())
                    .context("entry missing string `value`")?;
                db.put(key.to_string(), &value.to_string())
                    .map_err(anyerr)?;
                count += 1;
            }
        }
        IoFormat::Csv { file } => {
            let mut r = csv::Reader::from_path(file)?;
            for record in r.records() {
                let record = record?;
                let key = record.get(0).context("CSV row missing key column")?;
                let value = record.get(1).unwrap_or("");
                db.put(key.to_string(), &value.to_string())
                    .map_err(anyerr)?;
                count += 1;
            }
        }
    }
    eprintln!("imported {count} entries");
    Ok(())
}

fn cmd_admin(cli: &Cli, cmd: &AdminCmd) -> Result<()> {
    match cmd {
        AdminCmd::Stats => {
            let info = inspect(&cli.db).map_err(anyerr)?;
            let raw = open_raw_read_only(&cli.db)?;
            let estimate = raw
                .property_int_value("rocksdb.estimate-num-keys")
                .ok()
                .flatten()
                .unwrap_or(0);
            println!("kind:                 {}", info.kind);
            println!("estimated-num-keys:   {estimate}");
            println!("column-families:      {}", info.column_families.len());
            Ok(())
        }
        AdminCmd::Compact => {
            // Reorganizes the data. Note: this generic compaction does not install the TTL
            // compaction filter, so it does not reclaim expired TTL entries (use the typed API).
            let opts = Options::default();
            let cfs = DB::list_cf(&opts, &cli.db).unwrap_or_else(|_| vec!["default".to_string()]);
            let descriptors: Vec<ColumnFamilyDescriptor> = cfs
                .iter()
                .map(|name| ColumnFamilyDescriptor::new(name, Options::default()))
                .collect();
            let db = DB::open_cf_descriptors(&opts, &cli.db, descriptors)
                .map_err(|e| anyhow!("failed to open for compaction: {e}"))?;
            db.compact_range::<&[u8], &[u8]>(None, None);
            eprintln!("compaction complete");
            Ok(())
        }
        AdminCmd::Backup { path } => {
            let raw = open_raw_read_only(&cli.db)?;
            let checkpoint = Checkpoint::new(&raw).map_err(|e| anyhow!("{e}"))?;
            checkpoint
                .create_checkpoint(path)
                .map_err(|e| anyhow!("backup failed: {e}"))?;
            eprintln!("backup created at {}", path.display());
            Ok(())
        }
        AdminCmd::ListCf { internal } => {
            let opts = Options::default();
            let cfs = DB::list_cf(&opts, &cli.db).map_err(|e| anyhow!("{e}"))?;
            for cf in cfs {
                if *internal || !is_internal_cf(&cf) {
                    println!("{cf}");
                }
            }
            Ok(())
        }
    }
}
