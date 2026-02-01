use clap::Parser;
use std::path::PathBuf;

#[derive(Parser, Debug)]
#[command(name = "graphd", about = "Cypher-over-WebSocket server for embedded graph databases")]
pub struct Config {
    /// Port to listen on.
    #[arg(short, long, default_value = "7688")]
    pub port: u16,

    /// Bind address.
    #[arg(long, default_value = "127.0.0.1")]
    pub host: String,

    /// Path to the graph database directory. Created if it doesn't exist.
    #[arg(short, long, default_value = "./data")]
    pub data_dir: PathBuf,

    /// Authentication token (plaintext). If set, clients must provide this in their `hello` message.
    /// Can also be set via STRANA_TOKEN env var.
    #[arg(long, env = "STRANA_TOKEN", conflicts_with = "token_file")]
    pub token: Option<String>,

    /// Path to a JSON file of hashed tokens.
    #[arg(long, conflicts_with = "token")]
    pub token_file: Option<PathBuf>,

    /// Generate a new token and its SHA-256 hash, then exit.
    #[arg(long)]
    pub generate_token: bool,

    /// Enable write-ahead journal for backup and replay.
    #[arg(long)]
    pub journal: bool,

    /// Maximum journal segment file size in megabytes.
    #[arg(long, default_value = "64")]
    pub journal_segment_mb: u64,

    /// Journal fsync interval in milliseconds.
    #[arg(long, default_value = "100")]
    pub journal_fsync_ms: u64,

    /// Restore from the latest snapshot (or --snapshot path), replay journal, then exit.
    #[arg(long)]
    pub restore: bool,

    /// Path to a specific snapshot directory for --restore.
    #[arg(long, requires = "restore")]
    pub snapshot: Option<PathBuf>,

    /// GFS retention: number of daily snapshots to keep.
    #[arg(long, default_value = "7")]
    pub retain_daily: usize,

    /// GFS retention: number of weekly snapshots to keep.
    #[arg(long, default_value = "4")]
    pub retain_weekly: usize,

    /// GFS retention: number of monthly snapshots to keep.
    #[arg(long, default_value = "3")]
    pub retain_monthly: usize,
}
