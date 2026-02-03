use clap::Parser;
use std::path::PathBuf;

#[derive(Parser, Debug)]
#[command(name = "graphd", about = "Graph database server with Bolt protocol and Neo4j Query API")]
pub struct Config {
    /// HTTP port (Neo4j Query API + management).
    #[arg(long, default_value_t = 7688, env = "GRAPHD_HTTP_PORT")]
    pub http_port: u16,

    /// HTTP bind address.
    #[arg(long, default_value = "127.0.0.1", env = "GRAPHD_HTTP_HOST")]
    pub http_host: String,

    /// Bolt protocol port.
    #[arg(long, default_value_t = 7687, env = "GRAPHD_BOLT_PORT")]
    pub bolt_port: u16,

    /// Bolt bind address.
    #[arg(long, default_value = "127.0.0.1", env = "GRAPHD_BOLT_HOST")]
    pub bolt_host: String,

    /// Path to the graph database directory. Created if it doesn't exist.
    #[arg(short, long, default_value = "./data")]
    pub data_dir: PathBuf,

    /// Authentication token (plaintext). If set, clients must provide this token.
    /// Can also be set via GRAPHD_TOKEN env var.
    #[arg(long, env = "GRAPHD_TOKEN", conflicts_with = "token_file")]
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

    /// Encryption key for compacted journal files (hex-encoded, 64 chars = 32 bytes).
    /// Used for sealed .graphj files. Live segments are never encrypted.
    #[arg(long, env = "GRAPHD_JOURNAL_ENCRYPTION_KEY")]
    pub journal_encryption_key: Option<String>,

    /// Enable zstd compression for compacted journal files.
    #[arg(long)]
    pub journal_compress: bool,

    /// Zstd compression level for compacted journal files (1-19).
    #[arg(long, default_value = "3")]
    pub journal_compress_level: i32,

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

    /// HTTP transaction timeout in seconds. Abandoned transactions are reaped after this.
    #[arg(long, default_value = "30", env = "GRAPHD_TX_TIMEOUT")]
    pub tx_timeout_secs: u64,

    /// Maximum number of concurrent Bolt connections.
    #[arg(long, default_value = "256", env = "GRAPHD_BOLT_MAX_CONNECTIONS")]
    pub bolt_max_connections: u32,

    /// Number of concurrent read connections in the engine pool.
    #[arg(long, default_value = "4", env = "GRAPHD_READ_CONNECTIONS")]
    pub read_connections: usize,

    /// S3 bucket for snapshot uploads (e.g. graphd-snapshots).
    #[arg(long, env = "GRAPHD_S3_BUCKET")]
    pub s3_bucket: Option<String>,

    /// S3 key prefix for snapshots (e.g. backups/mydb/).
    #[arg(long, env = "GRAPHD_S3_PREFIX", default_value = "")]
    pub s3_prefix: String,
}
