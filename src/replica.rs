//! Replica mode for graphd.
//!
//! Read-only replica that polls cloud storage (S3/Tigris) or local filesystem for
//! journal segments and snapshots, replays them locally, and serves read-only queries.
//!
//! ## Architecture
//!
//! 1. **Initial sync** (blocks Bolt server startup):
//!    - Download latest snapshot → Extract → Download journals → Replay
//!
//! 2. **Bolt server starts** (read-only mode):
//!    - Serves queries while poller runs in background
//!
//! 3. **Poller loop** (tokio task):
//!    - Every N seconds: List S3 → Download new segments → Replay
//!
//! ## Key Insight
//!
//! Replica emulates primary's journal directory structure locally. Downloaded .graphj
//! files are placed in `data/journal/`, then existing `JournalReader` handles replay.
//! No custom replay logic needed.

use std::path::{Path, PathBuf};
use std::time::Duration;
use tracing::{debug, error, info, warn};

use aws_config::BehaviorVersion;
use aws_sdk_s3::Client as S3Client;

use crate::journal;
use crate::snapshot;

/// Replica configuration.
#[derive(Debug, Clone)]
pub struct ReplicaConfig {
    /// Source URL: s3://bucket/prefix or file:///path
    pub source: String,
    /// Local data directory
    pub data_dir: PathBuf,
    /// Polling interval
    pub poll_interval: Duration,
    /// Lag warning threshold
    pub lag_warn: Duration,
    /// Journal encryption key (if any)
    pub encryption_key: Option<[u8; 32]>,
}

/// Parsed replica source.
#[derive(Debug)]
enum ReplicaSource {
    S3 { bucket: String, prefix: String },
    File { path: PathBuf },
}

/// Parse replica source URL.
fn parse_source(source: &str) -> Result<ReplicaSource, String> {
    if let Some(rest) = source.strip_prefix("s3://") {
        // s3://bucket/prefix or s3://bucket
        let parts: Vec<&str> = rest.splitn(2, '/').collect();
        let bucket = parts[0].to_string();
        let prefix = if parts.len() > 1 {
            parts[1].to_string()
        } else {
            String::new()
        };
        Ok(ReplicaSource::S3 { bucket, prefix })
    } else if let Some(rest) = source.strip_prefix("file://") {
        Ok(ReplicaSource::File {
            path: PathBuf::from(rest),
        })
    } else {
        Err(format!(
            "Invalid replica source: {}. Must be s3://bucket/prefix or file:///path",
            source
        ))
    }
}

/// Parse duration string like "5s", "30s", "1m".
pub fn parse_duration(s: &str) -> Result<Duration, String> {
    let s = s.trim();
    if s.is_empty() {
        return Err("Empty duration string".into());
    }

    let (num_str, unit) = if s.ends_with("ms") {
        (&s[..s.len() - 2], "ms")
    } else if s.ends_with('s') {
        (&s[..s.len() - 1], "s")
    } else if s.ends_with('m') {
        (&s[..s.len() - 1], "m")
    } else if s.ends_with('h') {
        (&s[..s.len() - 1], "h")
    } else {
        return Err(format!("Invalid duration format: {}", s));
    };

    let num: u64 = num_str
        .parse()
        .map_err(|_| format!("Invalid number in duration: {}", s))?;

    let duration = match unit {
        "ms" => Duration::from_millis(num),
        "s" => Duration::from_secs(num),
        "m" => Duration::from_secs(num * 60),
        "h" => Duration::from_secs(num * 3600),
        _ => return Err(format!("Unknown duration unit: {}", unit)),
    };

    Ok(duration)
}

/// Perform initial sync: download snapshot + journals, replay to catch up.
/// This blocks Bolt server startup.
pub async fn initial_sync(config: &ReplicaConfig) -> Result<(), String> {
    info!("Starting replica initial sync from {}", config.source);

    let source = parse_source(&config.source)?;

    match source {
        ReplicaSource::S3 { bucket, prefix } => {
            initial_sync_s3(config, &bucket, &prefix).await
        }
        ReplicaSource::File { path } => initial_sync_file(config, &path).await,
    }
}

/// Run one poll cycle after initial sync to catch up to current state.
/// This ensures the replica is current before Bolt server starts serving queries.
pub async fn catchup_poll(
    config: &ReplicaConfig,
    engine: &std::sync::Arc<crate::engine::Engine>,
) -> Result<(), String> {
    info!("Running catch-up poll to ensure replica is current");

    match poll_and_apply(config, engine).await {
        Ok(()) => {
            info!("Catch-up poll complete");
            Ok(())
        }
        Err(e) => {
            warn!("Catch-up poll failed (will retry via background poller): {}", e);
            // Don't fail startup - background poller will retry
            Ok(())
        }
    }
}

/// Initial sync from S3.
///
/// Pipelines journal download and replay: snapshot files are restored first,
/// then journal segments are downloaded and replayed concurrently via a bounded channel.
async fn initial_sync_s3(
    config: &ReplicaConfig,
    bucket: &str,
    prefix: &str,
) -> Result<(), String> {
    use crate::backend::Backend;

    info!("Initial sync from S3: s3://{}/{}", bucket, prefix);

    // 1. Find latest snapshot
    let snapshot_prefix = if prefix.is_empty() {
        "snapshots/".to_string()
    } else {
        format!("{}snapshots/", prefix)
    };

    let snap_ref = match snapshot::find_latest_snapshot_s3(bucket, &snapshot_prefix).await {
        Ok(r) => r,
        Err(e) => {
            warn!(
                "No snapshots found in S3: {}, starting from empty database",
                e
            );
            std::fs::create_dir_all(config.data_dir.join("journal"))
                .map_err(|e| format!("Failed to create journal dir: {}", e))?;
            return Ok(());
        }
    };

    info!("Found latest snapshot (seq {})", snap_ref.sequence);

    // 2. Download snapshot
    let snapshots_dir = config.data_dir.join("snapshots");
    let snap_dest = snapshots_dir.join(format!("{:016}", snap_ref.sequence));

    snapshot::download_snapshot_s3_chunked(bucket, &snap_ref.prefix, &snap_dest)
        .await
        .map_err(|e| format!("Download snapshot failed: {}", e))?;

    // 3. Restore snapshot files only (no journal replay)
    let data_dir = config.data_dir.clone();
    let snap_dest_clone = snap_dest.clone();
    let (snap_seq, _snap_hash) = tokio::task::spawn_blocking(move || {
        snapshot::restore_snapshot_files(&data_dir, &snap_dest_clone)
    })
    .await
    .map_err(|e| format!("Task join failed: {}", e))??;

    info!("Snapshot files restored (seq {})", snap_seq);

    // 4. List journal segments from S3
    let segments = list_journal_segments_s3(bucket, prefix, snap_seq).await?;
    let num_segments = segments.len();
    info!(
        "Found {} journal segments to download (seq > {})",
        num_segments, snap_seq
    );

    if num_segments == 0 {
        return Ok(());
    }

    // 5. Ensure journal dir exists
    let journal_dir = config.data_dir.join("journal");
    std::fs::create_dir_all(&journal_dir)
        .map_err(|e| format!("Failed to create journal dir: {}", e))?;

    // 6. Pipeline: download (async producer) + replay (blocking consumer)
    let (tx, rx) = std::sync::mpsc::sync_channel::<PathBuf>(4);

    let db_path = config.data_dir.join("db");
    let encryption_key = config.encryption_key;

    let consumer = tokio::task::spawn_blocking(move || -> Result<u64, String> {
        let db = Backend::open(&db_path)?;
        let conn = db.connection()?;
        let mut total_replayed = 0u64;
        let mut last_seq = snap_seq;

        for segment_path in rx {
            let (replayed, last_replayed) =
                replay_segment_from(&conn, &segment_path, last_seq, encryption_key)?;
            total_replayed += replayed;
            if last_replayed > last_seq {
                last_seq = last_replayed;
            }
            debug!(
                "Replayed segment ({} entries, last_seq={}, {} total)",
                replayed, last_seq, total_replayed
            );
        }

        Ok(total_replayed)
    });

    // Producer: download segments, send paths to consumer as each completes
    let aws_config = aws_config::load_defaults(BehaviorVersion::latest()).await;
    let s3_client = S3Client::new(&aws_config);

    for (i, (s3_key, seq)) in segments.iter().enumerate() {
        info!(
            "Downloading journal segment {}/{}: {} (seq {})",
            i + 1,
            num_segments,
            s3_key,
            seq
        );

        let local_path =
            download_single_segment_s3(&s3_client, bucket, s3_key, *seq, &journal_dir).await?;

        if tx.send(local_path).is_err() {
            return Err("Consumer thread exited early".to_string());
        }
    }

    drop(tx);

    let total_replayed = consumer
        .await
        .map_err(|e| format!("Consumer task join failed: {}", e))??;

    info!(
        "Initial sync complete: replayed {} entries from {} segments",
        total_replayed, num_segments
    );

    Ok(())
}

/// Download journal segments from S3 newer than the given sequence.
async fn download_journal_segments_s3(
    config: &ReplicaConfig,
    bucket: &str,
    prefix: &str,
    since_seq: u64,
) -> Result<(), String> {
    let aws_config = aws_config::load_defaults(BehaviorVersion::latest()).await;
    let s3_client = S3Client::new(&aws_config);

    let journal_prefix = if prefix.is_empty() {
        "journal/".to_string()
    } else {
        format!("{}journal/", prefix)
    };

    let list_resp = s3_client
        .list_objects_v2()
        .bucket(bucket)
        .prefix(&journal_prefix)
        .send()
        .await
        .map_err(|e| format!("S3 list journal failed: {}", e))?;

    let contents = list_resp.contents();
    let mut segments: Vec<(String, u64)> = Vec::new();

    for obj in contents {
        if let Some(key) = obj.key() {
            if let Some(seq) = parse_journal_seq_from_key(key) {
                if seq > since_seq {
                    segments.push((key.to_string(), seq));
                }
            }
        }
    }

    segments.sort_by_key(|(_, seq)| *seq);

    let num_segments = segments.len();
    info!(
        "Found {} journal segments to download (seq > {})",
        num_segments, since_seq
    );

    if num_segments == 0 {
        return Ok(());
    }

    let journal_dir = config.data_dir.join("journal");
    std::fs::create_dir_all(&journal_dir)
        .map_err(|e| format!("Failed to create journal dir: {}", e))?;

    for (key, seq) in segments {
        info!("Downloading journal segment: {} (seq {})", key, seq);

        let get_resp = s3_client
            .get_object()
            .bucket(bucket)
            .key(&key)
            .send()
            .await
            .map_err(|e| format!("S3 get journal segment failed: {}", e))?;

        let segment_bytes = get_resp
            .body
            .collect()
            .await
            .map_err(|e| format!("S3 read segment failed: {}", e))?
            .into_bytes();

        let local_filename = format!("journal-{:016}.graphj", seq);
        let local_path = journal_dir.join(&local_filename);

        std::fs::write(&local_path, segment_bytes)
            .map_err(|e| format!("Write journal segment failed: {}", e))?;

        debug!("Downloaded segment to {}", local_path.display());
    }

    info!("Journal segments downloaded and ready for replay");

    Ok(())
}

/// List journal segments from S3 newer than `since_seq`.
/// Returns `Vec<(s3_key, seq)>` sorted by seq ascending.
async fn list_journal_segments_s3(
    bucket: &str,
    prefix: &str,
    since_seq: u64,
) -> Result<Vec<(String, u64)>, String> {
    let aws_config = aws_config::load_defaults(BehaviorVersion::latest()).await;
    let s3_client = S3Client::new(&aws_config);

    let journal_prefix = if prefix.is_empty() {
        "journal/".to_string()
    } else {
        format!("{}journal/", prefix)
    };

    let list_resp = s3_client
        .list_objects_v2()
        .bucket(bucket)
        .prefix(&journal_prefix)
        .send()
        .await
        .map_err(|e| format!("S3 list journal failed: {}", e))?;

    let mut segments: Vec<(String, u64)> = Vec::new();
    // Track the last segment that starts at or before since_seq —
    // it may straddle the boundary and contain entries > since_seq.
    let mut last_before: Option<(String, u64)> = None;

    for obj in list_resp.contents() {
        if let Some(key) = obj.key() {
            if let Some(seq) = parse_journal_seq_from_key(key) {
                if seq > since_seq {
                    segments.push((key.to_string(), seq));
                } else if last_before.as_ref().map_or(true, |(_, s)| seq > *s) {
                    last_before = Some((key.to_string(), seq));
                }
            }
        }
    }

    // Include the straddling segment (it may contain entries after since_seq).
    if let Some(straddling) = last_before {
        segments.push(straddling);
    }

    segments.sort_by_key(|(_, seq)| *seq);
    Ok(segments)
}

/// Download a single journal segment from S3 to the local journal dir.
/// Returns the local path of the downloaded file.
async fn download_single_segment_s3(
    s3_client: &S3Client,
    bucket: &str,
    key: &str,
    seq: u64,
    journal_dir: &Path,
) -> Result<PathBuf, String> {
    let get_resp = s3_client
        .get_object()
        .bucket(bucket)
        .key(key)
        .send()
        .await
        .map_err(|e| format!("S3 get journal segment failed: {}", e))?;

    let segment_bytes = get_resp
        .body
        .collect()
        .await
        .map_err(|e| format!("S3 read segment failed: {}", e))?
        .into_bytes();

    let local_filename = format!("journal-{:016}.graphj", seq);
    let local_path = journal_dir.join(&local_filename);

    std::fs::write(&local_path, &segment_bytes)
        .map_err(|e| format!("Write journal segment failed: {}", e))?;

    debug!("Downloaded segment to {}", local_path.display());
    Ok(local_path)
}

/// Initial sync from local filesystem.
async fn initial_sync_file(config: &ReplicaConfig, source_path: &Path) -> Result<(), String> {
    let t_total = std::time::Instant::now();
    info!("Initial sync from file: {}", source_path.display());

    // Find latest snapshot in source/snapshots/
    let t0 = std::time::Instant::now();
    let snapshot_dir = source_path.join("snapshots");
    if !snapshot_dir.exists() {
        warn!("No snapshots directory found, starting from empty database");
        std::fs::create_dir_all(config.data_dir.join("journal"))
            .map_err(|e| format!("Failed to create journal dir: {}", e))?;
        return Ok(());
    }

    let mut snapshots: Vec<(PathBuf, u64)> = Vec::new();
    for entry in std::fs::read_dir(&snapshot_dir)
        .map_err(|e| format!("Read snapshot dir failed: {}", e))?
    {
        let entry = entry.map_err(|e| format!("Read dir entry failed: {}", e))?;
        let path = entry.path();
        if let Some(filename) = path.file_name().and_then(|n| n.to_str()) {
            if let Some(seq) = parse_snapshot_seq_from_filename(filename) {
                snapshots.push((path, seq));
            }
        }
    }

    if snapshots.is_empty() {
        warn!("No snapshots found, starting from empty database");
        std::fs::create_dir_all(config.data_dir.join("journal"))
            .map_err(|e| format!("Failed to create journal dir: {}", e))?;
        return Ok(());
    }

    snapshots.sort_by_key(|(_, seq)| *seq);
    let (latest_path, latest_seq) = snapshots.last().unwrap();
    info!(
        "[timing] find_snapshot: {:?} (seq {})",
        t0.elapsed(),
        latest_seq
    );

    // Copy journal segments BEFORE restore so they can be replayed
    let t0 = std::time::Instant::now();
    let source_journal_dir = source_path.join("journal");
    let local_journal_dir = config.data_dir.join("journal");

    std::fs::create_dir_all(&local_journal_dir)
        .map_err(|e| format!("Failed to create journal dir: {}", e))?;

    if source_journal_dir.exists() {
        let mut segments: Vec<(PathBuf, u64)> = Vec::new();
        for entry in std::fs::read_dir(&source_journal_dir)
            .map_err(|e| format!("Read journal dir failed: {}", e))?
        {
            let entry = entry.map_err(|e| format!("Read dir entry failed: {}", e))?;
            let path = entry.path();
            if let Some(filename) = path.file_name().and_then(|n| n.to_str()) {
                if let Some(seq) = parse_journal_seq_from_filename(filename) {
                    // Copy all unsealed (active) segments, or sealed segments with seq > snapshot
                    let should_copy = if let Ok(is_sealed) = is_segment_sealed(&path) {
                        !is_sealed || seq > *latest_seq
                    } else {
                        // If we can't read the header, copy it (conservative approach)
                        true
                    };

                    if should_copy {
                        segments.push((path, seq));
                    }
                }
            }
        }

        segments.sort_by_key(|(_, seq)| *seq);

        let num_segments = segments.len();

        for (source_file, _seq) in segments {
            let filename = source_file.file_name().unwrap().to_string_lossy().to_string();
            let dest_file = local_journal_dir.join(&filename);

            std::fs::copy(&source_file, &dest_file)
                .map_err(|e| format!("Copy journal segment failed: {}", e))?;
        }

        info!(
            "[timing] copy_journal_segments: {:?} ({} segments)",
            t0.elapsed(),
            num_segments
        );
    } else {
        info!("[timing] copy_journal_segments: skipped (no journal dir)");
    }

    // Restore snapshot + replay downloaded journals
    let t0 = std::time::Instant::now();
    snapshot::restore(&config.data_dir, Some(latest_path.as_path()), config.encryption_key)
        .map_err(|e| format!("Restore snapshot failed: {}", e))?;
    info!("[timing] snapshot::restore: {:?}", t0.elapsed());

    info!(
        "[timing] initial_sync_file total: {:?}",
        t_total.elapsed()
    );

    Ok(())
}

/// Parse snapshot sequence from S3 key like "snapshots/0000000000000100.tar.zst".
fn parse_snapshot_seq_from_key(key: &str) -> Option<u64> {
    let filename = key.rsplit('/').next()?;
    parse_snapshot_seq_from_filename(filename)
}

/// Parse snapshot sequence from filename like "0000000000000100.tar.zst".
fn parse_snapshot_seq_from_filename(filename: &str) -> Option<u64> {
    // Handle both .tar.zst files (S3) and plain directory names (local)
    let stem = filename.strip_suffix(".tar.zst").unwrap_or(filename);
    u64::from_str_radix(stem, 10).ok()
}

/// Parse journal sequence from S3 key like "journal/journal-0000000000000100.graphj".
fn parse_journal_seq_from_key(key: &str) -> Option<u64> {
    let filename = key.rsplit('/').next()?;
    parse_journal_seq_from_filename(filename)
}

/// Parse journal sequence from filename like "journal-0000000000000100.graphj".
fn parse_journal_seq_from_filename(filename: &str) -> Option<u64> {
    let stem = filename.strip_prefix("journal-")?.strip_suffix(".graphj")?;
    u64::from_str_radix(stem, 10).ok()
}

/// Check if a journal segment is sealed by reading the GraphJ header.
fn is_segment_sealed(path: &Path) -> Result<bool, String> {
    use crate::graphj;
    use std::fs::File;

    let mut file = File::open(path).map_err(|e| format!("Open failed: {}", e))?;
    let header_opt = graphj::read_header(&mut file)
        .map_err(|e| format!("Read header failed: {}", e))?;
    let header = header_opt.ok_or_else(|| "Not a valid GraphJ file".to_string())?;
    Ok((header.flags & graphj::FLAG_SEALED) != 0)
}

/// Replay entries from a segment file that are > since_seq.
/// Returns (count_replayed, last_replayed_seq).
fn replay_segment_from(
    conn: &lbug::Connection,
    segment_path: &Path,
    since_seq: u64,
    encryption_key: Option<[u8; 32]>,
) -> Result<(u64, u64), String> {
    use crate::journal::JournalReader;

    let dir = segment_path
        .parent()
        .ok_or_else(|| "Invalid segment path".to_string())?;

    // Create a reader starting from since_seq + 1
    let reader = JournalReader::from_sequence_with_key(
        dir,
        since_seq + 1,
        [0u8; 32], // Hash checking will be skipped if we don't have the right previous hash
        encryption_key,
    )?;

    let mut replayed = 0u64;
    let mut last_replayed_seq = since_seq;

    // Wrap replay in a single transaction to avoid per-entry WAL fsync.
    // Journal entries are already-committed statements; if we crash mid-replay
    // we simply redo it from the snapshot.
    conn.query("BEGIN TRANSACTION")
        .map_err(|e| format!("BEGIN TRANSACTION failed: {e}"))?;

    for item in reader {
        let re = item?;
        if re.sequence <= since_seq {
            continue; // Skip already-applied entries
        }

        let params = journal::map_entries_to_param_values(&re.entry.params);
        match crate::snapshot::execute_restore_entry(conn, &re.entry.query, &params) {
            Ok(()) => {
                replayed += 1;
                last_replayed_seq = re.sequence;
                debug!("Replayed entry seq={}", re.sequence);
            }
            Err(e) => {
                warn!("Skipping entry seq={}: {}", re.sequence, e);
                last_replayed_seq = re.sequence;
            }
        }
    }

    conn.query("COMMIT")
        .map_err(|e| format!("COMMIT failed: {e}"))?;

    Ok((replayed, last_replayed_seq))
}

/// Spawn background poller task that periodically checks for new segments.
/// Takes an Arc<Engine> so it can use the Engine's writer connection for replay.
pub fn spawn_poller(
    config: ReplicaConfig,
    engine: std::sync::Arc<crate::engine::Engine>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        info!(
            "Starting replica poller (interval: {:?})",
            config.poll_interval
        );

        let mut interval = tokio::time::interval(config.poll_interval);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        let mut retry_count = 0;
        let max_retries = 5;
        let mut last_success_time = std::time::Instant::now();

        loop {
            interval.tick().await;

            match poll_and_apply(&config, &engine).await {
                Ok(()) => {
                    retry_count = 0;
                    last_success_time = std::time::Instant::now();

                    // Check lag
                    let lag = last_success_time.elapsed();
                    if lag > config.lag_warn {
                        warn!(
                            "Replica lag exceeds warning threshold: {:?} > {:?}",
                            lag, config.lag_warn
                        );
                    }
                }
                Err(e) if is_network_error(&e) => {
                    retry_count += 1;
                    if retry_count <= max_retries {
                        let backoff = std::time::Duration::from_secs(2u64.pow(retry_count.min(5)));
                        warn!(
                            "Network error during replica poll (retry {}/{}): {}. Retrying in {:?}",
                            retry_count, max_retries, e, backoff
                        );
                        tokio::time::sleep(backoff).await;
                    } else {
                        error!(
                            "Max retries exceeded for network error: {}. Will retry on next interval.",
                            e
                        );
                        retry_count = 0;
                    }
                }
                Err(e) => {
                    error!("Replica poll failed with non-network error: {}", e);
                    // Don't retry on non-network errors, wait for next interval
                }
            }
        }
    })
}

/// Check if an error is a transient network error (retryable).
fn is_network_error(err: &str) -> bool {
    err.contains("timed out")
        || err.contains("connection")
        || err.contains("network")
        || err.contains("timeout")
        || err.contains("S3")
}

/// Poll for new segments and apply them.
async fn poll_and_apply(
    config: &ReplicaConfig,
    engine: &std::sync::Arc<crate::engine::Engine>,
) -> Result<(), String> {
    debug!("Polling for new segments");

    let source = parse_source(&config.source)?;

    match source {
        ReplicaSource::S3 { bucket, prefix } => {
            poll_and_apply_s3(config, engine, &bucket, &prefix).await
        }
        ReplicaSource::File { path } => poll_and_apply_file(config, engine, &path).await,
    }
}

/// Poll S3 for new segments and apply them.
async fn poll_and_apply_s3(
    config: &ReplicaConfig,
    engine: &std::sync::Arc<crate::engine::Engine>,
    bucket: &str,
    prefix: &str,
) -> Result<(), String> {
    // Get current local state (blocking operation)
    let journal_dir = config.data_dir.join("journal");
    let journal_dir_clone = journal_dir.clone();
    let (last_local_seq, _last_hash) = tokio::task::spawn_blocking(move || {
        journal::recover_journal_state(&journal_dir_clone)
    })
    .await
    .map_err(|e| format!("Task join failed: {}", e))?
    .map_err(|e| format!("Recover journal state failed: {}", e))?;

    // List remote journal segments
    let aws_config = aws_config::load_defaults(BehaviorVersion::latest()).await;
    let s3_client = S3Client::new(&aws_config);

    let journal_prefix = if prefix.is_empty() {
        "journal/".to_string()
    } else {
        format!("{}journal/", prefix)
    };

    let list_resp = s3_client
        .list_objects_v2()
        .bucket(bucket)
        .prefix(&journal_prefix)
        .send()
        .await
        .map_err(|e| format!("S3 list journal failed: {}", e))?;

    let mut new_segments: Vec<(String, u64)> = Vec::new();
    let contents = list_resp.contents();
    for obj in contents {
        if let Some(key) = obj.key() {
            if let Some(seq) = parse_journal_seq_from_key(key) {
                if seq > last_local_seq {
                    new_segments.push((key.to_string(), seq));
                }
            }
        }
    }

    if new_segments.is_empty() {
        debug!("No new segments to download");
        return Ok(());
    }

    new_segments.sort_by_key(|(_, seq)| *seq);

    info!("Found {} new segments to download", new_segments.len());

    // Ensure journal dir exists before downloading segments.
    std::fs::create_dir_all(&journal_dir)
        .map_err(|e| format!("Failed to create journal dir: {}", e))?;

    // Download all segments first (network operations)
    let mut expected_seq = last_local_seq;
    for (key, seq) in &new_segments {
        // Check for gaps (but allow first segment after snapshot)
        if expected_seq > 0 && *seq != expected_seq + 1 {
            error!(
                "Gap in replication log: expected seq {}, got {}",
                expected_seq + 1,
                seq
            );
            std::process::exit(1);
        }

        info!("Downloading segment: {} (seq {})", key, seq);
        expected_seq = *seq;

        let get_resp = s3_client
            .get_object()
            .bucket(bucket)
            .key(key)
            .send()
            .await
            .map_err(|e| format!("S3 get segment failed: {}", e))?;

        let segment_bytes = get_resp
            .body
            .collect()
            .await
            .map_err(|e| format!("S3 read segment failed: {}", e))?
            .into_bytes();

        let local_filename = format!("journal-{:016}.graphj", seq);
        let local_path = journal_dir.join(&local_filename);

        // Move file write to blocking context
        let local_path_for_write = local_path.clone();
        tokio::task::spawn_blocking(move || std::fs::write(&local_path_for_write, segment_bytes))
            .await
            .map_err(|e| format!("Task join failed: {}", e))?
            .map_err(|e| format!("Write segment failed: {}", e))?;

        debug!("Downloaded segment to {}", local_path.display());
    }

    // Now replay all downloaded segments with Engine's writer connection
    // All blocking operations (acquire_writer, connection, file I/O, DB writes) in spawn_blocking
    let engine_clone = engine.clone();
    let journal_dir_clone = journal_dir.clone();
    let encryption_key = config.encryption_key;

    let total_replayed = tokio::task::spawn_blocking(move || -> Result<u64, String> {
        // Use Engine's writer lock to ensure replayed data is visible to readers
        let _writer_guard = engine_clone.acquire_writer();
        let conn = engine_clone
            .connection()
            .map_err(|e| format!("Failed to get engine connection: {}", e))?;

        let mut total = 0u64;

        for (_key, seq) in new_segments {
            let local_filename = format!("journal-{:016}.graphj", seq);
            let local_path = journal_dir_clone.join(&local_filename);

            // Replay only entries > last_local_seq from this segment
            let (replayed, _) = replay_segment_from(&conn, &local_path, last_local_seq, encryption_key)?;
            total += replayed;

            debug!("Replayed segment {} ({} entries)", seq, replayed);
        }

        Ok(total)
    })
    .await
    .map_err(|e| format!("Task join failed: {}", e))??;

    if total_replayed > 0 {
        info!("Downloaded and replayed {} new entries", total_replayed);
    }

    Ok(())
}

/// Poll local filesystem for new segments and apply them.
async fn poll_and_apply_file(
    config: &ReplicaConfig,
    engine: &std::sync::Arc<crate::engine::Engine>,
    source_path: &Path,
) -> Result<(), String> {
    let journal_dir = config.data_dir.join("journal");
    let source_path_owned = source_path.to_path_buf();

    // All blocking operations in spawn_blocking
    let engine_clone = engine.clone();
    let encryption_key = config.encryption_key;

    tokio::task::spawn_blocking(move || -> Result<(), String> {
        let (last_local_seq, _last_hash) = journal::recover_journal_state(&journal_dir)
            .map_err(|e| format!("Recover journal state failed: {}", e))?;

        let source_journal_dir = source_path_owned.join("journal");
        if !source_journal_dir.exists() {
            debug!("No source journal directory");
            return Ok(());
        }

        let mut segments_to_sync: Vec<(PathBuf, u64)> = Vec::new();
        for entry in std::fs::read_dir(&source_journal_dir)
            .map_err(|e| format!("Read journal dir failed: {}", e))?
        {
            let entry = entry.map_err(|e| format!("Read dir entry failed: {}", e))?;
            let path = entry.path();
            if let Some(filename) = path.file_name().and_then(|n| n.to_str()) {
                if let Some(seq) = parse_journal_seq_from_filename(filename) {
                    // Always sync unsealed (active) segments, or sealed segments with seq > last_local_seq
                    let should_sync = if let Ok(is_sealed) = is_segment_sealed(&path) {
                        !is_sealed || seq > last_local_seq
                    } else {
                        true
                    };

                    if should_sync {
                        segments_to_sync.push((path, seq));
                    }
                }
            }
        }

        if segments_to_sync.is_empty() {
            debug!("No new or updated segments to sync");
            return Ok(());
        }

        segments_to_sync.sort_by_key(|(_, seq)| *seq);

        info!("Found {} segments to sync", segments_to_sync.len());

        // Use Engine's writer lock and connection for replay
        // This ensures replayed data is visible to all readers
        let _writer_guard = engine_clone.acquire_writer();
        let conn = engine_clone
            .connection()
            .map_err(|e| format!("Failed to get engine connection: {}", e))?;

        let mut total_replayed = 0u64;

        for (source_file, seq) in segments_to_sync {
            let filename = format!("journal-{:016}.graphj", seq);
            let dest_file = journal_dir.join(&filename);

            // Copy the source file (overwrites if exists).
            // If primary is mid-write, we might get a partial last entry, but:
            // 1. All complete entries before it are valid (append-only + CRC)
            // 2. JournalReader stops at first CRC failure
            // 3. Next poll will get the complete entry
            std::fs::copy(&source_file, &dest_file)
                .map_err(|e| format!("Copy segment failed: {}", e))?;

            // Replay only entries > last_local_seq from this segment
            let (replayed, _) = replay_segment_from(&conn, &dest_file, last_local_seq, encryption_key)?;
            total_replayed += replayed;

            debug!("Synced segment {} ({} entries replayed)", seq, replayed);
        }

        if total_replayed > 0 {
            info!("Replayed {} new entries", total_replayed);
        }

        // Writer guard drops here, releasing the lock

        Ok(())
    })
    .await
    .map_err(|e| format!("Task join failed: {}", e))?
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_source_s3() {
        let src = parse_source("s3://my-bucket/my/prefix").unwrap();
        match src {
            ReplicaSource::S3 { bucket, prefix } => {
                assert_eq!(bucket, "my-bucket");
                assert_eq!(prefix, "my/prefix");
            }
            _ => panic!("Expected S3 source"),
        }
    }

    #[test]
    fn test_parse_source_s3_no_prefix() {
        let src = parse_source("s3://my-bucket").unwrap();
        match src {
            ReplicaSource::S3 { bucket, prefix } => {
                assert_eq!(bucket, "my-bucket");
                assert_eq!(prefix, "");
            }
            _ => panic!("Expected S3 source"),
        }
    }

    #[test]
    fn test_parse_source_file() {
        let src = parse_source("file:///tmp/replica-source").unwrap();
        match src {
            ReplicaSource::File { path } => {
                assert_eq!(path, PathBuf::from("/tmp/replica-source"));
            }
            _ => panic!("Expected File source"),
        }
    }

    #[test]
    fn test_parse_duration() {
        assert_eq!(parse_duration("5s").unwrap(), Duration::from_secs(5));
        assert_eq!(parse_duration("30s").unwrap(), Duration::from_secs(30));
        assert_eq!(parse_duration("1m").unwrap(), Duration::from_secs(60));
        assert_eq!(parse_duration("2h").unwrap(), Duration::from_secs(7200));
        assert_eq!(parse_duration("500ms").unwrap(), Duration::from_millis(500));
    }

    #[test]
    fn test_parse_snapshot_seq() {
        assert_eq!(
            parse_snapshot_seq_from_key("snapshots/0000000000000100.tar.zst"),
            Some(100)
        );
        assert_eq!(
            parse_snapshot_seq_from_filename("0000000000000100.tar.zst"),
            Some(100)
        );
    }

    #[test]
    fn test_parse_journal_seq() {
        assert_eq!(
            parse_journal_seq_from_key("journal/journal-0000000000001000.graphj"),
            Some(1000)
        );
        assert_eq!(
            parse_journal_seq_from_filename("journal-0000000000001000.graphj"),
            Some(1000)
        );
    }
}
