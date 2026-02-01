use crate::backend::Backend;
use crate::journal::{
    self, JournalCommand, JournalReader, JournalSender, JournalState,
};
use crate::values::json_params_to_lbug;
use crate::wire::proto;
use prost::Message;
use std::path::{Path, PathBuf};
use std::sync::atomic::Ordering;
use std::time::{SystemTime, UNIX_EPOCH};
use tracing::{info, warn};

// ─── Types ───

pub struct SnapshotInfo {
    pub sequence: u64,
    pub chain_hash: [u8; 32],
    pub timestamp_ms: i64,
    pub path: PathBuf,
}

#[derive(Clone)]
pub struct RetentionConfig {
    pub daily: usize,
    pub weekly: usize,
    pub monthly: usize,
}

struct SnapshotEntry {
    path: PathBuf,
    sequence: u64,
    chain_hash: [u8; 32],
    timestamp_ms: i64,
}

// ─── Filesystem helpers ───

/// Recursively copy a directory, skipping entries whose name is in `exclude`.
fn copy_dir(src: &Path, dst: &Path, exclude: &[&str]) -> Result<(), String> {
    std::fs::create_dir_all(dst)
        .map_err(|e| format!("Failed to create {}: {e}", dst.display()))?;
    for entry in
        std::fs::read_dir(src).map_err(|e| format!("Failed to read {}: {e}", src.display()))?
    {
        let entry = entry.map_err(|e| format!("Dir entry error: {e}"))?;
        let name = entry.file_name();
        let name_str = name.to_string_lossy();
        if exclude.iter().any(|&ex| name_str == ex) {
            continue;
        }
        let src_path = entry.path();
        let dst_path = dst.join(&name);
        if src_path.is_dir() {
            copy_dir(&src_path, &dst_path, &[])?;
        } else {
            std::fs::copy(&src_path, &dst_path).map_err(|e| {
                format!(
                    "Failed to copy {} -> {}: {e}",
                    src_path.display(),
                    dst_path.display()
                )
            })?;
        }
    }
    Ok(())
}

// ─── Snapshot creation ───

/// Create a point-in-time snapshot of the database.
///
/// The snapshot is stored at `{data_dir}/snapshots/{sequence:016}/` and includes
/// a copy of the database files plus a `snapshot.meta` protobuf file.
pub fn create_snapshot(
    data_dir: &Path,
    db: &Backend,
    journal: &JournalSender,
    journal_state: &JournalState,
    retention_config: &RetentionConfig,
) -> Result<SnapshotInfo, String> {
    // 1. Flush journal to ensure all entries are on disk.
    let (ack_tx, ack_rx) = std::sync::mpsc::sync_channel(1);
    journal
        .send(JournalCommand::Flush(ack_tx))
        .map_err(|_| "Journal writer is gone".to_string())?;
    ack_rx
        .recv()
        .map_err(|_| "Journal flush acknowledgement failed".to_string())?;

    // 2. Read journal state (sequence + chain hash).
    let sequence = journal_state.sequence.load(Ordering::SeqCst);
    let chain_hash = *journal_state.chain_hash.lock().unwrap();

    // 3. CHECKPOINT the database to flush its WAL.
    let conn = db.connection()?;
    if let Err(e) = conn.query("CHECKPOINT") {
        warn!("CHECKPOINT failed (continuing anyway): {e}");
    }
    drop(conn);

    // 4. Create snapshot directory and copy database file/dir.
    //    DB lives at data_dir/db — copy it into the snapshot.
    let db_path = data_dir.join("db");
    let snapshots_dir = data_dir.join("snapshots");
    let snap_dir = snapshots_dir.join(format!("{:016}", sequence));
    let snap_data_dir = snap_dir.join("data");
    std::fs::create_dir_all(&snap_data_dir)
        .map_err(|e| format!("Failed to create snapshot dir: {e}"))?;
    if db_path.is_dir() {
        copy_dir(&db_path, &snap_data_dir.join("db"), &[])?;
    } else {
        std::fs::copy(&db_path, &snap_data_dir.join("db"))
            .map_err(|e| format!("Failed to copy db file: {e}"))?;
    }

    // 5. Write snapshot.meta.
    let now_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64;
    let meta = proto::SnapshotMeta {
        sequence,
        chain_hash: chain_hash.to_vec(),
        timestamp_ms: now_ms,
        version: env!("CARGO_PKG_VERSION").to_string(),
    };
    std::fs::write(snap_dir.join("snapshot.meta"), meta.encode_to_vec())
        .map_err(|e| format!("Failed to write snapshot meta: {e}"))?;

    info!(
        "Snapshot created at {} (seq={}, hash={:x?})",
        snap_dir.display(),
        sequence,
        &chain_hash[..4]
    );

    // Run retention to clean up old snapshots and compact journal.
    let journal_dir = data_dir.join("journal");
    if let Err(e) = apply_retention(&snapshots_dir, &journal_dir, retention_config) {
        warn!("Retention failed: {e}");
    }

    Ok(SnapshotInfo {
        sequence,
        chain_hash,
        timestamp_ms: now_ms,
        path: snap_dir,
    })
}

// ─── Restore ───

/// Read a snapshot's metadata from its directory.
fn read_snapshot_meta(snap_dir: &Path) -> Result<proto::SnapshotMeta, String> {
    let meta_path = snap_dir.join("snapshot.meta");
    let bytes =
        std::fs::read(&meta_path).map_err(|e| format!("Failed to read snapshot meta: {e}"))?;
    proto::SnapshotMeta::decode(bytes.as_slice())
        .map_err(|e| format!("Failed to decode snapshot meta: {e}"))
}

/// Find the latest snapshot in the snapshots directory.
fn find_latest_snapshot(data_dir: &Path) -> Result<PathBuf, String> {
    let snapshots_dir = data_dir.join("snapshots");
    let entries = list_snapshots(&snapshots_dir)?;
    entries
        .into_iter()
        .max_by_key(|e| e.sequence)
        .map(|e| e.path)
        .ok_or_else(|| "No snapshots found".to_string())
}

/// Restore the database from a snapshot, then replay journal entries.
///
/// If `snapshot_path` is None, the latest snapshot is used.
/// After restoring the snapshot's data files, journal entries with sequence
/// greater than the snapshot's sequence are replayed in order.
pub fn restore(data_dir: &Path, snapshot_path: Option<&Path>) -> Result<(), String> {
    let snap_dir = match snapshot_path {
        Some(p) => p.to_path_buf(),
        None => find_latest_snapshot(data_dir)?,
    };

    let meta = read_snapshot_meta(&snap_dir)?;
    let snap_seq = meta.sequence;
    let snap_hash: [u8; 32] = meta
        .chain_hash
        .as_slice()
        .try_into()
        .map_err(|_| "Invalid chain hash length in snapshot meta".to_string())?;

    info!(
        "Restoring from snapshot seq={} ({})",
        snap_seq,
        snap_dir.display()
    );

    // 1. Remove old DB if present (may be file or directory).
    let db_path = data_dir.join("db");
    if db_path.is_dir() {
        std::fs::remove_dir_all(&db_path)
            .map_err(|e| format!("Failed to remove old db dir: {e}"))?;
    } else if db_path.exists() {
        std::fs::remove_file(&db_path)
            .map_err(|e| format!("Failed to remove old db file: {e}"))?;
    }

    // 2. Copy snapshot data to data_dir/db.
    let snap_data_dir = snap_dir.join("data");
    let snap_db = snap_data_dir.join("db");
    if snap_db.is_dir() {
        copy_dir(&snap_db, &db_path, &[])?;
    } else {
        std::fs::copy(&snap_db, &db_path)
            .map_err(|e| format!("Failed to copy db from snapshot: {e}"))?;
    }

    // 3. Open DB and replay journal entries.
    let journal_dir = data_dir.join("journal");
    if journal_dir.exists() {
        let db = Backend::open(&db_path)?;
        let conn = db.connection()?;

        // Use the snapshot's chain hash so the reader can validate entries even
        // after journal compaction has deleted old segments.
        let reader = JournalReader::from_sequence(&journal_dir, snap_seq + 1, snap_hash)?;
        let mut replayed = 0u64;
        let mut skipped = 0u64;

        for item in reader {
            let re = item?;
            let params = journal::map_entries_to_json(&re.entry.params);
            match execute_restore_entry(&conn, &re.entry.query, &params) {
                Ok(()) => replayed += 1,
                Err(e) => {
                    warn!(
                        "Skipping journal entry seq={}: {e}",
                        re.sequence
                    );
                    skipped += 1;
                }
            }
        }

        info!("Restore complete: replayed={replayed}, skipped={skipped}");
    } else {
        info!("Restore complete: no journal to replay");
    }

    Ok(())
}

/// Execute a single journal entry on a connection (no rewriter — journal
/// already stores rewritten queries with concrete param values).
fn execute_restore_entry(
    conn: &lbug::Connection<'_>,
    query: &str,
    params: &Option<serde_json::Value>,
) -> Result<(), String> {
    let has_params = params
        .as_ref()
        .and_then(|p| p.as_object())
        .map_or(false, |o| !o.is_empty());

    if has_params {
        let owned = json_params_to_lbug(params.as_ref().unwrap())?;
        let refs: Vec<(&str, lbug::Value)> =
            owned.iter().map(|(k, v)| (k.as_str(), v.clone())).collect();
        let mut prepared = conn.prepare(query).map_err(|e| format!("{e}"))?;
        conn.execute(&mut prepared, refs)
            .map_err(|e| format!("{e}"))?;
    } else {
        conn.query(query).map_err(|e| format!("{e}"))?;
    }
    Ok(())
}

// ─── GFS retention ───

/// List all snapshots in the given directory, reading their metadata.
fn list_snapshots(snapshots_dir: &Path) -> Result<Vec<SnapshotEntry>, String> {
    let mut entries = Vec::new();
    let read_dir = match std::fs::read_dir(snapshots_dir) {
        Ok(d) => d,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(entries),
        Err(e) => return Err(format!("Failed to read snapshots dir: {e}")),
    };
    for item in read_dir {
        let item = item.map_err(|e| format!("Dir entry error: {e}"))?;
        let path = item.path();
        if !path.is_dir() {
            continue;
        }
        let meta_path = path.join("snapshot.meta");
        if !meta_path.exists() {
            continue;
        }
        let meta = read_snapshot_meta(&path)?;
        entries.push(SnapshotEntry {
            path,
            sequence: meta.sequence,
            chain_hash: meta
                .chain_hash
                .as_slice()
                .try_into()
                .unwrap_or([0u8; 32]),
            timestamp_ms: meta.timestamp_ms,
        });
    }
    Ok(entries)
}

/// Parse the starting sequence number from a journal segment filename.
/// Expected format: `journal-{seq:016}.wal`
fn parse_segment_start_seq(path: &Path) -> Option<u64> {
    let name = path.file_stem()?.to_string_lossy();
    let seq_str = name.strip_prefix("journal-")?;
    seq_str.parse().ok()
}

/// Apply Grandfather-Father-Son retention to snapshots and compact the journal.
///
/// - `daily`: keep the N most recent snapshots
/// - `weekly`: keep one per week, up to M weeks
/// - `monthly`: keep one per month, up to L months
///
/// Journal segments fully covered by the oldest retained snapshot are deleted.
pub fn apply_retention(
    snapshots_dir: &Path,
    journal_dir: &Path,
    config: &RetentionConfig,
) -> Result<(), String> {
    let mut snapshots = list_snapshots(snapshots_dir)?;
    if snapshots.is_empty() {
        return Ok(());
    }
    snapshots.sort_by(|a, b| b.timestamp_ms.cmp(&a.timestamp_ms)); // newest first

    let mut keep = std::collections::HashSet::new();

    // Daily: keep the N most recent snapshots.
    for snap in snapshots.iter().take(config.daily) {
        keep.insert(snap.path.clone());
    }

    // Weekly: keep one per week (approximate 7-day buckets), up to M.
    const WEEK_MS: i64 = 7 * 24 * 3600 * 1000;
    let mut weeks_kept = 0usize;
    let mut last_week: Option<i64> = None;
    for snap in &snapshots {
        let week = snap.timestamp_ms / WEEK_MS;
        if last_week != Some(week) {
            keep.insert(snap.path.clone());
            last_week = Some(week);
            weeks_kept += 1;
            if weeks_kept >= config.weekly {
                break;
            }
        }
    }

    // Monthly: keep one per ~30-day bucket, up to L.
    const MONTH_MS: i64 = 30 * 24 * 3600 * 1000;
    let mut months_kept = 0usize;
    let mut last_month: Option<i64> = None;
    for snap in &snapshots {
        let month = snap.timestamp_ms / MONTH_MS;
        if last_month != Some(month) {
            keep.insert(snap.path.clone());
            last_month = Some(month);
            months_kept += 1;
            if months_kept >= config.monthly {
                break;
            }
        }
    }

    // Delete non-retained snapshots.
    let mut deleted = 0usize;
    for snap in &snapshots {
        if !keep.contains(&snap.path) {
            std::fs::remove_dir_all(&snap.path)
                .map_err(|e| format!("Failed to remove snapshot: {e}"))?;
            deleted += 1;
        }
    }

    // Compact journal: delete segments fully before the oldest retained snapshot.
    let oldest_kept_seq = snapshots
        .iter()
        .filter(|s| keep.contains(&s.path))
        .map(|s| s.sequence)
        .min()
        .unwrap_or(0);

    let compacted = if oldest_kept_seq > 0 && journal_dir.exists() {
        compact_journal(journal_dir, oldest_kept_seq)?
    } else {
        0
    };

    info!("Retention: kept={}, deleted={deleted}, journal_segments_compacted={compacted}",
        keep.len());

    Ok(())
}

/// Delete journal segment files whose entries are all before `min_sequence`.
///
/// A segment is safe to delete if the *next* segment's starting sequence
/// is <= min_sequence (meaning this segment ends before min_sequence).
/// The last segment is never deleted.
fn compact_journal(journal_dir: &Path, min_sequence: u64) -> Result<usize, String> {
    let mut segments: Vec<(PathBuf, u64)> = std::fs::read_dir(journal_dir)
        .map_err(|e| format!("Failed to read journal dir: {e}"))?
        .filter_map(|e| e.ok())
        .filter_map(|e| {
            let path = e.path();
            let seq = parse_segment_start_seq(&path)?;
            Some((path, seq))
        })
        .collect();
    segments.sort_by_key(|(_, seq)| *seq);

    let mut deleted = 0usize;
    for i in 0..segments.len().saturating_sub(1) {
        let next_start = segments[i + 1].1;
        if next_start <= min_sequence {
            std::fs::remove_file(&segments[i].0)
                .map_err(|e| format!("Failed to remove segment: {e}"))?;
            deleted += 1;
        }
    }
    Ok(deleted)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_copy_dir_basic() {
        let src = tempdir().unwrap();
        let dst = tempdir().unwrap();
        std::fs::write(src.path().join("a.txt"), "hello").unwrap();
        std::fs::create_dir(src.path().join("sub")).unwrap();
        std::fs::write(src.path().join("sub/b.txt"), "world").unwrap();

        copy_dir(src.path(), &dst.path().join("out"), &[]).unwrap();

        assert_eq!(
            std::fs::read_to_string(dst.path().join("out/a.txt")).unwrap(),
            "hello"
        );
        assert_eq!(
            std::fs::read_to_string(dst.path().join("out/sub/b.txt")).unwrap(),
            "world"
        );
    }

    #[test]
    fn test_copy_dir_with_exclude() {
        let src = tempdir().unwrap();
        let dst = tempdir().unwrap();
        std::fs::write(src.path().join("keep.txt"), "yes").unwrap();
        std::fs::create_dir(src.path().join("skip")).unwrap();
        std::fs::write(src.path().join("skip/nope.txt"), "no").unwrap();

        copy_dir(src.path(), &dst.path().join("out"), &["skip"]).unwrap();

        assert!(dst.path().join("out/keep.txt").exists());
        assert!(!dst.path().join("out/skip").exists());
    }

    #[test]
    fn test_list_snapshots_empty() {
        let dir = tempdir().unwrap();
        let snaps = list_snapshots(dir.path()).unwrap();
        assert!(snaps.is_empty());
    }

    #[test]
    fn test_list_snapshots_nonexistent() {
        let dir = tempdir().unwrap();
        let snaps = list_snapshots(&dir.path().join("nope")).unwrap();
        assert!(snaps.is_empty());
    }

    #[test]
    fn test_list_snapshots_with_entries() {
        let dir = tempdir().unwrap();
        for seq in [5u64, 10, 15] {
            let snap_dir = dir.path().join(format!("{:016}", seq));
            std::fs::create_dir_all(&snap_dir).unwrap();
            let meta = proto::SnapshotMeta {
                sequence: seq,
                chain_hash: vec![0u8; 32],
                timestamp_ms: seq as i64 * 1000,
                version: "test".into(),
            };
            std::fs::write(snap_dir.join("snapshot.meta"), meta.encode_to_vec()).unwrap();
        }

        let snaps = list_snapshots(dir.path()).unwrap();
        assert_eq!(snaps.len(), 3);
        let seqs: Vec<u64> = snaps.iter().map(|s| s.sequence).collect();
        assert!(seqs.contains(&5));
        assert!(seqs.contains(&10));
        assert!(seqs.contains(&15));
    }

    #[test]
    fn test_parse_segment_start_seq() {
        assert_eq!(
            parse_segment_start_seq(Path::new("journal-0000000000000001.wal")),
            Some(1)
        );
        assert_eq!(
            parse_segment_start_seq(Path::new("journal-0000000000000042.wal")),
            Some(42)
        );
        assert_eq!(parse_segment_start_seq(Path::new("other.wal")), None);
        assert_eq!(parse_segment_start_seq(Path::new("journal-.wal")), None);
    }

    #[test]
    fn test_retention_keeps_daily() {
        let dir = tempdir().unwrap();
        let snaps_dir = dir.path().join("snapshots");
        let journal_dir = dir.path().join("journal");
        std::fs::create_dir_all(&journal_dir).unwrap();

        // Create 5 snapshots
        for i in 1..=5u64 {
            let snap = snaps_dir.join(format!("{:016}", i));
            std::fs::create_dir_all(&snap).unwrap();
            let meta = proto::SnapshotMeta {
                sequence: i,
                chain_hash: vec![0u8; 32],
                timestamp_ms: i as i64 * 86_400_000, // 1 day apart
                version: "test".into(),
            };
            std::fs::write(snap.join("snapshot.meta"), meta.encode_to_vec()).unwrap();
        }

        apply_retention(
            &snaps_dir,
            &journal_dir,
            &RetentionConfig {
                daily: 3,
                weekly: 0,
                monthly: 0,
            },
        )
        .unwrap();

        let remaining = list_snapshots(&snaps_dir).unwrap();
        assert_eq!(remaining.len(), 3);
        let seqs: Vec<u64> = remaining.iter().map(|s| s.sequence).collect();
        // Should keep the 3 most recent
        assert!(seqs.contains(&5));
        assert!(seqs.contains(&4));
        assert!(seqs.contains(&3));
    }

    #[test]
    fn test_journal_compaction() {
        let dir = tempdir().unwrap();
        let journal_dir = dir.path().join("journal");
        std::fs::create_dir_all(&journal_dir).unwrap();

        // Create fake segment files
        for seq in [1u64, 10, 20, 30] {
            std::fs::write(
                journal_dir.join(format!("journal-{:016}.wal", seq)),
                "fake",
            )
            .unwrap();
        }

        // Compact: oldest retained snapshot is at seq=20
        let deleted = compact_journal(&journal_dir, 20).unwrap();
        assert_eq!(deleted, 2); // segments starting at 1 and 10

        let remaining: Vec<String> = std::fs::read_dir(&journal_dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .map(|e| e.file_name().to_string_lossy().to_string())
            .collect();
        assert_eq!(remaining.len(), 2);
        assert!(remaining.contains(&"journal-0000000000000020.wal".to_string()));
        assert!(remaining.contains(&"journal-0000000000000030.wal".to_string()));
    }

    #[test]
    fn test_snapshot_meta_roundtrip() {
        let dir = tempdir().unwrap();
        let snap_dir = dir.path().join("test_snap");
        std::fs::create_dir_all(&snap_dir).unwrap();

        let hash = [42u8; 32];
        let meta = proto::SnapshotMeta {
            sequence: 99,
            chain_hash: hash.to_vec(),
            timestamp_ms: 1234567890,
            version: "0.1.0".into(),
        };
        std::fs::write(snap_dir.join("snapshot.meta"), meta.encode_to_vec()).unwrap();

        let read = read_snapshot_meta(&snap_dir).unwrap();
        assert_eq!(read.sequence, 99);
        assert_eq!(read.chain_hash, hash.to_vec());
        assert_eq!(read.timestamp_ms, 1234567890);
        assert_eq!(read.version, "0.1.0");
    }
}
