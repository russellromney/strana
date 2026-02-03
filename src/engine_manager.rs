//! EngineManager: multi-tenant database lifecycle manager.
//!
//! Opens databases on demand, evicts idle ones to reclaim memory.
//! Dropping a Database frees its C++ buffer pool. The EngineManager
//! acts as an LRU cache: hot databases stay open with warm page caches,
//! cold databases get closed and their buffer pools freed.
//!
//! The graphd binary uses a single Engine directly. This module is
//! exported for cinch cloud's multi-tenant process.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::RwLock;
use tracing::info;

use crate::backend::Backend;
use crate::engine::Engine;
use crate::journal::{self, JournalSender, JournalState};

/// Epoch millis for lock-free last-accessed tracking.
fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

/// Per-database entry in the manager.
struct ManagedEngine {
    engine: Arc<Engine>,
    /// Epoch millis — updated atomically on every get(), no write lock needed.
    last_accessed_ms: AtomicU64,
    /// Per-database journal resources (dropped on eviction -> writer thread exits).
    _journal_tx: Option<JournalSender>,
    journal_state: Option<Arc<JournalState>>,
}

/// Configuration for the EngineManager.
pub struct EngineManagerConfig {
    /// Root directory containing per-database subdirectories.
    pub data_root: PathBuf,
    /// Per-database read concurrency (default 4).
    pub num_readers: usize,
    /// Max databases open at once (default 256).
    pub max_open: usize,
    /// Close databases after this much inactivity (default 5 min).
    pub idle_timeout: Duration,
    /// Whether to enable journaling for each database.
    pub journal_enabled: bool,
    /// Journal segment size in MB.
    pub journal_segment_mb: u64,
    /// Journal fsync interval in ms.
    pub journal_fsync_ms: u64,
    /// Optional encryption key for journal files (32 bytes).
    pub journal_encryption_key: Option<[u8; 32]>,
    /// Whether to compress journal compaction output.
    pub journal_compress: bool,
    /// Compression level for journal compaction (zstd).
    pub journal_compress_level: i32,
}

impl Default for EngineManagerConfig {
    fn default() -> Self {
        Self {
            data_root: PathBuf::from("/var/lib/cinch/databases"),
            num_readers: 4,
            max_open: 256,
            idle_timeout: Duration::from_secs(300),
            journal_enabled: true,
            journal_segment_mb: 64,
            journal_fsync_ms: 100,
            journal_encryption_key: None,
            journal_compress: false,
            journal_compress_level: 3,
        }
    }
}

/// Multi-tenant database lifecycle manager.
///
/// Opens databases on demand, caps total open databases, and evicts
/// idle ones to free C++ buffer pool memory.
pub struct EngineManager {
    config: EngineManagerConfig,
    engines: RwLock<HashMap<String, ManagedEngine>>,
}

impl EngineManager {
    pub fn new(config: EngineManagerConfig) -> Self {
        Self {
            config,
            engines: RwLock::new(HashMap::new()),
        }
    }

    /// Get or open an engine for the named database.
    ///
    /// Opens the database on first access. Evicts LRU if at capacity.
    pub async fn get(&self, db_name: &str) -> Result<Arc<Engine>, String> {
        // Fast path: already open (read lock only, atomic touch).
        {
            let engines = self.engines.read().await;
            if let Some(managed) = engines.get(db_name) {
                managed.last_accessed_ms.store(now_ms(), Ordering::Relaxed);
                return Ok(managed.engine.clone());
            }
        }

        // Slow path: open database (write lock).
        let mut engines = self.engines.write().await;

        // Double-check after acquiring write lock.
        if let Some(managed) = engines.get(db_name) {
            managed.last_accessed_ms.store(now_ms(), Ordering::Relaxed);
            return Ok(managed.engine.clone());
        }

        // Evict LRU if at capacity.
        if engines.len() >= self.config.max_open {
            Self::evict_lru(&mut engines);
            if engines.len() >= self.config.max_open {
                return Err(format!(
                    "Too many open databases (max={}); all in use",
                    self.config.max_open
                ));
            }
        }

        // Open database.
        let managed = self.open_database(db_name)?;
        let engine = managed.engine.clone();
        engines.insert(db_name.to_string(), managed);
        Ok(engine)
    }

    /// Get the JournalState for a database (if open and journaling).
    pub async fn journal_state(&self, db_name: &str) -> Option<Arc<JournalState>> {
        let engines = self.engines.read().await;
        engines
            .get(db_name)
            .and_then(|m| m.journal_state.clone())
    }

    /// Explicitly close a database (e.g. on tenant deletion).
    ///
    /// Flushes the journal under the write lock to prevent get() from reopening
    /// the database path while the old Database is still alive. Single-database
    /// flush is fast (one journal ack round-trip), so lock hold time is brief.
    pub async fn close(&self, db_name: &str) -> bool {
        let mut engines = self.engines.write().await;
        if let Some(managed) = engines.remove(db_name) {
            Self::flush_and_close(db_name, managed);
            true
        } else {
            false
        }
    }

    /// Periodic cleanup: evict databases idle longer than idle_timeout.
    /// Call from a background tokio::spawn interval task.
    ///
    /// Flushes journals *outside* the write lock to avoid blocking get() during
    /// batch eviction. This creates a brief window where a evicted database name
    /// could be reopened via get() while the old ManagedEngine is still flushing.
    /// This is acceptable because evicted databases have no active users
    /// (strong_count == 1), and the window is negligible in practice.
    pub async fn cleanup(&self) {
        let timeout_ms = self.config.idle_timeout.as_millis() as u64;
        let now = now_ms();

        // Collect stale entries under the lock, then flush outside it.
        let to_evict: Vec<(String, ManagedEngine)> = {
            let mut engines = self.engines.write().await;
            let stale: Vec<String> = engines
                .iter()
                .filter(|(_, m)| {
                    let idle_ms = now.saturating_sub(m.last_accessed_ms.load(Ordering::Relaxed));
                    idle_ms > timeout_ms
                        && Arc::strong_count(&m.engine) == 1 // no active references
                })
                .map(|(name, _)| name.clone())
                .collect();

            stale
                .into_iter()
                .filter_map(|name| engines.remove(&name).map(|m| (name, m)))
                .collect()
        }; // write lock released

        if !to_evict.is_empty() {
            let count = to_evict.len();
            for (name, managed) in to_evict {
                Self::flush_and_close(&name, managed);
            }
            info!("Evicted {} idle database(s)", count);
        }
    }

    /// Pool statistics for monitoring.
    pub async fn stats(&self) -> ManagerStats {
        let engines = self.engines.read().await;
        ManagerStats {
            open_databases: engines.len(),
            max_open: self.config.max_open,
        }
    }

    // ── Internal helpers ──

    fn open_database(&self, db_name: &str) -> Result<ManagedEngine, String> {
        let db_dir = self.config.data_root.join(db_name).join("db");
        // Create the parent (db_name/) but not db/ itself — lbug creates it.
        if let Some(parent) = db_dir.parent() {
            std::fs::create_dir_all(parent)
                .map_err(|e| format!("Failed to create db parent dir: {e}"))?;
        }

        let backend = Arc::new(Backend::open(&db_dir)?);

        // Per-database journal.
        let (journal_tx, journal_state) = if self.config.journal_enabled {
            let journal_dir = self.config.data_root.join(db_name).join("journal");
            let (seq, hash) =
                journal::recover_journal_state(&journal_dir).unwrap_or((0, [0u8; 32]));
            let state = Arc::new(JournalState::with_sequence_and_hash(seq, hash));
            let tx = journal::spawn_journal_writer(
                journal_dir,
                self.config.journal_segment_mb * 1024 * 1024,
                self.config.journal_fsync_ms,
                state.clone(),
            );
            (Some(tx), Some(state))
        } else {
            (None, None)
        };

        let engine = Arc::new(Engine::new(
            backend,
            self.config.num_readers,
            journal_tx.clone(),
        ));

        info!("Opened database '{}'", db_name);

        Ok(ManagedEngine {
            engine,
            last_accessed_ms: AtomicU64::new(now_ms()),
            _journal_tx: journal_tx,
            journal_state,
        })
    }

    fn evict_lru(engines: &mut HashMap<String, ManagedEngine>) {
        // Find the least recently accessed database with no active references.
        if let Some(name) = engines
            .iter()
            .filter(|(_, m)| Arc::strong_count(&m.engine) == 1)
            .min_by_key(|(_, m)| m.last_accessed_ms.load(Ordering::Relaxed))
            .map(|(name, _)| name.clone())
        {
            if let Some(managed) = engines.remove(&name) {
                Self::flush_and_close(&name, managed);
            }
        }
    }

    fn flush_and_close(name: &str, managed: ManagedEngine) {
        // Flush journal before dropping.
        if let Some(ref jtx) = managed._journal_tx {
            let (ack_tx, ack_rx) = std::sync::mpsc::sync_channel(1);
            if jtx.send(journal::JournalCommand::Flush(ack_tx)).is_ok() {
                ack_rx.recv().ok();
            }
        }
        // ManagedEngine drops here:
        //   _journal_tx drops -> journal writer thread exits
        //   engine drops -> Backend drops -> Database drops -> buffer pool freed
        info!("Closed database '{}'", name);
    }
}

pub struct ManagerStats {
    pub open_databases: usize,
    pub max_open: usize,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_config(dir: &std::path::Path) -> EngineManagerConfig {
        EngineManagerConfig {
            data_root: dir.to_path_buf(),
            num_readers: 2,
            max_open: 3,
            idle_timeout: Duration::from_millis(100),
            journal_enabled: false,
            journal_segment_mb: 64,
            journal_fsync_ms: 100,
            journal_encryption_key: None,
            journal_compress: false,
            journal_compress_level: 3,
        }
    }

    #[tokio::test]
    async fn open_on_demand() {
        let dir = tempfile::tempdir().unwrap();
        let mgr = EngineManager::new(test_config(dir.path()));

        let engine1 = mgr.get("db1").await.unwrap();
        let engine2 = mgr.get("db1").await.unwrap();

        // Same Arc.
        assert!(Arc::ptr_eq(&engine1, &engine2));
        assert_eq!(mgr.stats().await.open_databases, 1);
    }

    #[tokio::test]
    async fn idle_eviction() {
        let dir = tempfile::tempdir().unwrap();
        let mgr = EngineManager::new(test_config(dir.path()));

        let engine = mgr.get("db1").await.unwrap();
        // Run a query to verify it works.
        engine
            .execute("RETURN 1 AS n".to_string(), None)
            .await
            .unwrap();
        // Drop our reference so strong_count == 1 (only the manager holds it).
        drop(engine);

        // Wait for idle timeout.
        tokio::time::sleep(Duration::from_millis(150)).await;
        mgr.cleanup().await;

        assert_eq!(mgr.stats().await.open_databases, 0);

        // Reopens on next get().
        let engine = mgr.get("db1").await.unwrap();
        let result = engine
            .execute("RETURN 2 AS n".to_string(), None)
            .await
            .unwrap();
        assert_eq!(result.rows.len(), 1);
    }

    #[tokio::test]
    async fn lru_eviction_at_capacity() {
        let dir = tempfile::tempdir().unwrap();
        let mgr = EngineManager::new(test_config(dir.path()));

        // Open 3 databases (at capacity).
        let _e1 = mgr.get("db1").await.unwrap();
        drop(_e1);
        tokio::time::sleep(Duration::from_millis(10)).await;

        let _e2 = mgr.get("db2").await.unwrap();
        drop(_e2);
        tokio::time::sleep(Duration::from_millis(10)).await;

        let _e3 = mgr.get("db3").await.unwrap();
        drop(_e3);
        tokio::time::sleep(Duration::from_millis(10)).await;

        assert_eq!(mgr.stats().await.open_databases, 3);

        // Opening a 4th should evict the LRU (db1).
        let _e4 = mgr.get("db4").await.unwrap();
        assert_eq!(mgr.stats().await.open_databases, 3);

        // db1 was evicted, db2/db3/db4 remain.
        let engines = mgr.engines.read().await;
        assert!(!engines.contains_key("db1"));
        assert!(engines.contains_key("db2"));
        assert!(engines.contains_key("db3"));
        assert!(engines.contains_key("db4"));
    }

    #[tokio::test]
    async fn no_evict_while_in_use() {
        let dir = tempfile::tempdir().unwrap();
        let mgr = EngineManager::new(test_config(dir.path()));

        // Open and hold a reference.
        let engine = mgr.get("db1").await.unwrap();

        // Wait past idle timeout.
        tokio::time::sleep(Duration::from_millis(150)).await;
        mgr.cleanup().await;

        // Should NOT be evicted because we hold a reference (strong_count > 1).
        assert_eq!(mgr.stats().await.open_databases, 1);
        drop(engine);
    }

    #[tokio::test]
    async fn explicit_close() {
        let dir = tempfile::tempdir().unwrap();
        let mgr = EngineManager::new(test_config(dir.path()));

        mgr.get("db1").await.unwrap();
        assert_eq!(mgr.stats().await.open_databases, 1);

        let closed = mgr.close("db1").await;
        assert!(closed);
        assert_eq!(mgr.stats().await.open_databases, 0);

        // Closing again returns false.
        assert!(!mgr.close("db1").await);
    }

    #[tokio::test]
    async fn close_then_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let mgr = EngineManager::new(test_config(dir.path()));

        // Open, write data, close.
        let engine = mgr.get("db1").await.unwrap();
        engine
            .execute(
                "CREATE NODE TABLE CloseTest(val STRING, PRIMARY KEY(val))".to_string(),
                None,
            )
            .await
            .unwrap();
        engine
            .execute(
                "CREATE (:CloseTest {val: 'survive'})".to_string(),
                None,
            )
            .await
            .unwrap();
        drop(engine);

        let closed = mgr.close("db1").await;
        assert!(closed);
        assert_eq!(mgr.stats().await.open_databases, 0);

        // Reopen immediately — should succeed without file lock conflict.
        let engine = mgr.get("db1").await.unwrap();
        let result = engine
            .execute("MATCH (c:CloseTest) RETURN c.val".to_string(), None)
            .await
            .unwrap();
        assert_eq!(result.rows.len(), 1);
    }

    #[tokio::test]
    async fn capacity_error_when_all_in_use() {
        let dir = tempfile::tempdir().unwrap();
        let mgr = EngineManager::new(test_config(dir.path()));

        // Open 3 databases and hold references (strong_count > 1).
        let _e1 = mgr.get("db1").await.unwrap();
        let _e2 = mgr.get("db2").await.unwrap();
        let _e3 = mgr.get("db3").await.unwrap();

        // Opening a 4th should fail — all 3 are in use, can't evict.
        let err = mgr.get("db4").await.err().expect("should fail at capacity");
        assert!(err.contains("Too many open databases"), "got: {err}");
    }
}
