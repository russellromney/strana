//! Engine: per-database concurrency control layer.
//!
//! Wraps a `Backend` with:
//! - Read semaphore (N concurrent reads)
//! - Write mutex (1 serialized write at a time, auto-journaled)
//! - Snapshot lock (shared across reads/writes, exclusive for CHECKPOINT)
//!
//! Uses `tokio::spawn_blocking` from the shared thread pool — zero threads when idle,
//! scales across many databases in a multi-tenant process.

use std::sync::{Arc, RwLock};

use crate::backend::Backend;
use crate::journal::{self, JournalSender};
use crate::query::{self, ExecutedQuery};

/// Per-database query execution engine with concurrency control.
pub struct Engine {
    backend: Arc<Backend>,
    snapshot_lock: Arc<RwLock<()>>,
    journal: Option<JournalSender>,
    read_semaphore: tokio::sync::Semaphore,
    write_mutex: tokio::sync::Mutex<()>,
}

impl Engine {
    /// Create a new Engine wrapping the given backend.
    ///
    /// - `num_readers`: max concurrent read queries (semaphore permits)
    /// - `journal`: optional journal sender for write-ahead logging
    pub fn new(
        backend: Arc<Backend>,
        num_readers: usize,
        journal: Option<JournalSender>,
    ) -> Self {
        Self {
            backend,
            snapshot_lock: Arc::new(RwLock::new(())),
            journal,
            read_semaphore: tokio::sync::Semaphore::new(num_readers),
            write_mutex: tokio::sync::Mutex::new(()),
        }
    }

    /// Execute an auto-commit query, routing reads vs writes automatically.
    ///
    /// - Reads: bounded by semaphore (up to N concurrent).
    /// - Writes: serialized by mutex (1 at a time), auto-journaled on success.
    pub async fn execute(
        &self,
        query: String,
        params: Option<serde_json::Value>,
    ) -> Result<ExecutedQuery, String> {
        if journal::is_mutation(&query) {
            self.execute_write(query, params).await
        } else {
            self.execute_read(query, params).await
        }
    }

    async fn execute_read(
        &self,
        query: String,
        params: Option<serde_json::Value>,
    ) -> Result<ExecutedQuery, String> {
        let _permit = self
            .read_semaphore
            .acquire()
            .await
            .map_err(|_| "Engine closed".to_string())?;
        let backend = self.backend.clone();
        let lock = self.snapshot_lock.clone();
        tokio::task::spawn_blocking(move || {
            let conn = backend.connection()?;
            let _guard = lock.read().unwrap_or_else(|e| e.into_inner());
            query::run_query(&conn, &query, params.as_ref())
        })
        .await
        .map_err(|e| format!("Task panicked: {e}"))?
    }

    async fn execute_write(
        &self,
        query: String,
        params: Option<serde_json::Value>,
    ) -> Result<ExecutedQuery, String> {
        let _guard = self.write_mutex.lock().await;
        let backend = self.backend.clone();
        let lock = self.snapshot_lock.clone();
        let journal = self.journal.clone();
        tokio::task::spawn_blocking(move || {
            let conn = backend.connection()?;
            let _snap = lock.read().unwrap_or_else(|e| e.into_inner());
            let result = query::run_query(&conn, &query, params.as_ref());
            if let Ok(ref eq) = result {
                query::journal_entry(&journal, &eq.rewritten_query, &eq.merged_params);
            }
            result
        })
        .await
        .map_err(|e| format!("Task panicked: {e}"))?
    }

    /// Create a dedicated connection for Bolt sessions and HTTP transactions.
    /// The caller owns the connection for its lifetime.
    pub fn connection(&self) -> Result<lbug::Connection<'_>, String> {
        self.backend.connection()
    }

    /// Snapshot lock — take a read lock for queries, write lock for CHECKPOINT.
    pub fn snapshot_lock(&self) -> &Arc<RwLock<()>> {
        &self.snapshot_lock
    }

    /// Journal sender (None if journaling is disabled).
    pub fn journal(&self) -> &Option<JournalSender> {
        &self.journal
    }

    /// Underlying backend (for snapshot creation and restore paths).
    pub fn backend(&self) -> &Arc<Backend> {
        &self.backend
    }

    /// Acquire exclusive write access (blocking).
    ///
    /// Use from `spawn_blocking` contexts (Bolt sessions, HTTP transaction workers).
    /// For async contexts, use `execute()` which acquires the lock internally.
    ///
    /// Hold the guard for the duration of a write transaction (BEGIN→COMMIT),
    /// or for a single auto-commit mutation.
    pub fn acquire_writer(&self) -> tokio::sync::MutexGuard<'_, ()> {
        self.write_mutex.blocking_lock()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;

    fn temp_engine(dir: &Path, num_readers: usize) -> Engine {
        let db_path = dir.join("db");
        // Don't create the db dir — lbug creates it internally.
        let backend = Arc::new(Backend::open(&db_path).unwrap());
        Engine::new(backend, num_readers, None)
    }

    #[tokio::test]
    async fn read_query_through_pool() {
        let dir = tempfile::tempdir().unwrap();
        let engine = temp_engine(dir.path(), 4);
        let result = engine
            .execute("RETURN 1 AS n".to_string(), None)
            .await
            .unwrap();
        assert_eq!(result.columns, vec!["n"]);
        assert_eq!(result.rows.len(), 1);
    }

    #[tokio::test]
    async fn write_query_through_pool() {
        let dir = tempfile::tempdir().unwrap();
        let engine = temp_engine(dir.path(), 4);

        // Create table + insert.
        engine
            .execute(
                "CREATE NODE TABLE EngTest(val STRING, PRIMARY KEY(val))".to_string(),
                None,
            )
            .await
            .unwrap();
        engine
            .execute(
                "CREATE (:EngTest {val: 'hello'})".to_string(),
                None,
            )
            .await
            .unwrap();

        // Read it back.
        let result = engine
            .execute(
                "MATCH (e:EngTest) RETURN e.val".to_string(),
                None,
            )
            .await
            .unwrap();
        assert_eq!(result.rows.len(), 1);
    }

    #[tokio::test]
    async fn concurrent_reads() {
        let dir = tempfile::tempdir().unwrap();
        let engine = Arc::new(temp_engine(dir.path(), 4));

        let mut handles = Vec::new();
        for _ in 0..20 {
            let eng = engine.clone();
            handles.push(tokio::spawn(async move {
                eng.execute("RETURN 1 AS n".to_string(), None).await
            }));
        }

        for h in handles {
            let result = h.await.unwrap().unwrap();
            assert_eq!(result.rows.len(), 1);
        }
    }

    #[tokio::test]
    async fn mutation_routing() {
        let dir = tempfile::tempdir().unwrap();
        let engine = temp_engine(dir.path(), 4);

        // Write goes through writer.
        engine
            .execute(
                "CREATE NODE TABLE MutTest(val INT64, PRIMARY KEY(val))".to_string(),
                None,
            )
            .await
            .unwrap();
        engine
            .execute("CREATE (:MutTest {val: 42})".to_string(), None)
            .await
            .unwrap();

        // Read goes through reader pool — should see the write.
        let result = engine
            .execute("MATCH (m:MutTest) RETURN m.val".to_string(), None)
            .await
            .unwrap();
        assert_eq!(result.rows.len(), 1);
    }

    #[tokio::test]
    async fn dedicated_connection() {
        let dir = tempfile::tempdir().unwrap();
        let engine = temp_engine(dir.path(), 4);

        // connection() works independently of the pool.
        let conn = engine.connection().unwrap();
        let result = conn.query("RETURN 1").unwrap();
        let cols = result.get_column_names();
        assert_eq!(cols.len(), 1);
    }

    #[tokio::test]
    async fn snapshot_lock_blocks_queries() {
        let dir = tempfile::tempdir().unwrap();
        let engine = Arc::new(temp_engine(dir.path(), 4));

        // Take write lock (simulates CHECKPOINT).
        let lock = engine.snapshot_lock().clone();
        let _guard = lock.write().unwrap();

        // Spawn a read — it should block because the snapshot write lock is held.
        let eng = engine.clone();
        let handle = tokio::spawn(async move {
            eng.execute("RETURN 1 AS n".to_string(), None).await
        });

        // Give it a moment to confirm it's blocked.
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        assert!(!handle.is_finished());

        // Release the lock — query should complete.
        drop(_guard);
        let result = handle.await.unwrap().unwrap();
        assert_eq!(result.rows.len(), 1);
    }
}
