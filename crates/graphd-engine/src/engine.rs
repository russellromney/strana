//! Engine: per-database concurrency control layer.
//!
//! Wraps a `Backend` with:
//! - Read semaphore (N concurrent reads)
//! - Write mutex (1 serialized write at a time, auto-journaled)
//! - Snapshot lock (shared across reads/writes, exclusive for CHECKPOINT)
//! - Optional statement-level sandboxing
//!
//! Uses `tokio::spawn_blocking` from the shared thread pool — zero threads when idle,
//! scales across many databases in a multi-tenant process.

use std::sync::{Arc, RwLock};

use crate::backend::Backend;
use crate::error::GraphdError;
use crate::journal_types::{self as journal, JournalSender};
use crate::query::{self, ExecutedQuery};

use crate::sandbox::SandboxConfig;

/// Per-database query execution engine with concurrency control.
pub struct Engine {
    backend: Arc<Backend>,
    snapshot_lock: Arc<RwLock<()>>,
    journal: Option<JournalSender>,
    sandbox: Option<Arc<SandboxConfig>>,
    read_semaphore: tokio::sync::Semaphore,
    write_mutex: tokio::sync::Mutex<()>,
}

impl Engine {
    /// Create a new Engine wrapping the given backend.
    ///
    /// - `num_readers`: max concurrent read queries (semaphore permits)
    /// - `journal`: optional journal sender for write-ahead logging
    /// - `sandbox`: optional statement-level sandbox for multi-tenant safety
    pub fn new(
        backend: Arc<Backend>,
        num_readers: usize,
        journal: Option<JournalSender>,
        sandbox: Option<SandboxConfig>,
    ) -> Self {
        Self {
            backend,
            snapshot_lock: Arc::new(RwLock::new(())),
            journal,
            sandbox: sandbox.map(Arc::new),
            read_semaphore: tokio::sync::Semaphore::new(num_readers),
            write_mutex: tokio::sync::Mutex::new(()),
        }
    }

    /// Execute an auto-commit query, routing reads vs writes automatically.
    ///
    /// - Reads: bounded by semaphore (up to N concurrent).
    /// - Writes: serialized by mutex (1 at a time), auto-journaled on success.
    ///
    /// If a sandbox is configured, queries are checked before execution.
    pub async fn execute(
        &self,
        query: String,
        params: Option<serde_json::Value>,
    ) -> Result<ExecutedQuery, GraphdError> {
        // Sandbox check: prepare the query and verify the statement type is allowed.
        if let Some(ref sandbox) = self.sandbox {
            let backend = self.backend.clone();
            let q = query.clone();
            let sandbox = sandbox.clone();
            tokio::task::spawn_blocking(move || {
                let conn = backend.connection()?;
                sandbox.check_query(&conn, &q)
            })
            .await
            .map_err(|e| GraphdError::DatabaseError(format!("Task panicked: {e}")))?
            ?;
        }

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
    ) -> Result<ExecutedQuery, GraphdError> {
        let _permit = self
            .read_semaphore
            .acquire()
            .await
            .map_err(|_| GraphdError::DatabaseUnavailable("Engine closed".into()))?;
        let backend = self.backend.clone();
        let lock = self.snapshot_lock.clone();
        tokio::task::spawn_blocking(move || {
            let conn = backend.connection()?;
            let _guard = lock.read().unwrap_or_else(|e| e.into_inner());
            query::run_query(&conn, &query, params.as_ref())
        })
        .await
        .map_err(|e| GraphdError::DatabaseError(format!("Task panicked: {e}")))?
    }

    async fn execute_write(
        &self,
        query: String,
        params: Option<serde_json::Value>,
    ) -> Result<ExecutedQuery, GraphdError> {
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
        .map_err(|e| GraphdError::DatabaseError(format!("Task panicked: {e}")))?
    }

    /// Create a dedicated connection for Bolt sessions and HTTP transactions.
    /// The caller owns the connection for its lifetime.
    pub fn connection(&self) -> Result<lbug::Connection<'_>, GraphdError> {
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

    /// Sandbox config (for manual query checks in Bolt transaction mode).
    pub fn sandbox(&self) -> Option<&Arc<SandboxConfig>> {
        self.sandbox.as_ref()
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
        let backend = Arc::new(Backend::open(&db_path).unwrap());
        Engine::new(backend, num_readers, None, None)
    }

    fn temp_engine_sandboxed(dir: &Path) -> Engine {
        let db_path = dir.join("db");
        let backend = Arc::new(Backend::open(&db_path).unwrap());
        let sandbox = SandboxConfig::multi_tenant();
        Engine::new(backend, 4, None, Some(sandbox))
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

        let conn = engine.connection().unwrap();
        let result = conn.query("RETURN 1").unwrap();
        let cols = result.get_column_names();
        assert_eq!(cols.len(), 1);
    }

    #[tokio::test]
    async fn snapshot_lock_blocks_queries() {
        let dir = tempfile::tempdir().unwrap();
        let engine = Arc::new(temp_engine(dir.path(), 4));

        let lock = engine.snapshot_lock().clone();
        let _guard = lock.write().unwrap();

        let eng = engine.clone();
        let handle = tokio::spawn(async move {
            eng.execute("RETURN 1 AS n".to_string(), None).await
        });

        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        assert!(!handle.is_finished());

        drop(_guard);
        let result = handle.await.unwrap().unwrap();
        assert_eq!(result.rows.len(), 1);
    }

    #[tokio::test]
    async fn sandboxed_allows_read() {
        let dir = tempfile::tempdir().unwrap();
        let engine = temp_engine_sandboxed(dir.path());
        let result = engine.execute("RETURN 1 AS n".to_string(), None).await;
        assert!(result.is_ok(), "Read queries should be allowed");
    }

    #[tokio::test]
    async fn sandboxed_allows_schema() {
        let dir = tempfile::tempdir().unwrap();
        let engine = temp_engine_sandboxed(dir.path());
        let result = engine
            .execute(
                "CREATE NODE TABLE SandboxTest(id INT64, PRIMARY KEY(id))".to_string(),
                None,
            )
            .await;
        assert!(result.is_ok(), "Schema creation should be allowed");
    }

    #[tokio::test]
    async fn sandboxed_blocks_copy_from() {
        let dir = tempfile::tempdir().unwrap();
        let engine = temp_engine_sandboxed(dir.path());

        // Create a table first so COPY FROM parses
        engine
            .execute(
                "CREATE NODE TABLE CopyTest(id INT64, PRIMARY KEY(id))".to_string(),
                None,
            )
            .await
            .unwrap();

        // COPY FROM is blocked either by the sandbox (type check) or by lbug's binder
        // (file format validation). Either way, the query must not execute.
        let result = engine
            .execute("COPY CopyTest FROM '/etc/passwd'".to_string(), None)
            .await;
        assert!(result.is_err(), "COPY FROM should be blocked");
    }
}
