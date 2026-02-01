use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, oneshot};
use tracing::error;

use crate::backend::Backend;
use crate::journal::{self, JournalCommand, JournalSender, PendingEntry};
use crate::protocol::{BatchResultEntry, BatchStatement, ServerMessage};
use crate::values::{self, json_params_to_lbug, GraphValue};

/// An operation to be executed on the session's dedicated connection.
pub enum SessionOp {
    Execute {
        query: String,
        params: Option<serde_json::Value>,
        request_id: Option<String>,
        fetch_size: Option<u64>,
    },
    Begin {
        mode: Option<String>,
        request_id: Option<String>,
    },
    Commit {
        request_id: Option<String>,
    },
    Rollback {
        request_id: Option<String>,
    },
    Batch {
        statements: Vec<BatchStatement>,
        request_id: Option<String>,
    },
    Fetch {
        stream_id: u64,
        request_id: Option<String>,
    },
    CloseStream {
        stream_id: u64,
        request_id: Option<String>,
    },
}

/// A request sent from the async handler to the blocking session worker.
pub struct SessionRequest {
    pub op: SessionOp,
    pub reply: oneshot::Sender<ServerMessage>,
}

/// Handle for sending operations to a session worker.
pub struct SessionHandle {
    tx: mpsc::UnboundedSender<SessionRequest>,
}

impl SessionHandle {
    /// Send an operation to the session worker and await the response.
    pub async fn request(&self, op: SessionOp) -> Result<ServerMessage, String> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.tx
            .send(SessionRequest {
                op,
                reply: reply_tx,
            })
            .map_err(|_| "Session worker has shut down".to_string())?;
        reply_rx
            .await
            .map_err(|_| "Session worker dropped without responding".to_string())
    }
}

/// Spawn a blocking session worker that owns a database connection.
///
/// Returns a handle for sending operations to the worker. When the handle
/// is dropped (e.g., WebSocket disconnects), the worker exits and the
/// connection is dropped, auto-rolling back any uncommitted transaction.
pub fn spawn_session(
    db: Arc<Backend>,
    cursor_timeout: Duration,
    journal: Option<JournalSender>,
    snapshot_lock: Arc<RwLock<()>>,
) -> SessionHandle {
    let (tx, rx) = mpsc::unbounded_channel();
    let handle =
        tokio::task::spawn_blocking(move || session_worker(db, rx, cursor_timeout, journal, snapshot_lock));
    tokio::spawn(async move {
        if let Err(e) = handle.await {
            error!("Session worker panicked: {e}");
        }
    });
    SessionHandle { tx }
}

/// An open cursor holding a partially-iterated QueryResult.
struct OpenCursor<'db> {
    columns: Vec<String>,
    result: lbug::QueryResult<'db>,
    fetch_size: u64,
    last_accessed: Instant,
    buffered: Option<Vec<GraphValue>>,
}

/// Send a journal entry for a successful mutation.
fn journal_entry(journal: &Option<JournalSender>, query: &str, params: &Option<serde_json::Value>) {
    if let Some(ref tx) = journal {
        if journal::is_mutation(query) {
            let _ = tx.send(JournalCommand::Write(PendingEntry {
                query: query.to_string(),
                params: params.clone(),
            }));
        }
    }
}

/// Blocking session worker loop. Runs on tokio's blocking thread pool.
fn session_worker(
    db: Arc<Backend>,
    rx: mpsc::UnboundedReceiver<SessionRequest>,
    cursor_timeout: Duration,
    journal: Option<JournalSender>,
    snapshot_lock: Arc<RwLock<()>>,
) {
    let conn = match db.connection() {
        Ok(c) => c,
        Err(e) => {
            let msg = format!("Failed to create database connection: {e}");
            let mut rx = rx;
            while let Some(req) = rx.blocking_recv() {
                let _ = req.reply.send(ServerMessage::Error {
                    message: msg.clone(),
                    request_id: None,
                });
            }
            return;
        }
    };
    let mut rx = rx;
    let mut in_transaction = false;
    let mut next_stream_id: u64 = 1;
    let mut cursors: HashMap<u64, OpenCursor<'_>> = HashMap::new();
    let mut tx_journal_buf: Vec<PendingEntry> = Vec::new();

    while let Some(req) = rx.blocking_recv() {
        sweep_expired_cursors(&mut cursors, cursor_timeout);
        let response = match req.op {
            SessionOp::Execute {
                query,
                params,
                request_id,
                fetch_size,
            } => {
                if let Some(fs) = fetch_size {
                    if fs == 0 {
                        ServerMessage::Error {
                            message: "fetch_size must be >= 1".into(),
                            request_id,
                        }
                    } else {
                    // Streaming path — read lock for auto-commit mutations.
                    let _lock = if !in_transaction { Some(snapshot_lock.read().unwrap()) } else { None };
                    match run_query_raw(&conn, &query, params.as_ref()) {
                        Ok(mut raw) => {
                            if in_transaction {
                                if journal::is_mutation(&raw.rewritten_query) {
                                    tx_journal_buf.push(PendingEntry {
                                        query: raw.rewritten_query.clone(),
                                        params: raw.merged_params.clone(),
                                    });
                                }
                            } else {
                                journal_entry(&journal, &raw.rewritten_query, &raw.merged_params);
                            }
                            let mut buffered = None;
                            let (rows, has_more) = take_rows(&mut raw.query_result, fs as usize, &mut buffered);
                            if has_more {
                                let sid = next_stream_id;
                                next_stream_id += 1;
                                cursors.insert(
                                    sid,
                                    OpenCursor {
                                        columns: raw.columns.clone(),
                                        result: raw.query_result,
                                        fetch_size: fs,
                                        last_accessed: Instant::now(),
                                        buffered,
                                    },
                                );
                                ServerMessage::Result {
                                    columns: raw.columns,
                                    rows,
                                    timing_ms: raw.timing_ms,
                                    request_id,
                                    stream_id: Some(sid),
                                    has_more: Some(true),
                                }
                            } else {
                                ServerMessage::Result {
                                    columns: raw.columns,
                                    rows,
                                    timing_ms: raw.timing_ms,
                                    request_id,
                                    stream_id: None,
                                    has_more: None,
                                }
                            }
                        }
                        Err(e) => ServerMessage::Error {
                            message: e,
                            request_id,
                        },
                    }
                    }
                } else {
                    // Acquire read lock for auto-commit mutations so snapshot
                    // can't run between query execution and journaling.
                    let _lock = if !in_transaction { Some(snapshot_lock.read().unwrap()) } else { None };
                    match run_query(&conn, &query, params.as_ref()) {
                        Ok(eq) => {
                            if in_transaction {
                                if journal::is_mutation(&eq.rewritten_query) {
                                    tx_journal_buf.push(PendingEntry {
                                        query: eq.rewritten_query.clone(),
                                        params: eq.merged_params.clone(),
                                    });
                                }
                            } else {
                                journal_entry(&journal, &eq.rewritten_query, &eq.merged_params);
                            }
                            ServerMessage::Result {
                                columns: eq.columns,
                                rows: eq.rows,
                                timing_ms: eq.timing_ms,
                                request_id,
                                stream_id: None,
                                has_more: None,
                            }
                        }
                        Err(e) => ServerMessage::Error {
                            message: e,
                            request_id,
                        },
                    }
                }
            }

            SessionOp::Begin { mode, request_id } => {
                if in_transaction {
                    ServerMessage::Error {
                        message: "Transaction already active".into(),
                        request_id,
                    }
                } else if matches!(mode.as_deref(), Some(m) if m != "read") {
                    ServerMessage::Error {
                        message: format!(
                            "Invalid transaction mode: {}",
                            mode.as_deref().unwrap()
                        ),
                        request_id,
                    }
                } else {
                    let stmt = if mode.as_deref() == Some("read") {
                        "BEGIN TRANSACTION READ ONLY"
                    } else {
                        "BEGIN TRANSACTION"
                    };
                    match conn.query(stmt) {
                        Ok(_) => {
                            in_transaction = true;
                            ServerMessage::BeginOk { request_id }
                        }
                        Err(e) => ServerMessage::Error {
                            message: format!("{e}"),
                            request_id,
                        },
                    }
                }
            }

            SessionOp::Commit { request_id } => {
                if !in_transaction {
                    ServerMessage::Error {
                        message: "No active transaction".into(),
                        request_id,
                    }
                } else {
                    // Read lock around commit + journal flush.
                    let _lock = snapshot_lock.read().unwrap();
                    match conn.query("COMMIT") {
                        Ok(_) => {
                            in_transaction = false;
                            for entry in tx_journal_buf.drain(..) {
                                if let Some(ref tx) = journal {
                                    let _ = tx.send(JournalCommand::Write(entry));
                                }
                            }
                            ServerMessage::CommitOk { request_id }
                        }
                        Err(e) => ServerMessage::Error {
                            message: format!("{e}"),
                            request_id,
                        },
                    }
                }
            }

            SessionOp::Rollback { request_id } => {
                if !in_transaction {
                    ServerMessage::Error {
                        message: "No active transaction".into(),
                        request_id,
                    }
                } else {
                    match conn.query("ROLLBACK") {
                        Ok(_) => {
                            in_transaction = false;
                            tx_journal_buf.clear();
                            ServerMessage::RollbackOk { request_id }
                        }
                        Err(e) => ServerMessage::Error {
                            message: format!("{e}"),
                            request_id,
                        },
                    }
                }
            }

            SessionOp::Batch {
                statements,
                request_id,
            } => {
                // Read lock for auto-commit batch mutations.
                let _lock = if !in_transaction { Some(snapshot_lock.read().unwrap()) } else { None };
                let (msg, entries) = execute_batch(&conn, &statements, request_id);
                if in_transaction {
                    tx_journal_buf.extend(entries);
                } else {
                    for entry in entries {
                        if let Some(ref tx) = journal {
                            let _ = tx.send(JournalCommand::Write(entry));
                        }
                    }
                }
                msg
            }

            SessionOp::Fetch {
                stream_id,
                request_id,
            } => {
                match cursors.get_mut(&stream_id) {
                    Some(cursor) => {
                        cursor.last_accessed = Instant::now();
                        let fs = cursor.fetch_size as usize;
                        let (rows, has_more) = take_rows(&mut cursor.result, fs, &mut cursor.buffered);
                        let columns = cursor.columns.clone();
                        if has_more {
                            ServerMessage::Result {
                                columns,
                                rows,
                                timing_ms: 0.0,
                                request_id,
                                stream_id: Some(stream_id),
                                has_more: Some(true),
                            }
                        } else {
                            cursors.remove(&stream_id);
                            ServerMessage::Result {
                                columns,
                                rows,
                                timing_ms: 0.0,
                                request_id,
                                stream_id: None,
                                has_more: None,
                            }
                        }
                    }
                    None => ServerMessage::Error {
                        message: format!("Unknown stream_id: {stream_id}"),
                        request_id,
                    },
                }
            }

            SessionOp::CloseStream {
                stream_id,
                request_id,
            } => {
                if cursors.remove(&stream_id).is_some() {
                    ServerMessage::CloseStreamOk {
                        stream_id,
                        request_id,
                    }
                } else {
                    ServerMessage::Error {
                        message: format!("Unknown stream_id: {stream_id}"),
                        request_id,
                    }
                }
            }
        };

        // If the receiver is gone, stop.
        let _ = req.reply.send(response);
    }

    // rx closed — WebSocket disconnected. Connection drops here,
    // auto-rolling back any uncommitted transaction.
}

/// Result of executing a query via `run_query_raw`, including journal-relevant info.
pub(crate) struct RawExecution<'db> {
    pub query_result: lbug::QueryResult<'db>,
    pub columns: Vec<String>,
    pub timing_ms: f64,
    /// The rewritten query (with non-deterministic functions replaced).
    pub rewritten_query: String,
    /// Merged params (user + generated). None if no params.
    pub merged_params: Option<serde_json::Value>,
}

/// Result of executing a query via `run_query`, with all rows materialized.
pub(crate) struct ExecutedQuery {
    pub columns: Vec<String>,
    pub rows: Vec<Vec<GraphValue>>,
    pub timing_ms: f64,
    /// The rewritten query (with non-deterministic functions replaced).
    pub rewritten_query: String,
    /// Merged params (user + generated). None if no params.
    pub merged_params: Option<serde_json::Value>,
}

/// Execute a query and return the raw QueryResult, column names, timing,
/// and the rewritten query + merged params for journaling.
fn run_query_raw<'db>(
    conn: &lbug::Connection<'db>,
    query: &str,
    params: Option<&serde_json::Value>,
) -> Result<RawExecution<'db>, String> {
    // Rewrite non-deterministic functions (gen_random_uuid, current_timestamp, current_date)
    // to parameter references with concrete generated values.
    let rewrite = crate::rewriter::rewrite_query(query);
    let query_str = rewrite.query.clone();
    let query = &rewrite.query;
    let merged = crate::rewriter::merge_params(params, &rewrite.generated_params);
    let params = merged.as_ref();

    let start = Instant::now();

    let has_params = params
        .and_then(|p| p.as_object())
        .map_or(false, |o| !o.is_empty());

    let result = if has_params {
        match json_params_to_lbug(params.unwrap()) {
            Ok(owned) => {
                let refs: Vec<(&str, lbug::Value)> =
                    owned.iter().map(|(k, v)| (k.as_str(), v.clone())).collect();
                match conn.prepare(query) {
                    Ok(mut prepared) => {
                        conn.execute(&mut prepared, refs).map_err(|e| format!("{e}"))
                    }
                    Err(e) => Err(format!("{e}")),
                }
            }
            Err(e) => Err(e),
        }
    } else {
        conn.query(query).map_err(|e| format!("{e}"))
    };

    let timing_ms = start.elapsed().as_secs_f64() * 1000.0;
    let merged_params = merged.clone();

    match result {
        Ok(qr) => {
            let columns = qr.get_column_names();
            Ok(RawExecution {
                query_result: qr,
                columns,
                timing_ms,
                rewritten_query: query_str,
                merged_params,
            })
        }
        Err(e) => Err(e),
    }
}

/// Take up to `limit` rows from a QueryResult. Returns (rows, has_more).
/// Uses `buf` to buffer a peeked row across calls — pass `&mut None` for one-shot reads.
fn take_rows(
    result: &mut lbug::QueryResult<'_>,
    limit: usize,
    buf: &mut Option<Vec<GraphValue>>,
) -> (Vec<Vec<GraphValue>>, bool) {
    let mut rows = Vec::new();
    if let Some(row) = buf.take() {
        rows.push(row);
    }
    while rows.len() < limit {
        match result.next() {
            Some(row) => rows.push(row.iter().map(|v| values::from_lbug_value(v)).collect()),
            None => return (rows, false),
        }
    }
    // Peek one more to determine has_more without losing the row.
    match result.next() {
        Some(row) => {
            *buf = Some(row.iter().map(|v| values::from_lbug_value(v)).collect());
            (rows, true)
        }
        None => (rows, false),
    }
}

/// Remove cursors that have been idle longer than `timeout`.
fn sweep_expired_cursors(cursors: &mut HashMap<u64, OpenCursor<'_>>, timeout: Duration) {
    let now = Instant::now();
    cursors.retain(|_, cursor| now.duration_since(cursor.last_accessed) < timeout);
}

/// Execute a single query, collecting all rows.
pub(crate) fn run_query(
    conn: &lbug::Connection<'_>,
    query: &str,
    params: Option<&serde_json::Value>,
) -> Result<ExecutedQuery, String> {
    let mut raw = run_query_raw(conn, query, params)?;
    let (rows, _) = take_rows(&mut raw.query_result, usize::MAX, &mut None);
    Ok(ExecutedQuery {
        columns: raw.columns,
        rows,
        timing_ms: raw.timing_ms,
        rewritten_query: raw.rewritten_query,
        merged_params: raw.merged_params,
    })
}


/// Execute a batch of statements sequentially. Stops on first error.
/// Returns the result message and a vec of journal entries for successful mutations.
fn execute_batch(
    conn: &lbug::Connection<'_>,
    statements: &[BatchStatement],
    request_id: Option<String>,
) -> (ServerMessage, Vec<PendingEntry>) {
    let mut results = Vec::with_capacity(statements.len());
    let mut journal_entries = Vec::new();

    for stmt in statements {
        match run_query(conn, &stmt.query, stmt.params.as_ref()) {
            Ok(eq) => {
                if journal::is_mutation(&eq.rewritten_query) {
                    journal_entries.push(PendingEntry {
                        query: eq.rewritten_query,
                        params: eq.merged_params,
                    });
                }
                results.push(BatchResultEntry::Result {
                    columns: eq.columns,
                    rows: eq.rows,
                    timing_ms: eq.timing_ms,
                });
            }
            Err(e) => {
                results.push(BatchResultEntry::Error { message: e });
                break;
            }
        }
    }

    (ServerMessage::BatchResult { results, request_id }, journal_entries)
}
