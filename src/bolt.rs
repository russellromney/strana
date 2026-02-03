use std::sync::{Arc, RwLock};

use bytes::Bytes;
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufStream};
use tokio::net::{TcpListener, TcpStream};
use tokio_util::compat::TokioAsyncReadCompatExt;
use tracing::{debug, error, info, warn};

use crate::auth::TokenStore;
use crate::engine::Engine;
use crate::journal::{self, JournalCommand, JournalSender, PendingEntry};
use crate::query;
use crate::values::{self, GraphValue};

use bolt_proto::{Message, Value};
use bolt_proto::message::{Record, Success, Failure};
use std::collections::HashMap;

/// Bolt protocol version negotiated with the client.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BoltVersion {
    V4_4,
    /// Bolt 5.x where the u8 is the minor version (0-7 for Bolt 5.0-5.7)
    V5(u8),
}

/// Shared state for the Bolt listener.
#[derive(Clone)]
pub struct BoltState {
    pub engine: Arc<Engine>,
    pub tokens: Arc<TokenStore>,
    pub conn_semaphore: Arc<tokio::sync::Semaphore>,
}

/// Start the Bolt TCP listener.
pub async fn listen(addr: String, state: BoltState, mut shutdown: tokio::sync::watch::Receiver<bool>) {
    let listener = match TcpListener::bind(&addr).await {
        Ok(l) => l,
        Err(e) => {
            error!("Failed to bind Bolt listener on {addr}: {e}");
            return;
        }
    };
    info!("Bolt listening on {addr}");

    let semaphore = state.conn_semaphore.clone();

    loop {
        tokio::select! {
            result = listener.accept() => {
                match result {
                    Ok((socket, peer)) => {
                        let permit = match semaphore.clone().try_acquire_owned() {
                            Ok(p) => p,
                            Err(_) => {
                                warn!("Bolt connection limit reached, rejecting {peer}");
                                drop(socket);
                                continue;
                            }
                        };
                        debug!("Bolt connection from {peer}");
                        let state = state.clone();
                        tokio::spawn(async move {
                            let _permit = permit; // held until handler completes
                            if let Err(e) = handle_connection(socket, state).await {
                                debug!("Bolt connection error: {e}");
                            }
                        });
                    }
                    Err(e) => {
                        warn!("Bolt accept error: {e}");
                    }
                }
            }
            _ = shutdown.changed() => {
                info!("Bolt listener shutting down");
                break;
            }
        }
    }
}

/// Handle the full lifecycle of one Bolt TCP connection.
async fn handle_connection(socket: TcpStream, state: BoltState) -> Result<(), String> {
    let mut stream = BufStream::new(socket);

    // ── Bolt handshake ──
    let mut magic = [0u8; 4];
    stream
        .read_exact(&mut magic)
        .await
        .map_err(|e| format!("read magic: {e}"))?;
    if magic != [0x60, 0x60, 0xB0, 0x17] {
        return Err("Invalid Bolt magic bytes".into());
    }

    // Read 4 proposed versions (16 bytes) and negotiate.
    let mut versions = [0u8; 16];
    stream
        .read_exact(&mut versions)
        .await
        .map_err(|e| format!("read versions: {e}"))?;

    let bolt_version = match negotiate_bolt_version(&versions) {
        Some(version) => version,
        None => {
            // No supported version found — send zero version and close.
            stream
                .write_all(&[0x00, 0x00, 0x00, 0x00])
                .await
                .map_err(|e| format!("write version: {e}"))?;
            stream.flush().await.map_err(|e| format!("flush: {e}"))?;
            return Err("No supported Bolt version in client proposals".into());
        }
    };

    // Send negotiated version back to client
    let version_bytes = match bolt_version {
        BoltVersion::V4_4 => [0x00, 0x00, 0x04, 0x04],
        BoltVersion::V5(minor) => [0x00, 0x00, minor, 0x05],
    };
    stream
        .write_all(&version_bytes)
        .await
        .map_err(|e| format!("write version: {e}"))?;
    stream.flush().await.map_err(|e| format!("flush: {e}"))?;

    let version_str = match bolt_version {
        BoltVersion::V4_4 => "4.4".to_string(),
        BoltVersion::V5(minor) => format!("5.{}", minor),
    };
    info!("Negotiated Bolt version: {}", version_str);

    // ── Spawn blocking session worker ──
    // The Bolt session owns a database connection and runs queries on it.
    let engine = state.engine.clone();
    let tokens = state.tokens.clone();

    // Channel for sending requests from the async reader to the blocking worker.
    let (req_tx, req_rx) = tokio::sync::mpsc::unbounded_channel::<BoltOp>();
    let (resp_tx, mut resp_rx) = tokio::sync::mpsc::unbounded_channel::<Vec<Bytes>>();

    // Blocking session worker.
    let worker_handle = tokio::task::spawn_blocking(move || {
        bolt_session_worker(engine, tokens, req_rx, resp_tx);
    });

    // ── Message loop ──
    let mut compat_stream = stream.compat();
    loop {
        // Parse bolt-rs Message directly from stream (it reads chunks internally)
        let msg = match Message::from_stream(&mut compat_stream).await {
            Ok(m) => m,
            Err(e) => {
                debug!("Bolt read error or client disconnect: {e}");
                break;
            }
        };

        debug!("Received Bolt message: {:?}", msg);

        if req_tx.send(BoltOp::Request(msg)).is_err() {
            break; // worker exited
        }

        // Wait for response(s) from worker.
        match resp_rx.recv().await {
            Some(responses) => {
                debug!("Sending {} chunks to client", responses.len());
                // Responses are already chunked by into_chunks(), just write them directly
                // Need to get back the inner stream for writing
                let mut inner = compat_stream.into_inner();
                for (i, chunk) in responses.iter().enumerate() {
                    debug!("Writing chunk {}: {} bytes", i, chunk.len());
                    if let Err(e) = inner.write_all(chunk).await {
                        debug!("Bolt send error: {e}");
                        return Ok(());
                    }
                }
                if let Err(e) = inner.flush().await {
                    debug!("Bolt flush error: {e}");
                    return Ok(());
                }
                debug!("Successfully sent all chunks");
                // Wrap it again for the next iteration
                compat_stream = inner.compat();
            }
            None => break, // worker exited
        }
    }

    drop(req_tx);
    let _ = worker_handle.await;
    info!("Bolt client disconnected");
    Ok(())
}

/// Operations sent to the blocking session worker.
enum BoltOp {
    Request(Message),
}

/// Blocking session worker that owns the database connection.
fn bolt_session_worker(
    engine: Arc<Engine>,
    tokens: Arc<TokenStore>,
    rx: tokio::sync::mpsc::UnboundedReceiver<BoltOp>,
    tx: tokio::sync::mpsc::UnboundedSender<Vec<Bytes>>,
) {
    let conn = match engine.connection() {
        Ok(c) => c,
        Err(e) => {
            error!("Bolt: failed to create connection: {e}");
            return;
        }
    };
    let journal = engine.journal().clone();
    let snapshot_lock = engine.snapshot_lock().clone();

    let mut rx = rx;
    let mut authenticated = false;
    let mut in_transaction = false;
    let mut tx_journal_buf: Vec<PendingEntry> = Vec::new();

    // Write guard for serializing mutations across all paths.
    // Held for single auto-commit writes, or from BEGIN to COMMIT/ROLLBACK.
    let mut write_guard: Option<tokio::sync::MutexGuard<'_, ()>> = None;

    // Hold onto a QueryResult for PULL streaming.
    let mut current_result: Option<lbug::QueryResult<'_>> = None;
    let mut result_columns: Vec<String> = Vec::new();
    let mut result_consumed = false;

    while let Some(BoltOp::Request(req)) = rx.blocking_recv() {
        // Acquire write guard before executing mutations or starting transactions.
        match &req {
            Message::Run(run) if !in_transaction && write_guard.is_none() => {
                if journal::is_mutation(run.query()) {
                    write_guard = Some(engine.acquire_writer());
                }
            }
            Message::RunWithMetadata(run) if !in_transaction && write_guard.is_none() => {
                if journal::is_mutation(run.statement()) {
                    write_guard = Some(engine.acquire_writer());
                }
            }
            Message::Begin(_) if write_guard.is_none() => {
                write_guard = Some(engine.acquire_writer());
            }
            _ => {}
        }

        let responses = handle_bolt_message(
            &req,
            &conn,
            &tokens,
            &journal,
            &snapshot_lock,
            &mut authenticated,
            &mut in_transaction,
            &mut tx_journal_buf,
            &mut current_result,
            &mut result_columns,
            &mut result_consumed,
        );

        // Release write guard when not in a transaction.
        // Covers: auto-commit write completed, COMMIT, ROLLBACK, failed BEGIN.
        if write_guard.is_some() && !in_transaction {
            write_guard = None;
        }

        if tx.send(responses).is_err() {
            break;
        }
    }
}

/// Handle a single Bolt message, returning response bytes.
///
/// ## Protocol Version Support
///
/// **Bolt 4.4:**
/// - HELLO, RUN, PULL, DISCARD, BEGIN, COMMIT, ROLLBACK, RESET, GOODBYE
///
/// **Bolt 5.0-5.7:**
/// - All Bolt 4.4 messages
/// - LOGON/LOGOFF (replaces HELLO for authentication, introduced in 5.0)
/// - TELEMETRY (client telemetry reporting, introduced in 5.1)
///
/// The implementation is compatible with all messages across all supported versions.
#[allow(deprecated)]
fn handle_bolt_message<'db>(
    req: &Message,
    conn: &lbug::Connection<'db>,
    tokens: &Arc<TokenStore>,
    journal: &Option<JournalSender>,
    snapshot_lock: &Arc<RwLock<()>>,
    authenticated: &mut bool,
    in_transaction: &mut bool,
    tx_journal_buf: &mut Vec<PendingEntry>,
    current_result: &mut Option<lbug::QueryResult<'db>>,
    result_columns: &mut Vec<String>,
    result_consumed: &mut bool,
) -> Vec<Bytes> {
    match req {
        // ── HELLO ──
        Message::Hello(hello) => {
            if tokens.is_empty() {
                // Open access — no tokens configured.
                *authenticated = true;
                info!("Bolt: client connected (open access)");
            } else {
                // Extract credentials from HELLO metadata map.
                // Neo4j drivers send: scheme="basic", principal="neo4j", credentials="<token>"
                match extract_credentials(hello.metadata()) {
                    Some(token) => match tokens.validate(token) {
                        Ok(label) => {
                            *authenticated = true;
                            info!("Bolt: authenticated (label={label})");
                        }
                        Err(_) => {
                            return failure_message(
                                "Neo.ClientError.Security.Unauthorized",
                                "Invalid credentials",
                            );
                        }
                    },
                    None => {
                        return failure_message(
                            "Neo.ClientError.Security.Unauthorized",
                            "Authentication required: provide credentials in HELLO",
                        );
                    }
                }
            }

            let mut metadata = HashMap::new();
            metadata.insert(
                "server".to_string(),
                Value::from(format!("Neo4j/5.x.0-graphd-{}", env!("CARGO_PKG_VERSION"))),
            );
            metadata.insert("connection_id".to_string(), Value::from("bolt-1"));
            debug!("Sending HELLO SUCCESS with metadata: {:?}", metadata);
            let response = success_message(metadata);
            debug!("HELLO SUCCESS response has {} chunks", response.len());
            response
        }

        // ── RUN ──
        Message::Run(run) => {
            if !*authenticated {
                return failure_message(
                    "Neo.ClientError.Security.AuthenticationRateLimit",
                    "Not authenticated",
                );
            }

            let query_str = run.query();

            // Convert Bolt parameters to JSON for our query engine.
            let json_params = bolt_params_to_json(run.parameters());

            // Rewrite + execute via shared query engine.
            let _lock = if !*in_transaction {
                Some(snapshot_lock.read().unwrap_or_else(|e| e.into_inner()))
            } else {
                None
            };

            match query::run_query_raw(conn, query_str, json_params.as_ref()) {
                Ok(raw) => {
                    // Journal the mutation.
                    if *in_transaction {
                        if journal::is_mutation(&raw.rewritten_query) {
                            tx_journal_buf.push(PendingEntry {
                                query: raw.rewritten_query.clone(),
                                params: raw.merged_params.clone(),
                            });
                        }
                    } else {
                        query::journal_entry(journal, &raw.rewritten_query, &raw.merged_params);
                    }

                    let cols = raw.columns.clone();
                    *result_columns = raw.columns;
                    *current_result = Some(raw.query_result);
                    *result_consumed = false;

                    let mut metadata = HashMap::new();
                    metadata.insert("fields".to_string(), Value::from(cols));
                    success_message(metadata)
                }
                Err(e) => {
                    failure_message(
                        "Neo.DatabaseError.General.UnknownError",
                        &format!("Query error: {e}"),
                    )
                }
            }
        }

        // ── RUN_WITH_METADATA ──
        Message::RunWithMetadata(run) => {
            if !*authenticated {
                return failure_message(
                    "Neo.ClientError.Security.AuthenticationRateLimit",
                    "Not authenticated",
                );
            }

            let query_str = run.statement();

            // Convert Bolt parameters to JSON for our query engine.
            let json_params = bolt_params_to_json(run.parameters());

            // Rewrite + execute via shared query engine.
            let _lock = if !*in_transaction {
                Some(snapshot_lock.read().unwrap_or_else(|e| e.into_inner()))
            } else {
                None
            };

            match query::run_query_raw(conn, query_str, json_params.as_ref()) {
                Ok(raw) => {
                    // Journal the mutation.
                    if *in_transaction {
                        if journal::is_mutation(&raw.rewritten_query) {
                            tx_journal_buf.push(PendingEntry {
                                query: raw.rewritten_query.clone(),
                                params: raw.merged_params.clone(),
                            });
                        }
                    } else {
                        query::journal_entry(journal, &raw.rewritten_query, &raw.merged_params);
                    }

                    let cols = raw.columns.clone();
                    *result_columns = raw.columns;
                    *current_result = Some(raw.query_result);
                    *result_consumed = false;

                    let mut metadata = HashMap::new();
                    metadata.insert("fields".to_string(), Value::from(cols));
                    success_message(metadata)
                }
                Err(e) => {
                    failure_message(
                        "Neo.DatabaseError.General.UnknownError",
                        &format!("Query error: {e}"),
                    )
                }
            }
        }

        // ── PULL ──
        Message::Pull(pull) => {
            if current_result.is_none() || *result_consumed {
                return success_message(HashMap::new());
            }

            let max_records = pull.metadata()
                .get("n")
                .and_then(|v| match v {
                    Value::Integer(i) => Some(*i as usize),
                    _ => None,
                })
                .unwrap_or(1000);
            let result = current_result.as_mut().unwrap();

            let mut responses = Vec::new();
            let mut records_sent = 0;

            while records_sent < max_records {
                match result.next() {
                    Some(row) => {
                        let values: Vec<Value> = row
                            .iter()
                            .map(|v| graph_value_to_bolt(&values::from_lbug_value(v)))
                            .collect();
                        let record = Record::new(values);
                        let msg = Message::Record(record);
                        if let Ok(chunks) = msg.into_chunks() {
                            responses.extend(chunks);
                        }
                        records_sent += 1;
                    }
                    None => {
                        *result_consumed = true;
                        break;
                    }
                }
            }

            let has_more = !*result_consumed;
            let mut metadata = HashMap::new();
            metadata.insert("has_more".to_string(), Value::from(has_more));
            let success_chunks = success_message(metadata);
            responses.extend(success_chunks);
            responses
        }

        // ── DISCARD ──
        Message::Discard(_) => {
            *current_result = None;
            *result_consumed = true;
            success_message(HashMap::new())
        }

        // ── BEGIN ──
        Message::Begin(_) => {
            if !*authenticated {
                return failure_message(
                    "Neo.ClientError.Security.AuthenticationRateLimit",
                    "Not authenticated",
                );
            }
            if *in_transaction {
                return failure_message(
                    "Neo.ClientError.Transaction.TransactionNotFound",
                    "Transaction already active",
                );
            }

            match conn.query("BEGIN TRANSACTION") {
                Ok(_) => {
                    *in_transaction = true;
                    success_message(HashMap::new())
                }
                Err(e) => failure_message(
                    "Neo.DatabaseError.General.UnknownError",
                    &format!("BEGIN failed: {e}"),
                ),
            }
        }

        // ── COMMIT ──
        Message::Commit => {
            if !*in_transaction {
                return failure_message(
                    "Neo.ClientError.Transaction.TransactionNotFound",
                    "No active transaction",
                );
            }

            let _lock = snapshot_lock.read().unwrap_or_else(|e| e.into_inner());
            match conn.query("COMMIT") {
                Ok(_) => {
                    *in_transaction = false;
                    for entry in tx_journal_buf.drain(..) {
                        if let Some(ref jtx) = journal {
                            if jtx.send(JournalCommand::Write(entry)).is_err() {
                                warn!("Journal write failed: channel disconnected");
                            }
                        }
                    }
                    success_message(HashMap::new())
                }
                Err(e) => failure_message(
                    "Neo.DatabaseError.General.UnknownError",
                    &format!("COMMIT failed: {e}"),
                ),
            }
        }

        // ── ROLLBACK ──
        Message::Rollback => {
            if !*in_transaction {
                return failure_message(
                    "Neo.ClientError.Transaction.TransactionNotFound",
                    "No active transaction",
                );
            }

            match conn.query("ROLLBACK") {
                Ok(_) => {
                    *in_transaction = false;
                    tx_journal_buf.clear();
                    success_message(HashMap::new())
                }
                Err(e) => failure_message(
                    "Neo.DatabaseError.General.UnknownError",
                    &format!("ROLLBACK failed: {e}"),
                ),
            }
        }

        // ── RESET ──
        Message::Reset => {
            if *in_transaction {
                let _ = conn.query("ROLLBACK");
                *in_transaction = false;
                tx_journal_buf.clear();
            }
            *current_result = None;
            *result_consumed = true;
            success_message(HashMap::new())
        }

        // ── GOODBYE ──
        Message::Goodbye => {
            debug!("Received GOODBYE, closing connection");
            Vec::new() // No response needed, connection will close
        }

        // ── LOGON (Bolt 5.x) ──
        Message::Logon(logon) => {
            // LOGON replaces HELLO in Bolt 5.0+
            // Handle authentication the same way as HELLO
            if tokens.is_empty() {
                // Open access — no tokens configured.
                *authenticated = true;
                info!("Bolt 5.x: client connected (open access)");
            } else {
                // Extract credentials from LOGON metadata map.
                match extract_credentials(logon.metadata()) {
                    Some(token) => match tokens.validate(token) {
                        Ok(label) => {
                            *authenticated = true;
                            info!("Bolt 5.x: authenticated (label={label})");
                        }
                        Err(_) => {
                            return failure_message(
                                "Neo.ClientError.Security.Unauthorized",
                                "Invalid credentials",
                            );
                        }
                    },
                    None => {
                        return failure_message(
                            "Neo.ClientError.Security.Unauthorized",
                            "Authentication required: provide credentials in LOGON",
                        );
                    }
                }
            }

            let mut metadata = HashMap::new();
            metadata.insert(
                "server".to_string(),
                Value::from(format!("Neo4j/5.x.0-graphd-{}", env!("CARGO_PKG_VERSION"))),
            );
            metadata.insert("connection_id".to_string(), Value::from("bolt-1"));
            debug!("Sending LOGON SUCCESS with metadata: {:?}", metadata);
            success_message(metadata)
        }

        // ── LOGOFF (Bolt 5.x) ──
        Message::Logoff(_) => {
            debug!("Received LOGOFF, closing connection");
            success_message(HashMap::new())
        }

        // ── TELEMETRY (Bolt 5.1) ──
        Message::Telemetry(telemetry) => {
            debug!("Received TELEMETRY: api={}", telemetry.api());
            // Accept and acknowledge telemetry data
            success_message(HashMap::new())
        }

        // Catch-all for unsupported messages
        _ => {
            warn!("Unsupported Bolt message: {:?}", req);
            failure_message(
                "Neo.ClientError.Request.Invalid",
                "Unsupported message type",
            )
        }

    }
}

// ─── Response helpers ───

/// Create a SUCCESS response message as chunked bytes.
fn success_message(metadata: HashMap<String, Value>) -> Vec<Bytes> {
    let success = Success::new(metadata);
    let msg = Message::Success(success);
    msg.into_chunks().unwrap_or_else(|e| {
        error!("Failed to serialize SUCCESS: {e}");
        Vec::new()
    })
}

/// Create a FAILURE response message as chunked bytes.
fn failure_message(code: &str, message: &str) -> Vec<Bytes> {
    let mut metadata = HashMap::new();
    metadata.insert("code".to_string(), Value::from(code));
    metadata.insert("message".to_string(), Value::from(message));
    let failure = Failure::new(metadata);
    let msg = Message::Failure(failure);
    msg.into_chunks().unwrap_or_else(|e| {
        error!("Failed to serialize FAILURE: {e}");
        Vec::new()
    })
}

// ─── Value conversion ───

/// Convert Bolt parameters (HashMap<String, Value>) to JSON for the shared query engine.
fn bolt_params_to_json(params: &HashMap<String, Value>) -> Option<serde_json::Value> {
    if params.is_empty() {
        return None;
    }
    let mut map = serde_json::Map::new();
    for (k, v) in params {
        map.insert(k.clone(), bolt_value_to_json(v));
    }
    Some(serde_json::Value::Object(map))
}

/// Convert a bolt-rs Value to JSON.
fn bolt_value_to_json(bolt: &Value) -> serde_json::Value {
    match bolt {
        Value::Boolean(b) => serde_json::Value::Bool(*b),
        Value::Integer(i) => serde_json::json!(*i),
        Value::Float(f) => serde_json::json!(*f),
        Value::String(s) => serde_json::Value::String(s.clone()),
        Value::Null => serde_json::Value::Null,
        Value::List(list) => {
            serde_json::Value::Array(list.iter().map(bolt_value_to_json).collect())
        }
        Value::Map(map) => {
            let mut obj = serde_json::Map::new();
            for (k, v) in map {
                obj.insert(k.clone(), bolt_value_to_json(v));
            }
            serde_json::Value::Object(obj)
        }
        _ => serde_json::Value::Null, // Bytes, DateTime, Node, Relationship, etc.
    }
}

/// Convert a GraphValue to bolt-rs Value for RECORD responses.
fn graph_value_to_bolt(gv: &GraphValue) -> Value {
    match gv {
        GraphValue::Null => Value::Null,
        GraphValue::Bool(b) => Value::from(*b),
        GraphValue::Int(i) => Value::from(*i),
        GraphValue::Float(f) => Value::from(*f),
        GraphValue::String(s) => Value::from(s.clone()),
        GraphValue::List(items) => {
            Value::from(items.iter().map(graph_value_to_bolt).collect::<Vec<_>>())
        }
        GraphValue::Map(entries) => {
            let map: HashMap<String, Value> = entries
                .iter()
                .map(|(k, v)| (k.clone(), graph_value_to_bolt(v)))
                .collect();
            Value::from(map)
        }
        GraphValue::Tagged(_tagged) => {
            // Serialize tagged values (Node, Rel, Path) as maps with $type.
            let json = serde_json::to_value(gv).unwrap_or(serde_json::Value::Null);
            json_to_bolt(&json)
        }
    }
}

/// Convert serde_json::Value to bolt-rs Value (for tagged value serialization).
fn json_to_bolt(v: &serde_json::Value) -> Value {
    match v {
        serde_json::Value::Null => Value::Null,
        serde_json::Value::Bool(b) => Value::from(*b),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Value::from(i)
            } else if let Some(f) = n.as_f64() {
                Value::from(f)
            } else {
                Value::Null
            }
        }
        serde_json::Value::String(s) => Value::from(s.clone()),
        serde_json::Value::Array(arr) => {
            Value::from(arr.iter().map(json_to_bolt).collect::<Vec<_>>())
        }
        serde_json::Value::Object(obj) => {
            let map: HashMap<String, Value> = obj
                .iter()
                .map(|(k, v)| (k.clone(), json_to_bolt(v)))
                .collect();
            Value::from(map)
        }
    }
}

// ─── Bolt auth ───

/// Extract the "credentials" string from a HELLO metadata map.
fn extract_credentials(metadata: &HashMap<String, Value>) -> Option<&str> {
    metadata.get("credentials").and_then(|v| match v {
        Value::String(s) => Some(s.as_str()),
        _ => None,
    })
}

// ─── Bolt version negotiation ───

/// Negotiate Bolt protocol version with the client.
/// Each proposal is 4 bytes: [patch, range, minor, major].
/// `range` means the client accepts major.minor down to major.(minor - range).
/// Prefers the highest supported version.
///
/// Supports Bolt 4.4 and Bolt 5.0-5.7.
fn negotiate_bolt_version(versions: &[u8; 16]) -> Option<BoltVersion> {
    const MAX_BOLT_5_MINOR: u8 = 7;

    // Try to negotiate highest version first (5.7 > ... > 5.0 > 4.4)
    for i in 0..4 {
        let offset = i * 4;
        let major = versions[offset + 3];
        let minor = versions[offset + 2];
        let range = versions[offset + 1];

        // Check for Bolt 5.x (client accepts versions in range)
        if major == 5 {
            let min_minor = minor.saturating_sub(range);
            let max_minor = minor;

            // Find the highest version we support that the client accepts
            for supported_minor in (0..=MAX_BOLT_5_MINOR).rev() {
                if supported_minor >= min_minor && supported_minor <= max_minor {
                    return Some(BoltVersion::V5(supported_minor));
                }
            }
        }

        // Check for Bolt 4.4 (client accepts versions including 4.4)
        if major == 4 && minor >= 4 && minor.saturating_sub(range) <= 4 {
            return Some(BoltVersion::V4_4);
        }
    }

    None
}

#[cfg(test)]
mod tests {
    use super::*;

    // ─── bolt_value_to_json ───

    #[test]
    fn bolt_string_to_json() {
        let bolt = Value::from("hello");
        assert_eq!(bolt_value_to_json(&bolt), serde_json::json!("hello"));
    }

    #[test]
    fn bolt_integer_to_json() {
        let bolt = Value::from(42i64);
        assert_eq!(bolt_value_to_json(&bolt), serde_json::json!(42));
    }

    #[test]
    fn bolt_float_to_json() {
        let bolt = Value::from(3.14f64);
        assert_eq!(bolt_value_to_json(&bolt), serde_json::json!(3.14));
    }

    #[test]
    fn bolt_bool_to_json() {
        let bolt = Value::from(true);
        assert_eq!(bolt_value_to_json(&bolt), serde_json::json!(true));
    }

    #[test]
    fn bolt_null_to_json() {
        let bolt = Value::Null;
        assert_eq!(bolt_value_to_json(&bolt), serde_json::Value::Null);
    }

    #[test]
    fn bolt_list_to_json() {
        let bolt = Value::from(vec![Value::from(1i64), Value::from("two")]);
        assert_eq!(bolt_value_to_json(&bolt), serde_json::json!([1, "two"]));
    }

    #[test]
    fn bolt_map_to_json() {
        let mut map = HashMap::new();
        map.insert("name".to_string(), Value::from("Alice"));
        map.insert("age".to_string(), Value::from(30i64));
        let bolt = Value::from(map);
        let json = bolt_value_to_json(&bolt);
        assert_eq!(json["name"], "Alice");
        assert_eq!(json["age"], 30);
    }

    // ─── bolt_params_to_json ───

    #[test]
    fn empty_params_returns_none() {
        let params = HashMap::new();
        assert!(bolt_params_to_json(&params).is_none());
    }

    #[test]
    fn params_with_values() {
        let mut params = HashMap::new();
        params.insert("name".to_string(), Value::from("Bob"));
        let json = bolt_params_to_json(&params).unwrap();
        assert_eq!(json["name"], "Bob");
    }

    // ─── graph_value_to_bolt ───

    #[test]
    fn graph_null_to_bolt() {
        let gv = GraphValue::Null;
        assert_eq!(graph_value_to_bolt(&gv), Value::Null);
    }

    #[test]
    fn graph_bool_to_bolt() {
        let gv = GraphValue::Bool(true);
        match graph_value_to_bolt(&gv) {
            Value::Boolean(b) => assert!(b),
            other => panic!("Expected Boolean, got {other:?}"),
        }
    }

    #[test]
    fn graph_int_to_bolt() {
        let gv = GraphValue::Int(99);
        match graph_value_to_bolt(&gv) {
            Value::Integer(i) => assert_eq!(i, 99),
            other => panic!("Expected Integer, got {other:?}"),
        }
    }

    #[test]
    fn graph_float_to_bolt() {
        let gv = GraphValue::Float(2.718);
        match graph_value_to_bolt(&gv) {
            Value::Float(f) => assert!((f - 2.718).abs() < 1e-10),
            other => panic!("Expected Float, got {other:?}"),
        }
    }

    #[test]
    fn graph_string_to_bolt() {
        let gv = GraphValue::String("hello".into());
        match graph_value_to_bolt(&gv) {
            Value::String(s) => assert_eq!(s, "hello"),
            other => panic!("Expected String, got {other:?}"),
        }
    }

    #[test]
    fn graph_list_to_bolt() {
        let gv = GraphValue::List(vec![GraphValue::Int(1), GraphValue::Int(2)]);
        match graph_value_to_bolt(&gv) {
            Value::List(list) => {
                assert_eq!(list.len(), 2);
                match &list[0] {
                    Value::Integer(i) => assert_eq!(*i, 1),
                    other => panic!("Expected Integer, got {other:?}"),
                }
            }
            other => panic!("Expected List, got {other:?}"),
        }
    }

    #[test]
    fn graph_map_to_bolt() {
        let mut hm = std::collections::HashMap::new();
        hm.insert("key".to_string(), GraphValue::String("value".into()));
        let gv = GraphValue::Map(hm);
        match graph_value_to_bolt(&gv) {
            Value::Map(bmap) => {
                let val = bmap.get("key").unwrap();
                match val {
                    Value::String(s) => assert_eq!(s, "value"),
                    other => panic!("Expected String, got {other:?}"),
                }
            }
            other => panic!("Expected Map, got {other:?}"),
        }
    }

    // ─── json_to_bolt ───

    #[test]
    fn json_null_to_bolt() {
        assert_eq!(json_to_bolt(&serde_json::Value::Null), Value::Null);
    }

    #[test]
    fn json_string_to_bolt() {
        match json_to_bolt(&serde_json::json!("test")) {
            Value::String(s) => assert_eq!(s, "test"),
            other => panic!("Expected String, got {other:?}"),
        }
    }

    #[test]
    fn json_integer_to_bolt() {
        match json_to_bolt(&serde_json::json!(42)) {
            Value::Integer(i) => assert_eq!(i, 42),
            other => panic!("Expected Integer, got {other:?}"),
        }
    }

    #[test]
    fn json_float_to_bolt() {
        match json_to_bolt(&serde_json::json!(3.14)) {
            Value::Float(f) => assert!((f - 3.14).abs() < 1e-10),
            other => panic!("Expected Float, got {other:?}"),
        }
    }

    #[test]
    fn json_array_to_bolt() {
        match json_to_bolt(&serde_json::json!([1, "two"])) {
            Value::List(list) => assert_eq!(list.len(), 2),
            other => panic!("Expected List, got {other:?}"),
        }
    }

    #[test]
    fn json_object_to_bolt() {
        match json_to_bolt(&serde_json::json!({"a": 1})) {
            Value::Map(map) => {
                assert_eq!(map.len(), 1);
                let val = map.get("a").unwrap();
                match val {
                    Value::Integer(i) => assert_eq!(*i, 1),
                    other => panic!("Expected Integer, got {other:?}"),
                }
            }
            other => panic!("Expected Map, got {other:?}"),
        }
    }

    // ─── Roundtrip: JSON → Bolt → JSON ───

    #[test]
    fn json_bolt_roundtrip_nested() {
        let original = serde_json::json!({
            "name": "Alice",
            "age": 30,
            "active": true,
            "scores": [90, 85, 92],
            "address": {
                "city": "Wonderland",
                "zip": null
            }
        });
        let bolt = json_to_bolt(&original);
        let back = bolt_value_to_json(&bolt);
        assert_eq!(back["name"], "Alice");
        assert_eq!(back["age"], 30);
        assert_eq!(back["active"], true);
        assert_eq!(back["scores"], serde_json::json!([90, 85, 92]));
        assert_eq!(back["address"]["city"], "Wonderland");
        assert!(back["address"]["zip"].is_null());
    }

    // ─── Roundtrip: GraphValue → Bolt → JSON ───

    #[test]
    fn graph_value_bolt_roundtrip() {
        let gv = GraphValue::List(vec![
            GraphValue::String("hello".into()),
            GraphValue::Int(42),
            GraphValue::Float(1.5),
            GraphValue::Bool(false),
            GraphValue::Null,
        ]);
        let bolt = graph_value_to_bolt(&gv);
        let json = bolt_value_to_json(&bolt);
        assert_eq!(json, serde_json::json!(["hello", 42, 1.5, false, null]));
    }

    // ─── extract_credentials ───

    #[test]
    fn extract_credentials_basic_scheme() {
        let mut metadata = HashMap::new();
        metadata.insert("scheme".to_string(), Value::from("basic"));
        metadata.insert("principal".to_string(), Value::from("neo4j"));
        metadata.insert("credentials".to_string(), Value::from("my-secret-token"));
        assert_eq!(extract_credentials(&metadata), Some("my-secret-token"));
    }

    #[test]
    fn extract_credentials_missing() {
        let metadata = HashMap::new();
        assert_eq!(extract_credentials(&metadata), None);
    }

    #[test]
    fn extract_credentials_non_string() {
        let mut metadata = HashMap::new();
        metadata.insert("credentials".to_string(), Value::from(42i64));
        assert_eq!(extract_credentials(&metadata), None);
    }

    #[test]
    fn extract_credentials_empty_string() {
        let mut metadata = HashMap::new();
        metadata.insert("credentials".to_string(), Value::from(""));
        assert_eq!(extract_credentials(&metadata), Some(""));
    }

    // ─── negotiate_bolt_version ───

    #[test]
    fn version_exact_4_4() {
        // Client proposes exactly 4.4 in first slot.
        let mut v = [0u8; 16];
        v[0..4].copy_from_slice(&[0x00, 0x00, 0x04, 0x04]); // 4.4
        assert_eq!(negotiate_bolt_version(&v), Some(BoltVersion::V4_4));
    }

    #[test]
    fn version_range_includes_4_4() {
        // Client proposes 4.4 with range 2 → accepts 4.4, 4.3, 4.2.
        let mut v = [0u8; 16];
        v[0..4].copy_from_slice(&[0x00, 0x02, 0x04, 0x04]);
        assert_eq!(negotiate_bolt_version(&v), Some(BoltVersion::V4_4));
    }

    #[test]
    fn version_4_3_no_range() {
        // Client proposes only 4.3 — no range includes 4.4.
        let mut v = [0u8; 16];
        v[0..4].copy_from_slice(&[0x00, 0x00, 0x03, 0x04]); // 4.3
        assert_eq!(negotiate_bolt_version(&v), None);
    }

    #[test]
    fn version_5_0() {
        // Client proposes 5.0.
        let mut v = [0u8; 16];
        v[0..4].copy_from_slice(&[0x00, 0x00, 0x00, 0x05]); // 5.0
        assert_eq!(negotiate_bolt_version(&v), Some(BoltVersion::V5(0)));
    }

    #[test]
    fn version_5_1() {
        // Client proposes 5.1.
        let mut v = [0u8; 16];
        v[0..4].copy_from_slice(&[0x00, 0x00, 0x01, 0x05]); // 5.1
        assert_eq!(negotiate_bolt_version(&v), Some(BoltVersion::V5(1)));
    }

    #[test]
    fn version_prefers_highest() {
        // Client proposes 5.1, 5.0, 4.4 — should prefer 5.1.
        let mut v = [0u8; 16];
        v[0..4].copy_from_slice(&[0x00, 0x00, 0x01, 0x05]); // 5.1
        v[4..8].copy_from_slice(&[0x00, 0x00, 0x00, 0x05]); // 5.0
        v[8..12].copy_from_slice(&[0x00, 0x00, 0x04, 0x04]); // 4.4
        assert_eq!(negotiate_bolt_version(&v), Some(BoltVersion::V5(1)));
    }

    #[test]
    fn version_in_second_slot() {
        // First slot is unsupported version 6.0, second slot is 4.4.
        let mut v = [0u8; 16];
        v[0..4].copy_from_slice(&[0x00, 0x00, 0x00, 0x06]); // 6.0 (unsupported)
        v[4..8].copy_from_slice(&[0x00, 0x00, 0x04, 0x04]); // 4.4
        assert_eq!(negotiate_bolt_version(&v), Some(BoltVersion::V4_4));
    }

    #[test]
    fn version_all_zeros() {
        let v = [0u8; 16];
        assert_eq!(negotiate_bolt_version(&v), None);
    }

    #[test]
    fn version_high_minor_range_includes_4_4() {
        // Client proposes 4.6 with range 3 → accepts 4.6, 4.5, 4.4, 4.3.
        let mut v = [0u8; 16];
        v[0..4].copy_from_slice(&[0x00, 0x03, 0x06, 0x04]);
        assert_eq!(negotiate_bolt_version(&v), Some(BoltVersion::V4_4));
    }

    #[test]
    fn version_5_1_with_range() {
        // Client proposes 5.1 with range 1 → accepts 5.1 and 5.0.
        let mut v = [0u8; 16];
        v[0..4].copy_from_slice(&[0x00, 0x01, 0x01, 0x05]);
        assert_eq!(negotiate_bolt_version(&v), Some(BoltVersion::V5(1)));
    }

    #[test]
    fn version_malformed_range_greater_than_minor() {
        // Malformed: range > minor (5.1 with range 2 claims to accept down to 5.-1)
        // With saturating_sub, treats as accepting down to 5.0, so matches 5.1
        // This lenient behavior is acceptable for malformed input
        let mut v = [0u8; 16];
        v[0..4].copy_from_slice(&[0x00, 0x02, 0x01, 0x05]); // 5.1 with range 2
        assert_eq!(negotiate_bolt_version(&v), Some(BoltVersion::V5(1)));
    }

    #[test]
    fn version_5_2_down_to_5_0_matches_5_2() {
        // Client proposes 5.2 with range 2 → accepts 5.2, 5.1, 5.0
        // Should match 5.2 (highest we support in that range, since we now support up to 5.7)
        let mut v = [0u8; 16];
        v[0..4].copy_from_slice(&[0x00, 0x02, 0x02, 0x05]); // 5.2 with range 2
        assert_eq!(negotiate_bolt_version(&v), Some(BoltVersion::V5(2)));
    }

    #[test]
    fn version_5_2_no_range_matches_5_2() {
        // Client proposes only 5.2 (no range) → we now support 5.2
        let mut v = [0u8; 16];
        v[0..4].copy_from_slice(&[0x00, 0x00, 0x02, 0x05]); // 5.2 no range
        assert_eq!(negotiate_bolt_version(&v), Some(BoltVersion::V5(2)));
    }

    #[test]
    fn version_5_7() {
        // Client proposes 5.7 (latest supported).
        let mut v = [0u8; 16];
        v[0..4].copy_from_slice(&[0x00, 0x00, 0x07, 0x05]); // 5.7
        assert_eq!(negotiate_bolt_version(&v), Some(BoltVersion::V5(7)));
    }

    #[test]
    fn version_5_8_not_supported() {
        // Client proposes 5.8 → we only support up to 5.7
        let mut v = [0u8; 16];
        v[0..4].copy_from_slice(&[0x00, 0x00, 0x08, 0x05]); // 5.8
        assert_eq!(negotiate_bolt_version(&v), None);
    }

    #[test]
    fn version_5_10_with_range_matches_5_7() {
        // Client proposes 5.10 with range 5 → accepts 5.10 down to 5.5
        // Should match 5.7 (highest we support in that range)
        let mut v = [0u8; 16];
        v[0..4].copy_from_slice(&[0x00, 0x05, 0x0A, 0x05]); // 5.10 with range 5
        assert_eq!(negotiate_bolt_version(&v), Some(BoltVersion::V5(7)));
    }
}
