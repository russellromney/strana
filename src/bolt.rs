use std::sync::{Arc, RwLock};

use bytes::Bytes;
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufStream};
use tokio::net::{TcpListener, TcpStream};
use tokio_util::compat::TokioAsyncReadCompatExt;
use tracing::{debug, error, info, warn};

use crate::auth::TokenStore;
use crate::engine::Engine;
use crate::journal::{self, JournalCommand, JournalSender, PendingEntry};
use crate::metrics::Metrics;
use crate::query;
use crate::values::{self, json_to_param_values, GraphValue};

use bolt_proto::message::Record;
use bolt_proto::{Message, Value};
use std::collections::HashMap;

// Shared Bolt protocol types and helpers from graphd-engine.
use graphd_engine::bolt::{
    bolt_params_to_json, extract_credentials, success_message, version_bytes, BOLT_MAGIC,
};
pub use graphd_engine::bolt::{
    add_connection_hints, failure_message_versioned, negotiate_bolt_version,
    negotiate_bolt_version_with_limit, BoltVersion,
};

/// Shared state for the Bolt listener.
#[derive(Clone)]
pub struct BoltState {
    pub engine: Arc<Engine>,
    pub tokens: Arc<TokenStore>,
    pub conn_semaphore: Arc<tokio::sync::Semaphore>,
    pub replica: bool,
    pub metrics: Arc<Metrics>,
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
                            state.metrics.bolt_connections_active.inc();
                            let result = handle_connection(socket, state.clone()).await;
                            state.metrics.bolt_connections_active.dec();
                            if let Err(e) = result {
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
    if magic != BOLT_MAGIC {
        return Err("Invalid Bolt magic bytes".into());
    }

    // Read 4 proposed versions (16 bytes) and negotiate.
    let mut versions = [0u8; 16];
    stream
        .read_exact(&mut versions)
        .await
        .map_err(|e| format!("read versions: {e}"))?;

    // Check for artificial version limit (for testing)
    let max_version = get_max_bolt_version_from_env();

    let bolt_version = match negotiate_bolt_version_with_limit(&versions, max_version) {
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
    stream
        .write_all(&version_bytes(bolt_version))
        .await
        .map_err(|e| format!("write version: {e}"))?;
    stream.flush().await.map_err(|e| format!("flush: {e}"))?;

    info!("Negotiated Bolt version: {}", bolt_version);

    // ── Spawn blocking session worker ──
    // The Bolt session owns a database connection and runs queries on it.
    let engine = state.engine.clone();
    let tokens = state.tokens.clone();
    let replica = state.replica;

    // Channel for sending requests from the async reader to the blocking worker.
    let (req_tx, req_rx) = tokio::sync::mpsc::unbounded_channel::<BoltOp>();
    let (resp_tx, mut resp_rx) = tokio::sync::mpsc::unbounded_channel::<Vec<Bytes>>();

    // Blocking session worker.
    let worker_handle = tokio::task::spawn_blocking(move || {
        bolt_session_worker(bolt_version, engine, tokens, replica, req_rx, resp_tx);
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
    bolt_version: BoltVersion,
    engine: Arc<Engine>,
    tokens: Arc<TokenStore>,
    replica: bool,
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
            bolt_version,
            &req,
            &conn,
            &tokens,
            &journal,
            &snapshot_lock,
            replica,
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
/// - Messages: HELLO, RUN, PULL, DISCARD, BEGIN, COMMIT, ROLLBACK, RESET, GOODBYE
///
/// **Bolt 5.0:**
/// - All Bolt 4.4 messages
/// - Accepts `imp_user` metadata in ROUTE, RUN, BEGIN
///
/// **Bolt 5.1:**
/// - LOGON/LOGOFF messages for authentication (replaces HELLO auth flow)
///
/// **Bolt 5.2:**
/// - Accepts `notifications_minimum_severity` and `notifications_disabled_categories` in HELLO, RUN, BEGIN
///
/// **Bolt 5.3:**
/// - Accepts `bolt_agent` metadata in HELLO/LOGON
///
/// **Bolt 5.4:**
/// - TELEMETRY message for client metrics
/// - Returns `telemetry.enabled` connection hint in HELLO/LOGON SUCCESS
///
/// **Bolt 5.6:**
/// - `notifications_disabled_categories` renamed to `notifications_disabled_classifications`
/// - Returns `statuses` instead of `notifications` in query responses (GQL standard)
///
/// **Bolt 5.7:**
/// - GQL-compliant error reporting with `gql_status`, `neo4j_code`, `diagnostic_record`
/// - Optional manifest v1 handshake (not yet implemented)
///
/// The implementation accepts all version-specific metadata fields and returns
/// appropriate connection hints based on the negotiated protocol version.
#[allow(deprecated)]
fn handle_bolt_message<'db>(
    bolt_version: BoltVersion,
    req: &Message,
    conn: &lbug::Connection<'db>,
    tokens: &Arc<TokenStore>,
    journal: &Option<JournalSender>,
    snapshot_lock: &Arc<RwLock<()>>,
    replica: bool,
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
                            return failure_message_versioned(
                                bolt_version,
                                "Neo.ClientError.Security.Unauthorized",
                                "Invalid credentials",
                            );
                        }
                    },
                    None => {
                        return failure_message_versioned(
                            bolt_version,
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

            // Add connection hints based on Bolt version
            add_connection_hints(bolt_version, &mut metadata);

            debug!("Sending HELLO SUCCESS with metadata: {:?}", metadata);
            let response = success_message(metadata);
            debug!("HELLO SUCCESS response has {} chunks", response.len());
            response
        }

        // ── RUN ──
        Message::Run(run) => {
            if !*authenticated {
                return failure_message_versioned(
                    bolt_version,
                    "Neo.ClientError.Security.AuthenticationRateLimit",
                    "Not authenticated",
                );
            }

            let query_str = run.query();

            // Reject mutations in replica mode.
            if replica && journal::is_mutation(query_str) {
                return failure_message_versioned(
                    bolt_version,
                    "Neo.ClientError.Database.ReadOnlyMode",
                    "Cannot execute write operations on a read-only replica",
                );
            }

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
                                params: json_to_param_values(&raw.merged_params),
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
                    failure_message_versioned(bolt_version,
                        "Neo.DatabaseError.General.UnknownError",
                        &format!("Query error: {e}"),
                    )
                }
            }
        }

        // ── RUN_WITH_METADATA ──
        Message::RunWithMetadata(run) => {
            if !*authenticated {
                return failure_message_versioned(bolt_version,
                    "Neo.ClientError.Security.AuthenticationRateLimit",
                    "Not authenticated",
                );
            }

            let query_str = run.statement();

            // Reject mutations in replica mode.
            if replica && journal::is_mutation(query_str) {
                return failure_message_versioned(bolt_version,
                    "Neo.ClientError.Database.ReadOnlyMode",
                    "Cannot execute write operations on a read-only replica",
                );
            }

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
                                params: json_to_param_values(&raw.merged_params),
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
                    failure_message_versioned(bolt_version,
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
                return failure_message_versioned(
                    bolt_version,
                    "Neo.ClientError.Security.AuthenticationRateLimit",
                    "Not authenticated",
                );
            }
            if *in_transaction {
                return failure_message_versioned(
                    bolt_version,
                    "Neo.ClientError.Transaction.TransactionNotFound",
                    "Transaction already active",
                );
            }

            // Reject transactions in replica mode (can't predict if they'll contain mutations).
            if replica {
                return failure_message_versioned(
                    bolt_version,
                    "Neo.ClientError.Database.ReadOnlyMode",
                    "Cannot begin transactions on a read-only replica",
                );
            }

            match conn.query("BEGIN TRANSACTION") {
                Ok(_) => {
                    *in_transaction = true;
                    success_message(HashMap::new())
                }
                Err(e) => failure_message_versioned(
                    bolt_version,
                    "Neo.DatabaseError.General.UnknownError",
                    &format!("BEGIN failed: {e}"),
                ),
            }
        }

        // ── COMMIT ──
        Message::Commit => {
            if !*in_transaction {
                return failure_message_versioned(bolt_version,
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
                Err(e) => failure_message_versioned(bolt_version,
                    "Neo.DatabaseError.General.UnknownError",
                    &format!("COMMIT failed: {e}"),
                ),
            }
        }

        // ── ROLLBACK ──
        Message::Rollback => {
            if !*in_transaction {
                return failure_message_versioned(bolt_version,
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
                Err(e) => failure_message_versioned(bolt_version,
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
                            return failure_message_versioned(
                                bolt_version,
                                "Neo.ClientError.Security.Unauthorized",
                                "Invalid credentials",
                            );
                        }
                    },
                    None => {
                        return failure_message_versioned(
                            bolt_version,
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

            // Add connection hints based on Bolt version
            add_connection_hints(bolt_version, &mut metadata);

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
            failure_message_versioned(bolt_version,
                "Neo.ClientError.Request.Invalid",
                "Unsupported message type",
            )
        }

    }
}

// ─── Value conversion (local: uses crate::values::GraphValue) ───

/// Convert a GraphValue to bolt-rs Value for RECORD responses.
///
/// Kept local because it uses strana's `crate::values::GraphValue` type.
/// The shared `graphd_engine::bolt::graph_value_to_bolt` uses graphd-engine's
/// own GraphValue type (structurally identical but a different Rust type).
fn graph_value_to_bolt(gv: &GraphValue) -> Value {
    use graphd_engine::bolt::json_to_bolt;

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

// ─── Env-based version limit (strana-specific testing feature) ───

/// Get maximum Bolt version from environment (for version compatibility testing).
///
/// Set BOLT_MAX_VERSION to artificially limit version negotiation (e.g., "4.4", "5.0", "5.7").
///
/// This allows testing the full protocol implementation at each Bolt version level,
/// not just the handshake. When set, the server will negotiate down to the specified
/// version and use version-appropriate features:
///
/// - Bolt 4.4: Legacy error format, no connection hints
/// - Bolt 5.0-5.3: Legacy error format, no connection hints
/// - Bolt 5.4-5.6: Legacy error format, telemetry.enabled hint
/// - Bolt 5.7: GQL-compliant error format, telemetry.enabled hint
///
/// Usage:
/// ```bash
/// BOLT_MAX_VERSION=5.0 ./graphd
/// BOLT_MAX_VERSION=4.4 make e2e-py
/// make version-compat  # Test all versions automatically
/// ```
fn get_max_bolt_version_from_env() -> Option<BoltVersion> {
    std::env::var("BOLT_MAX_VERSION").ok().and_then(|s| {
        let parts: Vec<&str> = s.split('.').collect();
        if parts.len() != 2 {
            warn!("Invalid BOLT_MAX_VERSION format: {}", s);
            return None;
        }

        let major = parts[0].parse::<u8>().ok()?;
        let minor = parts[1].parse::<u8>().ok()?;

        match (major, minor) {
            (4, 4) => Some(BoltVersion::V4_4),
            (5, m) if m <= 7 => Some(BoltVersion::V5(m)),
            _ => {
                warn!("Unsupported BOLT_MAX_VERSION: {}.{}", major, minor);
                None
            }
        }
    })
}
