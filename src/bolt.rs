use std::sync::{Arc, RwLock};

use bytes::{Bytes, BytesMut};
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufStream};
use tokio::net::{TcpListener, TcpStream};
use tracing::{debug, error, info, warn};

use crate::auth::TokenStore;
use crate::engine::Engine;
use crate::journal::{self, JournalCommand, JournalSender, PendingEntry};
use crate::query;
use crate::values::{self, GraphValue};

use bolt4rs::bolt::response::success;
use bolt4rs::bolt::summary::{Success, Summary};
use bolt4rs::messages::{BoltRequest, BoltResponse, Record};
use bolt4rs::{BoltBoolean, BoltFloat, BoltInteger, BoltList, BoltMap, BoltNull, BoltString, BoltType};

const MAX_CHUNK_SIZE: usize = 65_535;

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

    if !negotiate_bolt_version(&versions) {
        // No supported version found — send zero version and close.
        stream
            .write_all(&[0x00, 0x00, 0x00, 0x00])
            .await
            .map_err(|e| format!("write version: {e}"))?;
        stream.flush().await.map_err(|e| format!("flush: {e}"))?;
        return Err("No supported Bolt version in client proposals".into());
    }

    stream
        .write_all(&[0x00, 0x00, 0x04, 0x04])
        .await
        .map_err(|e| format!("write version: {e}"))?;
    stream.flush().await.map_err(|e| format!("flush: {e}"))?;

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
    loop {
        match read_chunked_message(&mut stream).await {
            Ok(Some(msg_bytes)) => {
                let req = match BoltRequest::parse(bolt4rs::Version::V4_4, msg_bytes) {
                    Ok(r) => r,
                    Err(e) => {
                        warn!("Invalid Bolt message: {e}");
                        break;
                    }
                };

                if req_tx.send(BoltOp::Request(req)).is_err() {
                    break; // worker exited
                }

                // Wait for response(s) from worker.
                match resp_rx.recv().await {
                    Some(responses) => {
                        for response_bytes in responses {
                            if let Err(e) = send_chunked_message(&mut stream, response_bytes).await
                            {
                                debug!("Bolt send error: {e}");
                                return Ok(());
                            }
                        }
                    }
                    None => break, // worker exited
                }
            }
            Ok(None) => break,
            Err(e) => {
                debug!("Bolt read error: {e}");
                break;
            }
        }
    }

    drop(req_tx);
    let _ = worker_handle.await;
    info!("Bolt client disconnected");
    Ok(())
}

/// Operations sent to the blocking session worker.
enum BoltOp {
    Request(BoltRequest),
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
            BoltRequest::Run(run) if !in_transaction && write_guard.is_none() => {
                if journal::is_mutation(&run.query.to_string()) {
                    write_guard = Some(engine.acquire_writer());
                }
            }
            BoltRequest::Begin(_) if write_guard.is_none() => {
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
#[allow(deprecated)]
fn handle_bolt_message<'db>(
    req: &BoltRequest,
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
        BoltRequest::Hello(hello) => {
            if tokens.is_empty() {
                // Open access — no tokens configured.
                *authenticated = true;
                info!("Bolt: client connected (open access)");
            } else {
                // Extract credentials from HELLO extra map.
                // Neo4j drivers send: scheme="basic", principal="neo4j", credentials="<token>"
                match extract_credentials(hello.extra()) {
                    Some(token) => match tokens.validate(token) {
                        Ok(label) => {
                            *authenticated = true;
                            info!("Bolt: authenticated (label={label})");
                        }
                        Err(_) => {
                            return vec![failure_bytes(
                                "Neo.ClientError.Security.Unauthorized",
                                "Invalid credentials",
                            )];
                        }
                    },
                    None => {
                        return vec![failure_bytes(
                            "Neo.ClientError.Security.Unauthorized",
                            "Authentication required: provide credentials in HELLO",
                        )];
                    }
                }
            }

            let metadata = success::MetaBuilder::new()
                .server(&format!("Neo4j/5.x.0-graphd-{}", env!("CARGO_PKG_VERSION")))
                .connection_id("bolt-1")
                .build();
            let summary = Summary::Success(Success { metadata });
            vec![Bytes::from(summary.to_bytes().unwrap_or_default())]
        }

        // ── RUN ──
        BoltRequest::Run(run) => {
            if !*authenticated {
                return vec![failure_bytes(
                    "Neo.ClientError.Security.AuthenticationRateLimit",
                    "Not authenticated",
                )];
            }

            let query_str = run.query.to_string();

            // Convert Bolt parameters to JSON for our query engine.
            let json_params = bolt_params_to_json(&run.parameters);

            // Rewrite + execute via shared query engine.
            let _lock = if !*in_transaction {
                Some(snapshot_lock.read().unwrap_or_else(|e| e.into_inner()))
            } else {
                None
            };

            match query::run_query_raw(conn, &query_str, json_params.as_ref()) {
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

                    let metadata = success::MetaBuilder::new()
                        .fields(cols.into_iter())
                        .build();
                    let summary = Summary::Success(Success { metadata });
                    vec![Bytes::from(summary.to_bytes().unwrap_or_default())]
                }
                Err(e) => {
                    vec![failure_bytes(
                        "Neo.DatabaseError.General.UnknownError",
                        &format!("Query error: {e}"),
                    )]
                }
            }
        }

        // ── PULL ──
        BoltRequest::Pull(pull) => {
            if current_result.is_none() || *result_consumed {
                let metadata = success::MetaBuilder::new().build();
                let summary = Summary::Success(Success { metadata });
                return vec![Bytes::from(summary.to_bytes().unwrap_or_default())];
            }

            let max_records = pull.extra.get::<i64>("n").unwrap_or(1000) as usize;
            let result = current_result.as_mut().unwrap();

            let mut responses = Vec::new();
            let mut records_sent = 0;

            while records_sent < max_records {
                match result.next() {
                    Some(row) => {
                        let values: Vec<BoltType> = row
                            .iter()
                            .map(|v| graph_value_to_bolt(&values::from_lbug_value(v)))
                            .collect();
                        let record = Record {
                            data: BoltList { value: values },
                        };
                        let response = BoltResponse::Record(record);
                        if let Ok(bytes) = response.to_bytes() {
                            responses.push(bytes);
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
            let metadata = success::MetaBuilder::new()
                .done(!has_more)
                .has_more(has_more)
                .build();
            let summary = Summary::Success(Success { metadata });
            responses.push(Bytes::from(summary.to_bytes().unwrap_or_default()));
            responses
        }

        // ── DISCARD ──
        BoltRequest::Discard(_) => {
            *current_result = None;
            *result_consumed = true;
            let metadata = success::MetaBuilder::new().build();
            let summary = Summary::Success(Success { metadata });
            vec![Bytes::from(summary.to_bytes().unwrap_or_default())]
        }

        // ── BEGIN ──
        BoltRequest::Begin(_) => {
            if !*authenticated {
                return vec![failure_bytes(
                    "Neo.ClientError.Security.AuthenticationRateLimit",
                    "Not authenticated",
                )];
            }
            if *in_transaction {
                return vec![failure_bytes(
                    "Neo.ClientError.Transaction.TransactionNotFound",
                    "Transaction already active",
                )];
            }

            match conn.query("BEGIN TRANSACTION") {
                Ok(_) => {
                    *in_transaction = true;
                    let metadata = success::MetaBuilder::new().build();
                    let summary = Summary::Success(Success { metadata });
                    vec![Bytes::from(summary.to_bytes().unwrap_or_default())]
                }
                Err(e) => vec![failure_bytes(
                    "Neo.DatabaseError.General.UnknownError",
                    &format!("BEGIN failed: {e}"),
                )],
            }
        }

        // ── COMMIT ──
        BoltRequest::Commit(_) => {
            if !*in_transaction {
                return vec![failure_bytes(
                    "Neo.ClientError.Transaction.TransactionNotFound",
                    "No active transaction",
                )];
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
                    let metadata = success::MetaBuilder::new().build();
                    let summary = Summary::Success(Success { metadata });
                    vec![Bytes::from(summary.to_bytes().unwrap_or_default())]
                }
                Err(e) => vec![failure_bytes(
                    "Neo.DatabaseError.General.UnknownError",
                    &format!("COMMIT failed: {e}"),
                )],
            }
        }

        // ── ROLLBACK ──
        BoltRequest::Rollback(_) => {
            if !*in_transaction {
                return vec![failure_bytes(
                    "Neo.ClientError.Transaction.TransactionNotFound",
                    "No active transaction",
                )];
            }

            match conn.query("ROLLBACK") {
                Ok(_) => {
                    *in_transaction = false;
                    tx_journal_buf.clear();
                    let metadata = success::MetaBuilder::new().build();
                    let summary = Summary::Success(Success { metadata });
                    vec![Bytes::from(summary.to_bytes().unwrap_or_default())]
                }
                Err(e) => vec![failure_bytes(
                    "Neo.DatabaseError.General.UnknownError",
                    &format!("ROLLBACK failed: {e}"),
                )],
            }
        }

        // ── RESET ──
        BoltRequest::Reset(_) => {
            if *in_transaction {
                let _ = conn.query("ROLLBACK");
                *in_transaction = false;
                tx_journal_buf.clear();
            }
            *current_result = None;
            *result_consumed = true;
            let metadata = success::MetaBuilder::new().build();
            let summary = Summary::Success(Success { metadata });
            vec![Bytes::from(summary.to_bytes().unwrap_or_default())]
        }

    }
}

// ─── Value conversion ───

/// Convert Bolt parameters (BoltMap) to JSON for the shared query engine.
fn bolt_params_to_json(params: &BoltMap) -> Option<serde_json::Value> {
    if params.value.is_empty() {
        return None;
    }
    let mut map = serde_json::Map::new();
    for (k, v) in &params.value {
        map.insert(k.value.clone(), bolt_type_to_json(v));
    }
    Some(serde_json::Value::Object(map))
}

/// Convert a BoltType to JSON.
fn bolt_type_to_json(bolt: &BoltType) -> serde_json::Value {
    match bolt {
        BoltType::String(s) => serde_json::Value::String(s.value.clone()),
        BoltType::Integer(i) => serde_json::json!(i.value),
        BoltType::Float(f) => serde_json::json!(f.value),
        BoltType::Boolean(b) => serde_json::Value::Bool(b.value),
        BoltType::Null(_) => serde_json::Value::Null,
        BoltType::List(list) => {
            serde_json::Value::Array(list.value.iter().map(bolt_type_to_json).collect())
        }
        BoltType::Map(map) => {
            let mut obj = serde_json::Map::new();
            for (k, v) in &map.value {
                obj.insert(k.value.clone(), bolt_type_to_json(v));
            }
            serde_json::Value::Object(obj)
        }
        _ => serde_json::Value::Null, // DateTime etc. — not common as params
    }
}

/// Convert a GraphValue to BoltType for RECORD responses.
fn graph_value_to_bolt(gv: &GraphValue) -> BoltType {
    match gv {
        GraphValue::Null => BoltType::Null(BoltNull {}),
        GraphValue::Bool(b) => BoltType::Boolean(BoltBoolean { value: *b }),
        GraphValue::Int(i) => BoltType::Integer(BoltInteger { value: *i }),
        GraphValue::Float(f) => BoltType::Float(BoltFloat { value: *f }),
        GraphValue::String(s) => BoltType::String(BoltString {
            value: s.clone(),
        }),
        GraphValue::List(items) => BoltType::List(BoltList {
            value: items.iter().map(graph_value_to_bolt).collect(),
        }),
        GraphValue::Map(entries) => {
            let mut map = BoltMap::new();
            for (k, v) in entries {
                map.put(BoltString { value: k.clone() }, graph_value_to_bolt(v));
            }
            BoltType::Map(map)
        }
        GraphValue::Tagged(_tagged) => {
            // Serialize tagged values (Node, Rel, Path) as maps with $type.
            let json = serde_json::to_value(gv).unwrap_or(serde_json::Value::Null);
            json_to_bolt(&json)
        }
    }
}

/// Convert serde_json::Value to BoltType (for tagged value serialization).
fn json_to_bolt(v: &serde_json::Value) -> BoltType {
    match v {
        serde_json::Value::Null => BoltType::Null(BoltNull {}),
        serde_json::Value::Bool(b) => BoltType::Boolean(BoltBoolean { value: *b }),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                BoltType::Integer(BoltInteger { value: i })
            } else if let Some(f) = n.as_f64() {
                BoltType::Float(BoltFloat { value: f })
            } else {
                BoltType::Null(BoltNull {})
            }
        }
        serde_json::Value::String(s) => BoltType::String(BoltString {
            value: s.clone(),
        }),
        serde_json::Value::Array(arr) => BoltType::List(BoltList {
            value: arr.iter().map(json_to_bolt).collect(),
        }),
        serde_json::Value::Object(obj) => {
            let mut map = BoltMap::new();
            for (k, v) in obj {
                map.put(BoltString { value: k.clone() }, json_to_bolt(v));
            }
            BoltType::Map(map)
        }
    }
}

// ─── Bolt auth ───

/// Extract the "credentials" string from a HELLO extra map.
fn extract_credentials(extra: &BoltMap) -> Option<&str> {
    extra
        .value
        .iter()
        .find(|(k, _)| k.value == "credentials")
        .and_then(|(_, v)| match v {
            BoltType::String(s) => Some(s.value.as_str()),
            _ => None,
        })
}

// ─── Bolt version negotiation ───

/// Check if any of the 4 client-proposed versions includes Bolt 4.4.
/// Each proposal is 4 bytes: [patch, range, minor, major].
/// `range` means the client accepts major.minor down to major.(minor - range).
fn negotiate_bolt_version(versions: &[u8; 16]) -> bool {
    for i in 0..4 {
        let offset = i * 4;
        let major = versions[offset + 3];
        let minor = versions[offset + 2];
        let range = versions[offset + 1];
        if major == 4 && minor >= 4 && (minor - range) <= 4 {
            return true;
        }
    }
    false
}

// ─── Bolt framing ───

/// Create a FAILURE response as bytes.
fn failure_bytes(code: &str, message: &str) -> Bytes {
    let failure =
        bolt4rs::bolt::summary::Failure::new(code.to_string(), message.to_string());
    let summary: Summary<success::Meta> = Summary::Failure(failure);
    Bytes::from(summary.to_bytes().unwrap_or_default())
}

/// Read a chunked Bolt message from the stream.
async fn read_chunked_message(stream: &mut BufStream<TcpStream>) -> Result<Option<Bytes>, String> {
    let mut message = BytesMut::new();

    loop {
        let mut size_bytes = [0u8; 2];
        match stream.read_exact(&mut size_bytes).await {
            Ok(_) => {
                let chunk_size = u16::from_be_bytes(size_bytes) as usize;
                if chunk_size == 0 {
                    return Ok(if message.is_empty() {
                        None
                    } else {
                        Some(message.freeze())
                    });
                }
                let mut chunk = vec![0u8; chunk_size];
                stream
                    .read_exact(&mut chunk)
                    .await
                    .map_err(|e| format!("read chunk: {e}"))?;
                message.extend_from_slice(&chunk);
            }
            Err(_) => {
                return Ok(None);
            }
        }
    }
}

/// Send a chunked Bolt message over the stream.
async fn send_chunked_message(
    stream: &mut BufStream<TcpStream>,
    message: Bytes,
) -> Result<(), String> {
    for chunk in message.chunks(MAX_CHUNK_SIZE) {
        let chunk_len = chunk.len() as u16;
        stream
            .write_all(&chunk_len.to_be_bytes())
            .await
            .map_err(|e| format!("write chunk len: {e}"))?;
        stream
            .write_all(chunk)
            .await
            .map_err(|e| format!("write chunk: {e}"))?;
    }
    stream
        .write_all(&[0, 0])
        .await
        .map_err(|e| format!("write end marker: {e}"))?;
    stream.flush().await.map_err(|e| format!("flush: {e}"))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    // ─── bolt_type_to_json ───

    #[test]
    fn bolt_string_to_json() {
        let bolt = BoltType::String(BoltString { value: "hello".into() });
        assert_eq!(bolt_type_to_json(&bolt), serde_json::json!("hello"));
    }

    #[test]
    fn bolt_integer_to_json() {
        let bolt = BoltType::Integer(BoltInteger { value: 42 });
        assert_eq!(bolt_type_to_json(&bolt), serde_json::json!(42));
    }

    #[test]
    fn bolt_float_to_json() {
        let bolt = BoltType::Float(BoltFloat { value: 3.14 });
        assert_eq!(bolt_type_to_json(&bolt), serde_json::json!(3.14));
    }

    #[test]
    fn bolt_bool_to_json() {
        let bolt = BoltType::Boolean(BoltBoolean { value: true });
        assert_eq!(bolt_type_to_json(&bolt), serde_json::json!(true));
    }

    #[test]
    fn bolt_null_to_json() {
        let bolt = BoltType::Null(BoltNull {});
        assert_eq!(bolt_type_to_json(&bolt), serde_json::Value::Null);
    }

    #[test]
    fn bolt_list_to_json() {
        let bolt = BoltType::List(BoltList {
            value: vec![
                BoltType::Integer(BoltInteger { value: 1 }),
                BoltType::String(BoltString { value: "two".into() }),
            ],
        });
        assert_eq!(bolt_type_to_json(&bolt), serde_json::json!([1, "two"]));
    }

    #[test]
    fn bolt_map_to_json() {
        let mut map = BoltMap::new();
        map.put(
            BoltString { value: "name".into() },
            BoltType::String(BoltString { value: "Alice".into() }),
        );
        map.put(
            BoltString { value: "age".into() },
            BoltType::Integer(BoltInteger { value: 30 }),
        );
        let bolt = BoltType::Map(map);
        let json = bolt_type_to_json(&bolt);
        assert_eq!(json["name"], "Alice");
        assert_eq!(json["age"], 30);
    }

    // ─── bolt_params_to_json ───

    #[test]
    fn empty_params_returns_none() {
        let params = BoltMap::new();
        assert!(bolt_params_to_json(&params).is_none());
    }

    #[test]
    fn params_with_values() {
        let mut params = BoltMap::new();
        params.put(
            BoltString { value: "name".into() },
            BoltType::String(BoltString { value: "Bob".into() }),
        );
        let json = bolt_params_to_json(&params).unwrap();
        assert_eq!(json["name"], "Bob");
    }

    // ─── graph_value_to_bolt ───

    #[test]
    fn graph_null_to_bolt() {
        let gv = GraphValue::Null;
        match graph_value_to_bolt(&gv) {
            BoltType::Null(_) => {}
            other => panic!("Expected Null, got {other:?}"),
        }
    }

    #[test]
    fn graph_bool_to_bolt() {
        let gv = GraphValue::Bool(true);
        match graph_value_to_bolt(&gv) {
            BoltType::Boolean(b) => assert!(b.value),
            other => panic!("Expected Boolean, got {other:?}"),
        }
    }

    #[test]
    fn graph_int_to_bolt() {
        let gv = GraphValue::Int(99);
        match graph_value_to_bolt(&gv) {
            BoltType::Integer(i) => assert_eq!(i.value, 99),
            other => panic!("Expected Integer, got {other:?}"),
        }
    }

    #[test]
    fn graph_float_to_bolt() {
        let gv = GraphValue::Float(2.718);
        match graph_value_to_bolt(&gv) {
            BoltType::Float(f) => assert!((f.value - 2.718).abs() < 1e-10),
            other => panic!("Expected Float, got {other:?}"),
        }
    }

    #[test]
    fn graph_string_to_bolt() {
        let gv = GraphValue::String("hello".into());
        match graph_value_to_bolt(&gv) {
            BoltType::String(s) => assert_eq!(s.value, "hello"),
            other => panic!("Expected String, got {other:?}"),
        }
    }

    #[test]
    fn graph_list_to_bolt() {
        let gv = GraphValue::List(vec![GraphValue::Int(1), GraphValue::Int(2)]);
        match graph_value_to_bolt(&gv) {
            BoltType::List(list) => {
                assert_eq!(list.value.len(), 2);
                match &list.value[0] {
                    BoltType::Integer(i) => assert_eq!(i.value, 1),
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
            BoltType::Map(bmap) => {
                let val = bmap.value.get("key").unwrap();
                match val {
                    BoltType::String(s) => assert_eq!(s.value, "value"),
                    other => panic!("Expected String, got {other:?}"),
                }
            }
            other => panic!("Expected Map, got {other:?}"),
        }
    }

    // ─── json_to_bolt ───

    #[test]
    fn json_null_to_bolt() {
        match json_to_bolt(&serde_json::Value::Null) {
            BoltType::Null(_) => {}
            other => panic!("Expected Null, got {other:?}"),
        }
    }

    #[test]
    fn json_string_to_bolt() {
        match json_to_bolt(&serde_json::json!("test")) {
            BoltType::String(s) => assert_eq!(s.value, "test"),
            other => panic!("Expected String, got {other:?}"),
        }
    }

    #[test]
    fn json_integer_to_bolt() {
        match json_to_bolt(&serde_json::json!(42)) {
            BoltType::Integer(i) => assert_eq!(i.value, 42),
            other => panic!("Expected Integer, got {other:?}"),
        }
    }

    #[test]
    fn json_float_to_bolt() {
        match json_to_bolt(&serde_json::json!(3.14)) {
            BoltType::Float(f) => assert!((f.value - 3.14).abs() < 1e-10),
            other => panic!("Expected Float, got {other:?}"),
        }
    }

    #[test]
    fn json_array_to_bolt() {
        match json_to_bolt(&serde_json::json!([1, "two"])) {
            BoltType::List(list) => assert_eq!(list.value.len(), 2),
            other => panic!("Expected List, got {other:?}"),
        }
    }

    #[test]
    fn json_object_to_bolt() {
        match json_to_bolt(&serde_json::json!({"a": 1})) {
            BoltType::Map(map) => {
                assert_eq!(map.value.len(), 1);
                let val = map.value.get("a").unwrap();
                match val {
                    BoltType::Integer(i) => assert_eq!(i.value, 1),
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
        let back = bolt_type_to_json(&bolt);
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
        let json = bolt_type_to_json(&bolt);
        assert_eq!(json, serde_json::json!(["hello", 42, 1.5, false, null]));
    }

    // ─── extract_credentials ───

    #[test]
    fn extract_credentials_basic_scheme() {
        let mut extra = BoltMap::new();
        extra.put(
            BoltString { value: "scheme".into() },
            BoltType::String(BoltString { value: "basic".into() }),
        );
        extra.put(
            BoltString { value: "principal".into() },
            BoltType::String(BoltString { value: "neo4j".into() }),
        );
        extra.put(
            BoltString { value: "credentials".into() },
            BoltType::String(BoltString { value: "my-secret-token".into() }),
        );
        assert_eq!(extract_credentials(&extra), Some("my-secret-token"));
    }

    #[test]
    fn extract_credentials_missing() {
        let extra = BoltMap::new();
        assert_eq!(extract_credentials(&extra), None);
    }

    #[test]
    fn extract_credentials_non_string() {
        let mut extra = BoltMap::new();
        extra.put(
            BoltString { value: "credentials".into() },
            BoltType::Integer(BoltInteger { value: 42 }),
        );
        assert_eq!(extract_credentials(&extra), None);
    }

    #[test]
    fn extract_credentials_empty_string() {
        let mut extra = BoltMap::new();
        extra.put(
            BoltString { value: "credentials".into() },
            BoltType::String(BoltString { value: "".into() }),
        );
        assert_eq!(extract_credentials(&extra), Some(""));
    }

    // ─── negotiate_bolt_version ───

    #[test]
    fn version_exact_4_4() {
        // Client proposes exactly 4.4 in first slot.
        let mut v = [0u8; 16];
        v[0..4].copy_from_slice(&[0x00, 0x00, 0x04, 0x04]); // 4.4
        assert!(negotiate_bolt_version(&v));
    }

    #[test]
    fn version_range_includes_4_4() {
        // Client proposes 4.4 with range 2 → accepts 4.4, 4.3, 4.2.
        let mut v = [0u8; 16];
        v[0..4].copy_from_slice(&[0x00, 0x02, 0x04, 0x04]);
        assert!(negotiate_bolt_version(&v));
    }

    #[test]
    fn version_4_3_no_range() {
        // Client proposes only 4.3 — no range includes 4.4.
        let mut v = [0u8; 16];
        v[0..4].copy_from_slice(&[0x00, 0x00, 0x03, 0x04]); // 4.3
        assert!(!negotiate_bolt_version(&v));
    }

    #[test]
    fn version_5_0_with_range() {
        // Client proposes 5.0 with range 0 — doesn't include 4.x.
        let mut v = [0u8; 16];
        v[0..4].copy_from_slice(&[0x00, 0x00, 0x00, 0x05]); // 5.0
        assert!(!negotiate_bolt_version(&v));
    }

    #[test]
    fn version_in_second_slot() {
        // First slot is unsupported, second slot is 4.4.
        let mut v = [0u8; 16];
        v[0..4].copy_from_slice(&[0x00, 0x00, 0x00, 0x05]); // 5.0
        v[4..8].copy_from_slice(&[0x00, 0x00, 0x04, 0x04]); // 4.4
        assert!(negotiate_bolt_version(&v));
    }

    #[test]
    fn version_all_zeros() {
        let v = [0u8; 16];
        assert!(!negotiate_bolt_version(&v));
    }

    #[test]
    fn version_high_minor_range_includes_4_4() {
        // Client proposes 4.6 with range 3 → accepts 4.6, 4.5, 4.4, 4.3.
        let mut v = [0u8; 16];
        v[0..4].copy_from_slice(&[0x00, 0x03, 0x06, 0x04]);
        assert!(negotiate_bolt_version(&v));
    }
}
