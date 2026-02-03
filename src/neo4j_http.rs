use axum::extract::{Path, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::Json;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::Instant;
use tracing::warn;

use crate::auth::TokenStore;
use crate::engine::Engine;
use crate::journal::{self, JournalCommand, PendingEntry};
use crate::query::{self, ExecutedQuery};
use crate::snapshot::RetentionConfig;
use crate::values::GraphValue;

/// Shared application state.
#[derive(Clone)]
pub struct AppState {
    pub engine: Arc<Engine>,
    pub tokens: Arc<TokenStore>,
    pub journal_state: Option<Arc<crate::journal::JournalState>>,
    pub data_dir: std::path::PathBuf,
    pub retention_config: RetentionConfig,
    pub s3_bucket: Option<String>,
    pub s3_prefix: String,
    /// HTTP transaction store: tx_id → TransactionState.
    pub transactions: Arc<RwLock<HashMap<String, Arc<TransactionState>>>>,
}

/// An open HTTP transaction.
pub struct TransactionState {
    req_tx: std::sync::mpsc::Sender<TxOp>,
    resp_rx: std::sync::Mutex<std::sync::mpsc::Receiver<TxResult>>,
    pub last_accessed: std::sync::Mutex<Instant>,
}

enum TxOp {
    Run { query: String, params: Option<serde_json::Value> },
    Commit,
    Rollback,
}

enum TxResult {
    QueryResult(Result<ExecutedQuery, String>),
    Ok,
    Err(String),
}

// ─── Request / Response types ───

#[derive(Debug, Deserialize)]
pub struct QueryRequest {
    pub statement: String,
    #[serde(default)]
    pub parameters: Option<serde_json::Value>,
}

#[derive(Debug, Serialize)]
pub struct QueryResponse {
    pub data: QueryData,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub bookmarks: Vec<String>,
}

#[derive(Debug, Serialize)]
pub struct QueryData {
    pub fields: Vec<String>,
    pub values: Vec<Vec<serde_json::Value>>,
}

#[derive(Debug, Serialize)]
pub struct TxBeginResponse {
    pub id: String,
}

#[derive(Debug, Serialize)]
pub struct ErrorResponse {
    pub errors: Vec<ErrorDetail>,
}

#[derive(Debug, Serialize)]
pub struct ErrorDetail {
    pub message: String,
    pub code: String,
}

// ─── Auth ───

fn validate_auth(headers: &HeaderMap, tokens: &TokenStore) -> Result<(), Response> {
    if tokens.is_empty() {
        return Ok(());
    }

    // Try Bearer token first.
    if let Some(auth) = headers
        .get("authorization")
        .and_then(|v| v.to_str().ok())
    {
        if let Some(token) = auth.strip_prefix("Bearer ") {
            return tokens
                .validate(token)
                .map(|_| ())
                .map_err(|_| error_response(StatusCode::UNAUTHORIZED, "Unauthorized"));
        }
        // Try Basic auth — password is the token.
        if let Some(encoded) = auth.strip_prefix("Basic ") {
            if let Ok(decoded) = base64::Engine::decode(
                &base64::engine::general_purpose::STANDARD,
                encoded,
            ) {
                if let Ok(s) = String::from_utf8(decoded) {
                    if let Some((_user, pass)) = s.split_once(':') {
                        return tokens
                            .validate(pass)
                            .map(|_| ())
                            .map_err(|_| {
                                error_response(StatusCode::UNAUTHORIZED, "Unauthorized")
                            });
                    }
                }
            }
        }
    }

    Err(error_response(StatusCode::UNAUTHORIZED, "Unauthorized"))
}

fn error_response(status: StatusCode, message: &str) -> Response {
    (
        status,
        Json(ErrorResponse {
            errors: vec![ErrorDetail {
                message: message.to_string(),
                code: "Neo.ClientError.Security.Unauthorized".to_string(),
            }],
        }),
    )
        .into_response()
}

fn db_error_response(message: &str) -> Response {
    (
        StatusCode::UNPROCESSABLE_ENTITY,
        Json(ErrorResponse {
            errors: vec![ErrorDetail {
                message: message.to_string(),
                code: "Neo.DatabaseError.General.UnknownError".to_string(),
            }],
        }),
    )
        .into_response()
}

// ─── Helpers ───

fn rows_to_json(rows: &[Vec<GraphValue>]) -> Vec<Vec<serde_json::Value>> {
    rows.iter()
        .map(|row| {
            row.iter()
                .map(|v| serde_json::to_value(v).unwrap_or(serde_json::Value::Null))
                .collect()
        })
        .collect()
}

// ─── Handlers ───

/// POST /db/{name}/query/v2 — auto-commit query.
pub async fn query_handler(
    State(state): State<AppState>,
    Path(_db_name): Path<String>,
    headers: HeaderMap,
    Json(req): Json<QueryRequest>,
) -> Response {
    if let Err(e) = validate_auth(&headers, &state.tokens) {
        return e;
    }

    match state.engine.execute(req.statement, req.parameters).await {
        Ok(eq) => (
            StatusCode::ACCEPTED,
            Json(QueryResponse {
                data: QueryData {
                    fields: eq.columns,
                    values: rows_to_json(&eq.rows),
                },
                bookmarks: vec![],
            }),
        )
            .into_response(),
        Err(e) => db_error_response(&e),
    }
}

/// POST /db/{name}/query/v2/tx — begin transaction.
///
/// Waits for the blocking worker to confirm BEGIN TRANSACTION succeeded before
/// returning the transaction ID. This means the client only receives a tx_id
/// that is immediately usable, and gets the real error if BEGIN fails.
pub async fn begin_handler(
    State(state): State<AppState>,
    Path(_db_name): Path<String>,
    headers: HeaderMap,
) -> Response {
    if let Err(e) = validate_auth(&headers, &state.tokens) {
        return e;
    }

    let tx_id = uuid::Uuid::new_v4().to_string();
    let engine = state.engine.clone();

    let (req_tx, req_rx_worker) = std::sync::mpsc::channel::<TxOp>();
    let (resp_tx_worker, resp_rx) = std::sync::mpsc::channel::<TxResult>();
    let (begin_tx, begin_rx) = tokio::sync::oneshot::channel::<Result<(), String>>();

    // Spawn a blocking worker that owns the connection + transaction.
    // The worker acquires the Engine's write lock for the transaction's lifetime.
    tokio::task::spawn_blocking(move || {
        tx_worker(engine, begin_tx, req_rx_worker, resp_tx_worker);
    });

    // Wait for the worker to confirm BEGIN succeeded.
    match begin_rx.await {
        Ok(Ok(())) => {
            let tx_state = Arc::new(TransactionState {
                req_tx,
                resp_rx: std::sync::Mutex::new(resp_rx),
                last_accessed: std::sync::Mutex::new(Instant::now()),
            });
            state.transactions.write().unwrap_or_else(|e| e.into_inner())
                .insert(tx_id.clone(), tx_state);
            (StatusCode::ACCEPTED, Json(TxBeginResponse { id: tx_id })).into_response()
        }
        Ok(Err(e)) => db_error_response(&e),
        Err(_) => db_error_response("Transaction worker failed to start"),
    }
}

/// POST /db/{name}/query/v2/tx/{id} — run query in existing transaction.
pub async fn run_in_tx_handler(
    State(state): State<AppState>,
    Path((_db_name, tx_id)): Path<(String, String)>,
    headers: HeaderMap,
    Json(req): Json<QueryRequest>,
) -> Response {
    if let Err(e) = validate_auth(&headers, &state.tokens) {
        return e;
    }

    let tx_state = {
        let txs = state.transactions.read().unwrap_or_else(|e| e.into_inner());
        match txs.get(&tx_id) {
            Some(t) => t.clone(),
            None => return db_error_response(&format!("Transaction {tx_id} not found")),
        }
    }; // map lock dropped here

    // Update last-accessed timestamp.
    *tx_state.last_accessed.lock().unwrap_or_else(|e| e.into_inner()) = Instant::now();

    if tx_state
        .req_tx
        .send(TxOp::Run {
            query: req.statement,
            params: req.parameters,
        })
        .is_err()
    {
        state.transactions.write().unwrap_or_else(|e| e.into_inner()).remove(&tx_id);
        return db_error_response("Transaction worker died");
    }

    let resp = tx_state.resp_rx.lock().unwrap_or_else(|e| e.into_inner()).recv();

    match resp {
        Ok(TxResult::QueryResult(Ok(eq))) => (
            StatusCode::ACCEPTED,
            Json(QueryResponse {
                data: QueryData {
                    fields: eq.columns,
                    values: rows_to_json(&eq.rows),
                },
                bookmarks: vec![],
            }),
        )
            .into_response(),
        Ok(TxResult::QueryResult(Err(e))) => db_error_response(&e),
        _ => db_error_response("Unexpected response from transaction worker"),
    }
}

/// POST /db/{name}/query/v2/tx/{id}/commit — commit transaction.
pub async fn commit_handler(
    State(state): State<AppState>,
    Path((_db_name, tx_id)): Path<(String, String)>,
    headers: HeaderMap,
) -> Response {
    if let Err(e) = validate_auth(&headers, &state.tokens) {
        return e;
    }

    let tx_state = {
        let txs = state.transactions.read().unwrap_or_else(|e| e.into_inner());
        match txs.get(&tx_id) {
            Some(t) => t.clone(),
            None => return db_error_response(&format!("Transaction {tx_id} not found")),
        }
    }; // map lock dropped here

    if tx_state.req_tx.send(TxOp::Commit).is_err() {
        state.transactions.write().unwrap_or_else(|e| e.into_inner()).remove(&tx_id);
        return db_error_response("Transaction worker died");
    }

    let resp = tx_state.resp_rx.lock().unwrap_or_else(|e| e.into_inner()).recv();

    // Remove transaction after commit.
    state.transactions.write().unwrap_or_else(|e| e.into_inner()).remove(&tx_id);

    match resp {
        Ok(TxResult::Ok) => (StatusCode::ACCEPTED, Json(serde_json::json!({}))).into_response(),
        Ok(TxResult::Err(e)) => db_error_response(&e),
        _ => db_error_response("Unexpected response"),
    }
}

/// DELETE /db/{name}/query/v2/tx/{id} — rollback transaction.
pub async fn rollback_handler(
    State(state): State<AppState>,
    Path((_db_name, tx_id)): Path<(String, String)>,
    headers: HeaderMap,
) -> Response {
    if let Err(e) = validate_auth(&headers, &state.tokens) {
        return e;
    }

    let tx_state = {
        let txs = state.transactions.read().unwrap_or_else(|e| e.into_inner());
        match txs.get(&tx_id) {
            Some(t) => t.clone(),
            None => return db_error_response(&format!("Transaction {tx_id} not found")),
        }
    }; // map lock dropped here

    if tx_state.req_tx.send(TxOp::Rollback).is_err() {
        // Worker already dead — transaction rolled back on drop.
    }

    let resp = tx_state.resp_rx.lock().unwrap_or_else(|e| e.into_inner()).recv();

    state.transactions.write().unwrap_or_else(|e| e.into_inner()).remove(&tx_id);

    match resp {
        Ok(TxResult::Ok) | Err(_) => {
            (StatusCode::ACCEPTED, Json(serde_json::json!({}))).into_response()
        }
        Ok(TxResult::Err(e)) => db_error_response(&e),
        _ => (StatusCode::ACCEPTED, Json(serde_json::json!({}))).into_response(),
    }
}

/// POST /v1/snapshot — create a point-in-time snapshot.
pub async fn snapshot_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Response {
    if let Err(e) = validate_auth(&headers, &state.tokens) {
        return e;
    }

    let (journal, journal_state) = match (state.engine.journal(), &state.journal_state) {
        (Some(j), Some(s)) => (j.clone(), s.clone()),
        _ => {
            return db_error_response("Journal is not enabled");
        }
    };

    let db = state.engine.backend().clone();
    let data_dir = state.data_dir.clone();
    let lock = state.engine.snapshot_lock().clone();
    let retention = state.retention_config.clone();
    let s3_bucket = state.s3_bucket.clone();
    let s3_prefix = state.s3_prefix.clone();

    let result = tokio::task::spawn_blocking(move || {
        let _lock = lock.write().unwrap_or_else(|e| e.into_inner());
        crate::snapshot::create_snapshot(&data_dir, &db, &journal, &journal_state, &retention)
    })
    .await
    .unwrap();

    match result {
        Ok(info) => {
            if let Some(ref bucket) = s3_bucket {
                if let Err(e) = crate::snapshot::upload_snapshot_s3(
                    &info.path,
                    bucket,
                    &s3_prefix,
                    info.sequence,
                )
                .await
                {
                    warn!("S3 snapshot upload failed: {e}");
                }
            }

            (
                StatusCode::OK,
                Json(QueryResponse {
                    data: QueryData {
                        fields: vec!["sequence".into(), "path".into()],
                        values: vec![vec![
                            serde_json::json!(info.sequence),
                            serde_json::json!(info.path.display().to_string()),
                        ]],
                    },
                    bookmarks: vec![],
                }),
            )
                .into_response()
        }
        Err(e) => db_error_response(&e),
    }
}

// ─── Transaction worker ───

fn tx_worker(
    engine: Arc<Engine>,
    begin_tx: tokio::sync::oneshot::Sender<Result<(), String>>,
    rx: std::sync::mpsc::Receiver<TxOp>,
    tx: std::sync::mpsc::Sender<TxResult>,
) {
    // Acquire the Engine's write lock for the transaction's lifetime.
    // Released when the function returns (COMMIT, ROLLBACK, or channel close).
    let _write_guard = engine.acquire_writer();

    let conn = match engine.connection() {
        Ok(c) => c,
        Err(e) => {
            let _ = begin_tx.send(Err(format!("Connection: {e}")));
            return;
        }
    };
    let journal = engine.journal().clone();
    let snapshot_lock = engine.snapshot_lock().clone();

    if let Err(e) = conn.query("BEGIN TRANSACTION") {
        let _ = begin_tx.send(Err(format!("BEGIN: {e}")));
        return;
    }

    // Signal that BEGIN succeeded — begin_handler can now return the tx_id.
    let _ = begin_tx.send(Ok(()));

    let mut journal_buf: Vec<PendingEntry> = Vec::new();

    while let Ok(op) = rx.recv() {
        match op {
            TxOp::Run { query: q, params } => {
                let result = query::run_query(&conn, &q, params.as_ref());
                if let Ok(ref eq) = result {
                    if journal::is_mutation(&eq.rewritten_query) {
                        journal_buf.push(PendingEntry {
                            query: eq.rewritten_query.clone(),
                            params: eq.merged_params.clone(),
                        });
                    }
                }
                let _ = tx.send(TxResult::QueryResult(result));
            }
            TxOp::Commit => {
                let _lock = snapshot_lock.read().unwrap_or_else(|e| e.into_inner());
                match conn.query("COMMIT") {
                    Ok(_) => {
                        // Flush buffered journal entries.
                        if let Some(ref jtx) = journal {
                            for entry in journal_buf.drain(..) {
                                if jtx.send(JournalCommand::Write(entry)).is_err() {
                                    warn!("Journal write failed: channel disconnected");
                                }
                            }
                        }
                        let _ = tx.send(TxResult::Ok);
                    }
                    Err(e) => {
                        let _ = tx.send(TxResult::Err(format!("COMMIT: {e}")));
                    }
                }
                return;
            }
            TxOp::Rollback => {
                let _ = conn.query("ROLLBACK");
                let _ = tx.send(TxResult::Ok);
                return;
            }
        }
    }

    // Channel closed without commit — auto-rollback.
    let _ = conn.query("ROLLBACK");
}

// ─── Transaction reaper ───

/// Spawn a background task that reaps abandoned HTTP transactions.
/// Runs every 10 seconds, removes transactions whose `last_accessed` exceeds `timeout`.
pub fn spawn_tx_reaper(
    transactions: Arc<RwLock<HashMap<String, Arc<TransactionState>>>>,
    timeout: std::time::Duration,
) {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(10));
        loop {
            interval.tick().await;
            let stale: Vec<String> = {
                let txs = transactions.read().unwrap_or_else(|e| e.into_inner());
                txs.iter()
                    .filter_map(|(id, state)| {
                        let last = *state.last_accessed.lock().unwrap_or_else(|e| e.into_inner());
                        if last.elapsed() > timeout {
                            Some(id.clone())
                        } else {
                            None
                        }
                    })
                    .collect()
            };
            if !stale.is_empty() {
                let mut txs = transactions.write().unwrap_or_else(|e| e.into_inner());
                for id in &stale {
                    txs.remove(id);
                }
                tracing::info!("Reaped {} abandoned transaction(s)", stale.len());
            }
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    // ─── rows_to_json ───

    #[test]
    fn empty_rows() {
        let rows: Vec<Vec<GraphValue>> = vec![];
        let json = rows_to_json(&rows);
        assert!(json.is_empty());
    }

    #[test]
    fn single_row_primitives() {
        let rows = vec![vec![
            GraphValue::String("Alice".into()),
            GraphValue::Int(30),
            GraphValue::Bool(true),
        ]];
        let json = rows_to_json(&rows);
        assert_eq!(json.len(), 1);
        assert_eq!(json[0][0], serde_json::json!("Alice"));
        assert_eq!(json[0][1], serde_json::json!(30));
        assert_eq!(json[0][2], serde_json::json!(true));
    }

    #[test]
    fn multiple_rows() {
        let rows = vec![
            vec![GraphValue::Int(1)],
            vec![GraphValue::Int(2)],
            vec![GraphValue::Int(3)],
        ];
        let json = rows_to_json(&rows);
        assert_eq!(json.len(), 3);
        assert_eq!(json[0][0], serde_json::json!(1));
        assert_eq!(json[2][0], serde_json::json!(3));
    }

    #[test]
    fn null_and_float_values() {
        let rows = vec![vec![GraphValue::Null, GraphValue::Float(3.14)]];
        let json = rows_to_json(&rows);
        assert!(json[0][0].is_null());
        assert_eq!(json[0][1], serde_json::json!(3.14));
    }

    #[test]
    fn nested_list_and_map() {
        let mut hm = std::collections::HashMap::new();
        hm.insert("key".to_string(), GraphValue::String("val".into()));
        let rows = vec![vec![
            GraphValue::List(vec![GraphValue::Int(1), GraphValue::Int(2)]),
            GraphValue::Map(hm),
        ]];
        let json = rows_to_json(&rows);
        assert_eq!(json[0][0], serde_json::json!([1, 2]));
        assert_eq!(json[0][1]["key"], serde_json::json!("val"));
    }

    // ─── validate_auth ───

    #[test]
    fn open_access_always_passes() {
        let tokens = TokenStore::open();
        let headers = HeaderMap::new();
        assert!(validate_auth(&headers, &tokens).is_ok());
    }

    #[test]
    fn bearer_auth_valid_token() {
        let token = "test-secret-token";
        let tokens = TokenStore::from_token(token);
        let mut headers = HeaderMap::new();
        headers.insert("authorization", format!("Bearer {token}").parse().unwrap());
        assert!(validate_auth(&headers, &tokens).is_ok());
    }

    #[test]
    fn bearer_auth_invalid_token() {
        let tokens = TokenStore::from_token("correct-token");
        let mut headers = HeaderMap::new();
        headers.insert("authorization", "Bearer wrong-token".parse().unwrap());
        assert!(validate_auth(&headers, &tokens).is_err());
    }

    #[test]
    fn basic_auth_password_is_token() {
        let token = "my-secret";
        let tokens = TokenStore::from_token(token);
        let encoded = base64::Engine::encode(
            &base64::engine::general_purpose::STANDARD,
            format!("neo4j:{token}"),
        );
        let mut headers = HeaderMap::new();
        headers.insert("authorization", format!("Basic {encoded}").parse().unwrap());
        assert!(validate_auth(&headers, &tokens).is_ok());
    }

    #[test]
    fn basic_auth_wrong_password() {
        let tokens = TokenStore::from_token("correct");
        let encoded = base64::Engine::encode(
            &base64::engine::general_purpose::STANDARD,
            "neo4j:wrong",
        );
        let mut headers = HeaderMap::new();
        headers.insert("authorization", format!("Basic {encoded}").parse().unwrap());
        assert!(validate_auth(&headers, &tokens).is_err());
    }

    #[test]
    fn missing_auth_header_with_tokens_fails() {
        let tokens = TokenStore::from_token("secret");
        let headers = HeaderMap::new();
        assert!(validate_auth(&headers, &tokens).is_err());
    }

    // ─── Request deserialization ───

    #[test]
    fn query_request_deserialize() {
        let json = serde_json::json!({
            "statement": "MATCH (n) RETURN n",
            "parameters": {"name": "Alice"}
        });
        let req: QueryRequest = serde_json::from_value(json).unwrap();
        assert_eq!(req.statement, "MATCH (n) RETURN n");
        assert!(req.parameters.is_some());
    }

    #[test]
    fn query_request_without_params() {
        let json = serde_json::json!({ "statement": "RETURN 1" });
        let req: QueryRequest = serde_json::from_value(json).unwrap();
        assert_eq!(req.statement, "RETURN 1");
        assert!(req.parameters.is_none());
    }

    // ─── Response serialization ───

    #[test]
    fn query_response_serialize() {
        let resp = QueryResponse {
            data: QueryData {
                fields: vec!["n".into()],
                values: vec![vec![serde_json::json!(42)]],
            },
            bookmarks: vec![],
        };
        let json = serde_json::to_value(&resp).unwrap();
        assert_eq!(json["data"]["fields"], serde_json::json!(["n"]));
        assert_eq!(json["data"]["values"], serde_json::json!([[42]]));
        // bookmarks should be omitted when empty
        assert!(json.get("bookmarks").is_none());
    }
}
