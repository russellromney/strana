use axum::body::Bytes;
use axum::extract::State;
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::Json;
use serde::{Deserialize, Serialize};
use std::sync::{Arc, RwLock};

use crate::auth::TokenStore;
use crate::backend::Backend;
use crate::journal::{self, JournalCommand, JournalSender, PendingEntry};
use crate::protocol::BatchResultEntry;
use crate::session::run_query;
use crate::server::AppState;
use crate::values::GraphValue;
use crate::wire;

// ─── Request types ───

#[derive(Debug, Deserialize)]
pub struct ExecuteRequest {
    pub query: String,
    #[serde(default)]
    pub params: Option<serde_json::Value>,
}

#[derive(Debug, Deserialize)]
pub struct BatchRequest {
    pub statements: Vec<StatementEntry>,
}

#[derive(Debug, Deserialize)]
pub struct StatementEntry {
    pub query: String,
    #[serde(default)]
    pub params: Option<serde_json::Value>,
}

// ─── Response types ───

#[derive(Debug, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum HttpResult {
    Result {
        columns: Vec<String>,
        rows: Vec<Vec<GraphValue>>,
        timing_ms: f64,
    },
    Error {
        message: String,
    },
    BatchResult {
        results: Vec<BatchResultEntry>,
    },
    PipelineResult {
        results: Vec<BatchResultEntry>,
    },
}

// ─── Content-type helpers ───

fn is_protobuf(headers: &HeaderMap) -> bool {
    headers
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .map_or(false, |ct| ct.contains("protobuf"))
}

fn respond(is_proto: bool, status: StatusCode, result: &HttpResult) -> Response {
    if is_proto {
        let bytes = wire::http_result_to_proto(result);
        (status, [("content-type", "application/x-protobuf")], bytes).into_response()
    } else {
        (status, Json(result)).into_response()
    }
}

fn error_response(is_proto: bool, status: StatusCode, message: &str) -> Response {
    respond(
        is_proto,
        status,
        &HttpResult::Error {
            message: message.to_string(),
        },
    )
}

// ─── Auth ───

fn validate_bearer(
    headers: &HeaderMap,
    tokens: &TokenStore,
    is_proto: bool,
) -> Result<(), Response> {
    if tokens.is_empty() {
        return Ok(());
    }
    let token = headers
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "));
    match token {
        Some(t) => tokens
            .validate(t)
            .map(|_| ())
            .map_err(|_| error_response(is_proto, StatusCode::UNAUTHORIZED, "Unauthorized")),
        None => Err(error_response(
            is_proto,
            StatusCode::UNAUTHORIZED,
            "Unauthorized",
        )),
    }
}

// ─── Handlers ───

/// POST /v1/execute — single query, single response.
pub async fn execute_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let proto = is_protobuf(&headers);
    if let Err(e) = validate_bearer(&headers, &state.tokens, proto) {
        return e;
    }

    let (query, params) = if proto {
        match wire::decode_execute_request(&body) {
            Ok(r) => r,
            Err(e) => {
                return error_response(true, StatusCode::BAD_REQUEST, &format!("Invalid request body: {e}"));
            }
        }
    } else {
        match serde_json::from_slice::<ExecuteRequest>(&body) {
            Ok(r) => (r.query, r.params),
            Err(e) => {
                return error_response(false, StatusCode::BAD_REQUEST, &format!("Invalid request body: {e}"));
            }
        }
    };

    let db = state.db.clone();
    let journal = state.journal.clone();
    let lock = state.snapshot_lock.clone();
    let result = tokio::task::spawn_blocking(move || {
        execute_on_connection(&db, &query, params.as_ref(), &journal, &lock)
    })
    .await
    .unwrap();

    respond(proto, StatusCode::OK, &result)
}

/// POST /v1/batch — array of statements, no transaction.
pub async fn batch_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let proto = is_protobuf(&headers);
    if let Err(e) = validate_bearer(&headers, &state.tokens, proto) {
        return e;
    }

    let statements = if proto {
        match wire::decode_batch_request(&body) {
            Ok(s) => s,
            Err(e) => {
                return error_response(true, StatusCode::BAD_REQUEST, &format!("Invalid request body: {e}"));
            }
        }
    } else {
        match serde_json::from_slice::<BatchRequest>(&body) {
            Ok(r) => r.statements,
            Err(e) => {
                return error_response(false, StatusCode::BAD_REQUEST, &format!("Invalid request body: {e}"));
            }
        }
    };

    let db = state.db.clone();
    let journal = state.journal.clone();
    let lock = state.snapshot_lock.clone();
    let result =
        tokio::task::spawn_blocking(move || execute_batch_on_connection(&db, &statements, &journal, &lock))
            .await
            .unwrap();

    respond(proto, StatusCode::OK, &result)
}

/// POST /v1/pipeline — array of statements in a transaction. Rolls back on error.
pub async fn pipeline_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let proto = is_protobuf(&headers);
    if let Err(e) = validate_bearer(&headers, &state.tokens, proto) {
        return e;
    }

    let statements = if proto {
        match wire::decode_batch_request(&body) {
            Ok(s) => s,
            Err(e) => {
                return error_response(true, StatusCode::BAD_REQUEST, &format!("Invalid request body: {e}"));
            }
        }
    } else {
        match serde_json::from_slice::<BatchRequest>(&body) {
            Ok(r) => r.statements,
            Err(e) => {
                return error_response(false, StatusCode::BAD_REQUEST, &format!("Invalid request body: {e}"));
            }
        }
    };

    let db = state.db.clone();
    let journal = state.journal.clone();
    let lock = state.snapshot_lock.clone();
    let result =
        tokio::task::spawn_blocking(move || execute_pipeline_on_connection(&db, &statements, &journal, &lock))
            .await
            .unwrap();

    respond(proto, StatusCode::OK, &result)
}

/// POST /v1/snapshot — create a point-in-time snapshot (requires journal to be enabled).
pub async fn snapshot_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Response {
    let proto = is_protobuf(&headers);
    if let Err(e) = validate_bearer(&headers, &state.tokens, proto) {
        return e;
    }

    let (journal, journal_state) = match (&state.journal, &state.journal_state) {
        (Some(j), Some(s)) => (j.clone(), s.clone()),
        _ => {
            return error_response(proto, StatusCode::BAD_REQUEST, "Journal is not enabled");
        }
    };

    let db = state.db.clone();
    let data_dir = state.data_dir.clone();
    let lock = state.snapshot_lock.clone();
    let retention = state.retention_config.clone();
    let s3_bucket = state.s3_bucket.clone();
    let s3_prefix = state.s3_prefix.clone();

    let result = tokio::task::spawn_blocking(move || {
        // Write lock blocks all mutations from executing + journaling.
        let _lock = lock.write().unwrap();
        crate::snapshot::create_snapshot(&data_dir, &db, &journal, &journal_state, &retention)
    })
    .await
    .unwrap();

    match result {
        Ok(info) => {
            // Upload to S3 if configured (best-effort — don't fail the response).
            if let Some(ref bucket) = s3_bucket {
                if let Err(e) = crate::snapshot::upload_snapshot_s3(
                    &info.path,
                    bucket,
                    &s3_prefix,
                    info.sequence,
                )
                .await
                {
                    tracing::warn!("S3 snapshot upload failed: {e}");
                }
            }

            respond(
                proto,
                StatusCode::OK,
                &HttpResult::Result {
                    columns: vec!["sequence".into(), "path".into()],
                    rows: vec![vec![
                        GraphValue::Int(info.sequence as i64),
                        GraphValue::String(info.path.display().to_string()),
                    ]],
                    timing_ms: 0.0,
                },
            )
        }
        Err(e) => error_response(proto, StatusCode::INTERNAL_SERVER_ERROR, &e),
    }
}

// ─── Journal helper ───

/// Send a journal entry for a successful mutation (no-op if journal is disabled or query is read-only).
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

// ─── Blocking execution helpers ───

fn execute_on_connection(
    db: &Arc<Backend>,
    query: &str,
    params: Option<&serde_json::Value>,
    journal: &Option<JournalSender>,
    snapshot_lock: &Arc<RwLock<()>>,
) -> HttpResult {
    let conn = match db.connection() {
        Ok(c) => c,
        Err(e) => {
            return HttpResult::Error {
                message: format!("Failed to create connection: {e}"),
            };
        }
    };
    let _lock = snapshot_lock.read().unwrap();
    match run_query(&conn, query, params) {
        Ok(eq) => {
            journal_entry(journal, &eq.rewritten_query, &eq.merged_params);
            HttpResult::Result {
                columns: eq.columns,
                rows: eq.rows,
                timing_ms: eq.timing_ms,
            }
        }
        Err(e) => HttpResult::Error { message: e },
    }
}

fn execute_batch_on_connection(
    db: &Arc<Backend>,
    statements: &[StatementEntry],
    journal: &Option<JournalSender>,
    snapshot_lock: &Arc<RwLock<()>>,
) -> HttpResult {
    let conn = match db.connection() {
        Ok(c) => c,
        Err(e) => {
            return HttpResult::Error {
                message: format!("Failed to create connection: {e}"),
            };
        }
    };

    let _lock = snapshot_lock.read().unwrap();
    let mut results = Vec::with_capacity(statements.len());
    for stmt in statements {
        match run_query(&conn, &stmt.query, stmt.params.as_ref()) {
            Ok(eq) => {
                journal_entry(journal, &eq.rewritten_query, &eq.merged_params);
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

    HttpResult::BatchResult { results }
}

fn execute_pipeline_on_connection(
    db: &Arc<Backend>,
    statements: &[StatementEntry],
    journal: &Option<JournalSender>,
    snapshot_lock: &Arc<RwLock<()>>,
) -> HttpResult {
    let conn = match db.connection() {
        Ok(c) => c,
        Err(e) => {
            return HttpResult::Error {
                message: format!("Failed to create connection: {e}"),
            };
        }
    };

    // Begin transaction
    if let Err(e) = conn.query("BEGIN TRANSACTION") {
        return HttpResult::Error {
            message: format!("Failed to begin transaction: {e}"),
        };
    }

    let mut results = Vec::with_capacity(statements.len());
    let mut journal_buf: Vec<PendingEntry> = Vec::new();
    let mut had_error = false;

    for stmt in statements {
        match run_query(&conn, &stmt.query, stmt.params.as_ref()) {
            Ok(eq) => {
                if journal::is_mutation(&eq.rewritten_query) {
                    journal_buf.push(PendingEntry {
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
                had_error = true;
                break;
            }
        }
    }

    // Commit or rollback — read lock around commit + journal flush.
    if had_error {
        let _ = conn.query("ROLLBACK");
    } else {
        let _lock = snapshot_lock.read().unwrap();
        if let Err(e) = conn.query("COMMIT") {
            drop(_lock);
            let _ = conn.query("ROLLBACK");
            results.push(BatchResultEntry::Error {
                message: format!("Commit failed: {e}"),
            });
        } else {
            if let Some(ref tx) = journal {
                for entry in journal_buf {
                    let _ = tx.send(JournalCommand::Write(entry));
                }
            }
        }
    }

    HttpResult::PipelineResult { results }
}
