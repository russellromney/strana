use tracing::warn;

use crate::journal_types::{self, JournalCommand, JournalSender, PendingEntry};
use crate::values::{self, json_params_to_lbug, GraphValue};

/// Result of executing a query via `run_query_raw`, including journal-relevant info.
pub struct RawExecution<'db> {
    pub query_result: lbug::QueryResult<'db>,
    pub columns: Vec<String>,
    /// The rewritten query (with non-deterministic functions replaced).
    pub rewritten_query: String,
    /// Merged params (user + generated). None if no params.
    pub merged_params: Option<serde_json::Value>,
}

/// Result of executing a query via `run_query`, with all rows materialized.
#[derive(Debug)]
pub struct ExecutedQuery {
    pub columns: Vec<String>,
    pub rows: Vec<Vec<GraphValue>>,
    /// The rewritten query (with non-deterministic functions replaced).
    pub rewritten_query: String,
    /// Merged params (user + generated). None if no params.
    pub merged_params: Option<serde_json::Value>,
}

/// Execute a query and return the raw QueryResult, column names, timing,
/// and the rewritten query + merged params for journaling.
pub fn run_query_raw<'db>(
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

    let merged_params = merged.clone();

    match result {
        Ok(qr) => {
            let columns = qr.get_column_names();
            Ok(RawExecution {
                query_result: qr,
                columns,
                rewritten_query: query_str,
                merged_params,
            })
        }
        Err(e) => Err(e),
    }
}

/// Take up to `limit` rows from a QueryResult. Returns (rows, has_more).
/// Uses `buf` to buffer a peeked row across calls — pass `&mut None` for one-shot reads.
pub(crate) fn take_rows(
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

/// Execute a single query, collecting all rows.
pub fn run_query(
    conn: &lbug::Connection<'_>,
    query: &str,
    params: Option<&serde_json::Value>,
) -> Result<ExecutedQuery, String> {
    let mut raw = run_query_raw(conn, query, params)?;
    let (rows, _) = take_rows(&mut raw.query_result, usize::MAX, &mut None);
    Ok(ExecutedQuery {
        columns: raw.columns,
        rows,
        rewritten_query: raw.rewritten_query,
        merged_params: raw.merged_params,
    })
}

/// Send a journal entry for a successful mutation (no-op if journal is disabled or query is read-only).
pub fn journal_entry(
    journal: &Option<JournalSender>,
    query: &str,
    params: &Option<serde_json::Value>,
) {
    if let Some(ref tx) = journal {
        if journal_types::is_mutation(query) {
            if tx
                .send(JournalCommand::Write(PendingEntry {
                    query: query.to_string(),
                    params: params.clone(),
                }))
                .is_err()
            {
                warn!("Journal write failed: channel disconnected (mutation not journaled)");
            }
        }
    }
}
