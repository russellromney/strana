use serde_json::json;
use std::sync::Arc;
use graphd::auth::TokenStore;
use graphd::backend::Backend;
use graphd::engine::Engine;
use graphd::neo4j_http;
use graphd::snapshot::RetentionConfig;
use std::collections::HashMap;
use tokio::net::TcpListener;

// ─── Server helpers ───

struct TestServer {
    http: String,
    #[allow(dead_code)]
    bolt_addr: String,
    _data_dir: tempfile::TempDir,
}

async fn start_server() -> TestServer {
    start_server_with_config(Arc::new(TokenStore::open()), false).await
}

async fn start_server_with_token(token: &str) -> TestServer {
    start_server_with_config(Arc::new(TokenStore::from_token(token)), false).await
}

async fn start_server_with_journal() -> TestServer {
    start_server_with_config(Arc::new(TokenStore::open()), true).await
}

async fn start_server_with_config(
    tokens: Arc<TokenStore>,
    journal_enabled: bool,
) -> TestServer {
    let data_dir = tempfile::tempdir().unwrap();
    let db_path = data_dir.path().join("db");
    let db = Backend::open(&db_path).expect("Failed to open database");
    let db = Arc::new(db);

    let (journal_tx, journal_state) = if journal_enabled {
        let journal_dir = data_dir.path().join("journal");
        std::fs::create_dir_all(&journal_dir).unwrap();
        let (seq, hash) =
            graphd::journal::recover_journal_state(&journal_dir).unwrap_or_else(|e| {
                panic!("Failed to recover journal state: {e}");
            });
        let state = Arc::new(graphd::journal::JournalState::with_sequence_and_hash(
            seq, hash,
        ));
        let tx = graphd::journal::spawn_journal_writer(
            journal_dir,
            64 * 1024 * 1024,
            100,
            state.clone(),
        );
        (Some(tx), Some(state))
    } else {
        (None, None)
    };

    let engine = Arc::new(Engine::new(db, 2, journal_tx));

    // Start Bolt listener (placeholder — Bolt integration tests need raw TCP client).
    let bolt_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let bolt_addr = bolt_listener.local_addr().unwrap();

    // Start HTTP listener.
    let http_state = neo4j_http::AppState {
        engine,
        tokens,
        journal_state,
        data_dir: data_dir.path().to_path_buf(),
        retention_config: RetentionConfig {
            daily: 7,
            weekly: 4,
            monthly: 3,
        },
        s3_bucket: None,
        s3_prefix: String::new(),
        transactions: Arc::new(std::sync::RwLock::new(HashMap::new())),
    };

    let app = axum::Router::new()
        .route(
            "/db/{name}/query/v2",
            axum::routing::post(neo4j_http::query_handler),
        )
        .route(
            "/db/{name}/query/v2/tx",
            axum::routing::post(neo4j_http::begin_handler),
        )
        .route(
            "/db/{name}/query/v2/tx/{id}",
            axum::routing::post(neo4j_http::run_in_tx_handler)
                .delete(neo4j_http::rollback_handler),
        )
        .route(
            "/db/{name}/query/v2/tx/{id}/commit",
            axum::routing::post(neo4j_http::commit_handler),
        )
        .route(
            "/v1/snapshot",
            axum::routing::post(neo4j_http::snapshot_handler),
        )
        .route("/health", axum::routing::get(|| async { "ok" }))
        .with_state(http_state);

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let http_addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    TestServer {
        http: format!("http://127.0.0.1:{}", http_addr.port()),
        bolt_addr: format!("127.0.0.1:{}", bolt_addr.port()),
        _data_dir: data_dir,
    }
}

fn client() -> reqwest::Client {
    reqwest::Client::new()
}

async fn exec(base: &str, statement: &str) -> reqwest::Response {
    client()
        .post(format!("{base}/db/graph/query/v2"))
        .json(&json!({ "statement": statement }))
        .send()
        .await
        .unwrap()
}

async fn exec_ok(base: &str, statement: &str) {
    let resp = exec(base, statement).await;
    let status = resp.status();
    if status != 202 {
        let body: serde_json::Value = resp.json().await.unwrap();
        panic!("Query failed ({status}): {body:#}");
    }
}

async fn exec_json(base: &str, statement: &str) -> serde_json::Value {
    let resp = exec(base, statement).await;
    let status = resp.status();
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(status.as_u16(), 202, "Query failed: {body:#}");
    body
}

async fn exec_params(
    base: &str,
    statement: &str,
    params: serde_json::Value,
) -> reqwest::Response {
    client()
        .post(format!("{base}/db/graph/query/v2"))
        .json(&json!({ "statement": statement, "parameters": params }))
        .send()
        .await
        .unwrap()
}

async fn exec_params_json(
    base: &str,
    statement: &str,
    params: serde_json::Value,
) -> serde_json::Value {
    let resp = exec_params(base, statement, params).await;
    let status = resp.status();
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(status.as_u16(), 202, "Query failed: {body:#}");
    body
}

async fn exec_with_auth(
    base: &str,
    statement: &str,
    token: &str,
) -> reqwest::Response {
    client()
        .post(format!("{base}/db/graph/query/v2"))
        .bearer_auth(token)
        .json(&json!({ "statement": statement }))
        .send()
        .await
        .unwrap()
}

/// Create the Person node table (common across many tests).
async fn create_person_table(base: &str) {
    exec_ok(
        base,
        "CREATE NODE TABLE Person(name STRING, age INT64, id STRING, tags STRING[], PRIMARY KEY(name))",
    )
    .await;
}

// ─── Health ───

#[tokio::test]
async fn test_health_endpoint() {
    let server = start_server().await;
    let resp = client()
        .get(format!("{}/health", server.http))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    assert_eq!(resp.text().await.unwrap(), "ok");
}

// ─── Auto-commit queries ───

#[tokio::test]
async fn test_simple_return() {
    let server = start_server().await;
    let body = exec_json(&server.http, "RETURN 1 AS n").await;
    assert_eq!(body["data"]["fields"], json!(["n"]));
    assert_eq!(body["data"]["values"], json!([[1]]));
}

#[tokio::test]
async fn test_create_and_query() {
    let server = start_server().await;
    create_person_table(&server.http).await;

    let body = exec_json(
        &server.http,
        "CREATE (p:Person {name: 'Alice', age: 30}) RETURN p.name, p.age",
    )
    .await;
    assert_eq!(body["data"]["values"][0][0], "Alice");
    assert_eq!(body["data"]["values"][0][1], 30);

    // Query it back.
    let body = exec_json(
        &server.http,
        "MATCH (p:Person {name: 'Alice'}) RETURN p.name, p.age",
    )
    .await;
    assert_eq!(body["data"]["values"][0][0], "Alice");
    assert_eq!(body["data"]["values"][0][1], 30);
}

#[tokio::test]
async fn test_bad_query_returns_error() {
    let server = start_server().await;
    let resp = exec(&server.http, "THIS IS NOT CYPHER").await;
    assert_eq!(resp.status(), 422);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert!(!body["errors"].as_array().unwrap().is_empty());
}

#[tokio::test]
async fn test_multiple_rows() {
    let server = start_server().await;
    create_person_table(&server.http).await;

    exec_ok(&server.http, "CREATE (:Person {name: 'A'}), (:Person {name: 'B'}), (:Person {name: 'C'})").await;
    let body = exec_json(
        &server.http,
        "MATCH (p:Person) RETURN p.name ORDER BY p.name",
    )
    .await;
    let values = body["data"]["values"].as_array().unwrap();
    assert_eq!(values.len(), 3);
    assert_eq!(values[0][0], "A");
    assert_eq!(values[1][0], "B");
    assert_eq!(values[2][0], "C");
}

#[tokio::test]
async fn test_return_node() {
    let server = start_server().await;
    create_person_table(&server.http).await;

    exec_ok(&server.http, "CREATE (:Person {name: 'Alice', age: 30})").await;
    let body = exec_json(&server.http, "MATCH (p:Person) RETURN p").await;
    let node = &body["data"]["values"][0][0];
    assert_eq!(node["label"], "Person");
    assert_eq!(node["properties"]["name"], "Alice");
    assert_eq!(node["properties"]["age"], 30);
}

// ─── Parameterized queries ───

#[tokio::test]
async fn test_parameterized_query() {
    let server = start_server().await;
    create_person_table(&server.http).await;
    exec_ok(&server.http, "CREATE (:Person {name: 'Alice', age: 30})").await;

    let body = exec_params_json(
        &server.http,
        "MATCH (p:Person {name: $name}) RETURN p.age",
        json!({ "name": "Alice" }),
    )
    .await;
    assert_eq!(body["data"]["values"][0][0], 30);
}

#[tokio::test]
async fn test_parameterized_insert() {
    let server = start_server().await;
    create_person_table(&server.http).await;

    let body = exec_params_json(
        &server.http,
        "CREATE (p:Person {name: $name, age: $age}) RETURN p.name, p.age",
        json!({ "name": "Bob", "age": 25 }),
    )
    .await;
    assert_eq!(body["data"]["values"][0][0], "Bob");
    assert_eq!(body["data"]["values"][0][1], 25);
}

#[tokio::test]
async fn test_param_types() {
    let server = start_server().await;
    let body = exec_params_json(
        &server.http,
        "RETURN $s AS s, $i AS i, $f AS f, $b AS b",
        json!({ "s": "hello", "i": 42, "f": 3.14, "b": true }),
    )
    .await;
    let row = &body["data"]["values"][0];
    assert_eq!(row[0], "hello");
    assert_eq!(row[1], 42);
    assert!((row[2].as_f64().unwrap() - 3.14).abs() < 0.001);
    assert_eq!(row[3], true);
}

#[tokio::test]
async fn test_array_params() {
    let server = start_server().await;
    let body = exec_params_json(
        &server.http,
        "RETURN $arr AS arr",
        json!({ "arr": [1, 2, 3] }),
    )
    .await;
    assert_eq!(body["data"]["values"][0][0], json!([1, 2, 3]));
}

// ─── Auth ───

#[tokio::test]
async fn test_auth_required() {
    let server = start_server_with_token("secret-token").await;
    let resp = exec(&server.http, "RETURN 1").await;
    assert_eq!(resp.status(), 401);
}

#[tokio::test]
async fn test_auth_bearer_success() {
    let server = start_server_with_token("secret-token").await;
    let resp = exec_with_auth(&server.http, "RETURN 1 AS n", "secret-token").await;
    assert_eq!(resp.status(), 202);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["data"]["values"][0][0], 1);
}

#[tokio::test]
async fn test_auth_bearer_wrong_token() {
    let server = start_server_with_token("correct").await;
    let resp = exec_with_auth(&server.http, "RETURN 1", "wrong").await;
    assert_eq!(resp.status(), 401);
}

#[tokio::test]
async fn test_auth_basic() {
    let server = start_server_with_token("my-token").await;
    let encoded = base64::Engine::encode(
        &base64::engine::general_purpose::STANDARD,
        "neo4j:my-token",
    );
    let resp = client()
        .post(format!("{}/db/graph/query/v2", server.http))
        .header("authorization", format!("Basic {encoded}"))
        .json(&json!({ "statement": "RETURN 1 AS n" }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 202);
}

#[tokio::test]
async fn test_auth_open_access() {
    let server = start_server().await;
    let body = exec_json(&server.http, "RETURN 1 AS n").await;
    assert_eq!(body["data"]["values"][0][0], 1);
}

// ─── Transactions ───

#[tokio::test]
async fn test_begin_commit() {
    let server = start_server().await;
    exec_ok(
        &server.http,
        "CREATE NODE TABLE TxTest(val STRING, PRIMARY KEY(val))",
    )
    .await;

    // Begin transaction.
    let resp = client()
        .post(format!("{}/db/graph/query/v2/tx", server.http))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 202);
    let body: serde_json::Value = resp.json().await.unwrap();
    let tx_id = body["id"].as_str().unwrap();

    // Run query in transaction.
    let resp = client()
        .post(format!("{}/db/graph/query/v2/tx/{tx_id}", server.http))
        .json(&json!({ "statement": "CREATE (:TxTest {val: 'committed'})" }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 202);

    // Commit.
    let resp = client()
        .post(format!("{}/db/graph/query/v2/tx/{tx_id}/commit", server.http))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 202);

    // Verify data persisted.
    let body = exec_json(&server.http, "MATCH (t:TxTest) RETURN t.val").await;
    assert_eq!(body["data"]["values"][0][0], "committed");
}

#[tokio::test]
async fn test_begin_rollback() {
    let server = start_server().await;
    exec_ok(
        &server.http,
        "CREATE NODE TABLE TxTest(val STRING, PRIMARY KEY(val))",
    )
    .await;

    // Begin.
    let resp = client()
        .post(format!("{}/db/graph/query/v2/tx", server.http))
        .send()
        .await
        .unwrap();
    let body: serde_json::Value = resp.json().await.unwrap();
    let tx_id = body["id"].as_str().unwrap();

    // Create in tx.
    client()
        .post(format!("{}/db/graph/query/v2/tx/{tx_id}", server.http))
        .json(&json!({ "statement": "CREATE (:TxTest {val: 'rolled_back'})" }))
        .send()
        .await
        .unwrap();

    // Rollback.
    let resp = client()
        .delete(format!("{}/db/graph/query/v2/tx/{tx_id}", server.http))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 202);

    // Verify data NOT persisted.
    let body = exec_json(&server.http, "MATCH (t:TxTest) RETURN count(t) AS cnt").await;
    assert_eq!(body["data"]["values"][0][0], 0);
}

#[tokio::test]
async fn test_nonexistent_transaction() {
    let server = start_server().await;
    let resp = client()
        .post(format!("{}/db/graph/query/v2/tx/nonexistent", server.http))
        .json(&json!({ "statement": "RETURN 1" }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 422);
}

#[tokio::test]
async fn test_commit_nonexistent_transaction() {
    let server = start_server().await;
    let resp = client()
        .post(format!(
            "{}/db/graph/query/v2/tx/nonexistent/commit",
            server.http
        ))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 422);
}

#[tokio::test]
async fn test_transaction_multiple_queries() {
    let server = start_server().await;
    exec_ok(
        &server.http,
        "CREATE NODE TABLE Multi(val INT64, PRIMARY KEY(val))",
    )
    .await;

    // Begin.
    let resp = client()
        .post(format!("{}/db/graph/query/v2/tx", server.http))
        .send()
        .await
        .unwrap();
    let body: serde_json::Value = resp.json().await.unwrap();
    let tx_id = body["id"].as_str().unwrap();

    // Run multiple queries.
    client()
        .post(format!("{}/db/graph/query/v2/tx/{tx_id}", server.http))
        .json(&json!({ "statement": "CREATE (:Multi {val: 1})" }))
        .send()
        .await
        .unwrap();
    client()
        .post(format!("{}/db/graph/query/v2/tx/{tx_id}", server.http))
        .json(&json!({ "statement": "CREATE (:Multi {val: 2})" }))
        .send()
        .await
        .unwrap();

    // Query within transaction.
    let resp = client()
        .post(format!("{}/db/graph/query/v2/tx/{tx_id}", server.http))
        .json(&json!({ "statement": "MATCH (m:Multi) RETURN count(m) AS cnt" }))
        .send()
        .await
        .unwrap();
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["data"]["values"][0][0], 2);

    // Commit.
    client()
        .post(format!("{}/db/graph/query/v2/tx/{tx_id}/commit", server.http))
        .send()
        .await
        .unwrap();
}

// ─── Rewriter ───

#[tokio::test]
async fn test_rewriter_uuid() {
    let server = start_server().await;
    create_person_table(&server.http).await;

    let body = exec_json(
        &server.http,
        "CREATE (p:Person {name: 'uuid_test', id: gen_random_uuid()}) RETURN p.id",
    )
    .await;
    let uuid_str = body["data"]["values"][0][0].as_str().unwrap();
    assert_eq!(uuid_str.len(), 36);
    assert_eq!(&uuid_str[8..9], "-");
}

#[tokio::test]
async fn test_rewriter_multiple_uuids_distinct() {
    let server = start_server().await;
    let body = exec_json(
        &server.http,
        "RETURN gen_random_uuid() AS a, gen_random_uuid() AS b",
    )
    .await;
    let a = body["data"]["values"][0][0].as_str().unwrap();
    let b = body["data"]["values"][0][1].as_str().unwrap();
    assert_ne!(a, b);
}

#[tokio::test]
async fn test_rewriter_timestamp() {
    let server = start_server().await;
    let body = exec_json(
        &server.http,
        "RETURN current_timestamp() AS ts",
    )
    .await;
    let val = &body["data"]["values"][0][0];
    // The rewriter replaces current_timestamp() with an ISO 8601 string param.
    // kuzu returns it as a string, so check it parses as a valid timestamp.
    let ts_str = val.as_str().expect("timestamp should be a string");
    // Format: "YYYY-MM-DDThh:mm:ss.uuuuuu"
    assert!(ts_str.len() >= 19, "timestamp too short: {ts_str}");
    let year: u32 = ts_str[..4].parse().expect("bad year");
    assert!(year >= 2024, "timestamp year too old: {ts_str}");
}

#[tokio::test]
async fn test_rewriter_preserves_string_literals() {
    let server = start_server().await;
    let body = exec_json(
        &server.http,
        "RETURN 'gen_random_uuid()' AS literal",
    )
    .await;
    assert_eq!(body["data"]["values"][0][0], "gen_random_uuid()");
}

#[tokio::test]
async fn test_rewriter_user_and_generated_params() {
    let server = start_server().await;
    create_person_table(&server.http).await;

    let body = exec_params_json(
        &server.http,
        "CREATE (p:Person {name: $name, id: gen_random_uuid()}) RETURN p.name, p.id",
        json!({ "name": "Eve" }),
    )
    .await;
    assert_eq!(body["data"]["values"][0][0], "Eve");
    let uuid = body["data"]["values"][0][1].as_str().unwrap();
    assert_eq!(uuid.len(), 36);
}

// ─── Journal ───

#[tokio::test]
async fn test_journal_records_mutations() {
    let server = start_server_with_journal().await;
    exec_ok(
        &server.http,
        "CREATE NODE TABLE JTest(val STRING, PRIMARY KEY(val))",
    )
    .await;

    exec_ok(&server.http, "CREATE (:JTest {val: 'hello'})").await;
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    let journal_dir = server._data_dir.path().join("journal");
    let reader = graphd::journal::JournalReader::open(&journal_dir).unwrap();
    let entries: Vec<_> = reader.collect();
    // Should have at least 2 entries: the CREATE NODE TABLE + the CREATE.
    assert!(
        entries.len() >= 2,
        "Journal should have at least 2 entries, got {}",
        entries.len()
    );
    // Last entry should be the CREATE.
    let last = entries.last().unwrap().as_ref().unwrap();
    assert!(
        last.entry.query.contains("CREATE") && last.entry.query.contains("JTest"),
        "Last entry should be the data CREATE"
    );
}

#[tokio::test]
async fn test_journal_skips_reads() {
    let server = start_server_with_journal().await;

    exec_ok(&server.http, "RETURN 1").await;
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    let journal_dir = server._data_dir.path().join("journal");
    let reader = graphd::journal::JournalReader::open(&journal_dir).unwrap();
    let entries: Vec<_> = reader.collect();
    assert!(entries.is_empty(), "Reads should not be journaled");
}

#[tokio::test]
async fn test_journal_tx_commit_journals_all() {
    let server = start_server_with_journal().await;
    exec_ok(
        &server.http,
        "CREATE NODE TABLE JTx(val INT64, PRIMARY KEY(val))",
    )
    .await;
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    // Read journal to count baseline entries (CREATE NODE TABLE).
    let journal_dir = server._data_dir.path().join("journal");
    let baseline = {
        let reader = graphd::journal::JournalReader::open(&journal_dir).unwrap();
        reader.count()
    };

    // Begin tx.
    let resp = client()
        .post(format!("{}/db/graph/query/v2/tx", server.http))
        .send()
        .await
        .unwrap();
    let body: serde_json::Value = resp.json().await.unwrap();
    let tx_id = body["id"].as_str().unwrap();

    // Two mutations.
    client()
        .post(format!("{}/db/graph/query/v2/tx/{tx_id}", server.http))
        .json(&json!({ "statement": "CREATE (:JTx {val: 1})" }))
        .send()
        .await
        .unwrap();
    client()
        .post(format!("{}/db/graph/query/v2/tx/{tx_id}", server.http))
        .json(&json!({ "statement": "CREATE (:JTx {val: 2})" }))
        .send()
        .await
        .unwrap();

    // Commit.
    client()
        .post(format!("{}/db/graph/query/v2/tx/{tx_id}/commit", server.http))
        .send()
        .await
        .unwrap();

    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    let reader = graphd::journal::JournalReader::open(&journal_dir).unwrap();
    let total = reader.count();
    assert_eq!(
        total - baseline,
        2,
        "Both mutations should be journaled after commit (baseline={baseline}, total={total})"
    );
}

#[tokio::test]
async fn test_journal_tx_rollback_journals_none() {
    let server = start_server_with_journal().await;
    exec_ok(
        &server.http,
        "CREATE NODE TABLE JTx(val STRING, PRIMARY KEY(val))",
    )
    .await;
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    let journal_dir = server._data_dir.path().join("journal");
    let baseline = {
        let reader = graphd::journal::JournalReader::open(&journal_dir).unwrap();
        reader.count()
    };

    // Begin tx.
    let resp = client()
        .post(format!("{}/db/graph/query/v2/tx", server.http))
        .send()
        .await
        .unwrap();
    let body: serde_json::Value = resp.json().await.unwrap();
    let tx_id = body["id"].as_str().unwrap();

    // Mutation in tx.
    client()
        .post(format!("{}/db/graph/query/v2/tx/{tx_id}", server.http))
        .json(&json!({ "statement": "CREATE (:JTx {val: 'rolled_back'})" }))
        .send()
        .await
        .unwrap();

    // Rollback.
    client()
        .delete(format!("{}/db/graph/query/v2/tx/{tx_id}", server.http))
        .send()
        .await
        .unwrap();

    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    let reader = graphd::journal::JournalReader::open(&journal_dir).unwrap();
    let total = reader.count();
    assert_eq!(
        total - baseline,
        0,
        "Rolled-back mutations should not be journaled"
    );
}

#[tokio::test]
async fn test_journal_params_preserved() {
    let server = start_server_with_journal().await;
    exec_ok(
        &server.http,
        "CREATE NODE TABLE JParams(name STRING, age INT64, PRIMARY KEY(name))",
    )
    .await;

    exec_params(
        &server.http,
        "CREATE (:JParams {name: $name, age: $age})",
        json!({ "name": "Alice", "age": 30 }),
    )
    .await;
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    let journal_dir = server._data_dir.path().join("journal");
    let reader = graphd::journal::JournalReader::open(&journal_dir).unwrap();
    let entries: Vec<_> = reader.collect();
    // Find the CREATE (:JParams ...) entry (skip the CREATE NODE TABLE).
    let entry = entries
        .iter()
        .filter_map(|e| e.as_ref().ok())
        .find(|e| e.entry.query.contains("JParams") && e.entry.query.contains("$name"))
        .expect("Should find the parameterized CREATE entry");
    let params_json = graphd::journal::map_entries_to_json(&entry.entry.params);
    let params = params_json.unwrap();
    assert_eq!(params["name"], "Alice");
    assert_eq!(params["age"], 30);
}

#[tokio::test]
async fn test_journal_array_params_roundtrip() {
    let server = start_server_with_journal().await;
    exec_ok(
        &server.http,
        "CREATE NODE TABLE JArr(name STRING, tags STRING[], PRIMARY KEY(name))",
    )
    .await;

    exec_params(
        &server.http,
        "CREATE (:JArr {name: 'tagged', tags: $tags})",
        json!({ "tags": ["a", "b", "c"] }),
    )
    .await;
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    let journal_dir = server._data_dir.path().join("journal");
    let reader = graphd::journal::JournalReader::open(&journal_dir).unwrap();
    let entries: Vec<_> = reader.collect();
    let entry = entries
        .iter()
        .filter_map(|e| e.as_ref().ok())
        .find(|e| e.entry.query.contains("JArr") && e.entry.query.contains("$tags"))
        .expect("Should find the parameterized CREATE entry");
    let params_json = graphd::journal::map_entries_to_json(&entry.entry.params);
    let params = params_json.unwrap();
    assert_eq!(params["tags"], json!(["a", "b", "c"]));
}

// ─── Snapshot ───

#[tokio::test]
async fn test_snapshot_requires_journal() {
    let server = start_server().await;
    let resp = client()
        .post(format!("{}/v1/snapshot", server.http))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 422);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert!(body["errors"][0]["message"]
        .as_str()
        .unwrap()
        .contains("Journal"));
}

#[tokio::test]
async fn test_snapshot_endpoint() {
    let server = start_server_with_journal().await;
    exec_ok(
        &server.http,
        "CREATE NODE TABLE SnapTest(val STRING, PRIMARY KEY(val))",
    )
    .await;
    exec_ok(&server.http, "CREATE (:SnapTest {val: 'hello'})").await;
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    let resp = client()
        .post(format!("{}/v1/snapshot", server.http))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert!(body["data"]["fields"]
        .as_array()
        .unwrap()
        .contains(&json!("sequence")));
}

// ─── Regression tests ───

#[tokio::test]
async fn test_regression_array_param_insert_and_query() {
    let server = start_server().await;
    create_person_table(&server.http).await;

    let body = exec_params_json(
        &server.http,
        "CREATE (p:Person {name: $name, tags: $tags}) RETURN p.tags",
        json!({ "name": "Alice", "tags": ["engineer", "leader"] }),
    )
    .await;
    assert_eq!(body["data"]["values"][0][0], json!(["engineer", "leader"]));

    // Query back.
    let body = exec_json(
        &server.http,
        "MATCH (p:Person {name: 'Alice'}) RETURN p.tags",
    )
    .await;
    assert_eq!(body["data"]["values"][0][0], json!(["engineer", "leader"]));
}

// Note: nested array params like [[1,2],[3,4]] are not supported by kuzu —
// it cannot infer the type of nested arrays passed as parameters.
// Flat array params work fine (tested in test_journal_array_params).

#[tokio::test]
async fn test_regression_array_param_roundtrip() {
    let server = start_server().await;
    let body = exec_params_json(
        &server.http,
        "RETURN $arr AS arr",
        json!({ "arr": [10, 20, 30] }),
    )
    .await;
    assert_eq!(body["data"]["values"][0][0], json!([10, 20, 30]));
}

// ─── DB name path param ignored ───

#[tokio::test]
async fn test_db_name_ignored() {
    let server = start_server().await;
    let resp = client()
        .post(format!("{}/db/some-other-db/query/v2", server.http))
        .json(&json!({ "statement": "RETURN 1 AS n" }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 202);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["data"]["values"][0][0], 1);
}

// ─── Auth on all endpoints ───

#[tokio::test]
async fn test_auth_on_all_endpoints() {
    let server = start_server_with_token("secret").await;

    let resp = exec(&server.http, "RETURN 1").await;
    assert_eq!(resp.status(), 401);

    let resp = client()
        .post(format!("{}/db/graph/query/v2/tx", server.http))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 401);

    let resp = client()
        .post(format!("{}/v1/snapshot", server.http))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 401);

    // Health is always open.
    let resp = client()
        .get(format!("{}/health", server.http))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
}

// ─── Edge cases ───

#[tokio::test]
async fn test_empty_result_set() {
    let server = start_server().await;
    create_person_table(&server.http).await;

    let body = exec_json(
        &server.http,
        "MATCH (x:Person {name: 'nonexistent'}) RETURN x.name",
    )
    .await;
    assert!(body["data"]["values"].as_array().unwrap().is_empty());
}

#[tokio::test]
async fn test_null_values() {
    let server = start_server().await;
    let body = exec_json(&server.http, "RETURN null AS n").await;
    assert!(body["data"]["values"][0][0].is_null());
}

#[tokio::test]
async fn test_float_precision() {
    let server = start_server().await;
    let body = exec_json(&server.http, "RETURN 3.14159 AS pi").await;
    let pi = body["data"]["values"][0][0].as_f64().unwrap();
    assert!((pi - 3.14159).abs() < 1e-5);
}

#[tokio::test]
async fn test_large_integer() {
    let server = start_server().await;
    let body = exec_json(&server.http, "RETURN 9999999999 AS big").await;
    assert_eq!(body["data"]["values"][0][0], 9999999999_i64);
}

#[tokio::test]
async fn test_empty_string() {
    let server = start_server().await;
    let body = exec_json(&server.http, "RETURN '' AS empty").await;
    assert_eq!(body["data"]["values"][0][0], "");
}

#[tokio::test]
async fn test_boolean_values() {
    let server = start_server().await;
    let body = exec_json(&server.http, "RETURN true AS t, false AS f").await;
    assert_eq!(body["data"]["values"][0][0], true);
    assert_eq!(body["data"]["values"][0][1], false);
}

#[tokio::test]
async fn test_list_return() {
    let server = start_server().await;
    let body = exec_json(&server.http, "RETURN [1, 2, 3] AS arr").await;
    assert_eq!(body["data"]["values"][0][0], json!([1, 2, 3]));
}
