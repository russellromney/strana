use futures_util::{SinkExt, StreamExt};
use serde_json::json;
use std::sync::Arc;
use std::time::Duration;
use strana::auth::TokenStore;
use strana::wire::proto;
use tokio::net::TcpListener;
use tokio_tungstenite::tungstenite::Message as WsMsg;

type WsStream = tokio_tungstenite::WebSocketStream<
    tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
>;

// ─── Server helpers ───

struct ServerUrls {
    ws: String,
    http: String,
}

async fn start_server(data_dir: &std::path::Path, tokens: Arc<TokenStore>) -> ServerUrls {
    start_server_with_timeout(data_dir, tokens, Duration::from_secs(30)).await
}

async fn start_server_with_timeout(
    data_dir: &std::path::Path,
    tokens: Arc<TokenStore>,
    cursor_timeout: Duration,
) -> ServerUrls {
    let db_path = data_dir.join("db");
    let db =
        strana::backend::Backend::open(&db_path).expect("Failed to open database");
    let state = strana::server::AppState {
        db: Arc::new(db),
        tokens,
        cursor_timeout,
        journal: None,
        journal_state: None,
        data_dir: db_path,
        snapshot_lock: Arc::new(std::sync::RwLock::new(())),
        retention_config: strana::snapshot::RetentionConfig { daily: 7, weekly: 4, monthly: 3 },
    };
    let app = axum::Router::new()
        .route("/ws", axum::routing::get(strana::server::ws_upgrade))
        .route("/v1/execute", axum::routing::post(strana::http::execute_handler))
        .route("/v1/batch", axum::routing::post(strana::http::batch_handler))
        .route("/v1/pipeline", axum::routing::post(strana::http::pipeline_handler))
        .with_state(state);
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let port = addr.port();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    ServerUrls {
        ws: format!("ws://127.0.0.1:{port}/ws"),
        http: format!("http://127.0.0.1:{port}"),
    }
}

async fn connect(url: &str) -> WsStream {
    let (ws, _) = tokio_tungstenite::connect_async(url).await.unwrap();
    ws
}

// ─── Protobuf WS send/recv ───

async fn send(ws: &mut WsStream, msg: proto::ClientMessage) {
    let bytes = prost::Message::encode_to_vec(&msg);
    ws.send(WsMsg::Binary(bytes.into())).await.unwrap();
}

async fn recv(ws: &mut WsStream) -> proto::ServerMessage {
    loop {
        match ws.next().await.unwrap().unwrap() {
            WsMsg::Binary(bytes) => {
                return <proto::ServerMessage as prost::Message>::decode(bytes.as_ref()).unwrap();
            }
            WsMsg::Ping(_) | WsMsg::Pong(_) => continue,
            other => panic!("Unexpected message: {other:?}"),
        }
    }
}

// ─── Message builders ───

fn hello(token: Option<&str>) -> proto::ClientMessage {
    proto::ClientMessage {
        msg: Some(proto::client_message::Msg::Hello(proto::Hello {
            token: token.map(|s| s.into()),
        })),
    }
}

fn execute(query: &str) -> proto::ClientMessage {
    execute_full(query, vec![], None, None)
}

fn execute_full(
    query: &str,
    params: Vec<proto::MapEntry>,
    request_id: Option<&str>,
    fetch_size: Option<u64>,
) -> proto::ClientMessage {
    proto::ClientMessage {
        msg: Some(proto::client_message::Msg::Execute(proto::Execute {
            query: query.into(),
            params,
            request_id: request_id.map(|s| s.into()),
            fetch_size,
        })),
    }
}

fn begin(mode: Option<&str>, request_id: Option<&str>) -> proto::ClientMessage {
    proto::ClientMessage {
        msg: Some(proto::client_message::Msg::Begin(proto::Begin {
            mode: mode.map(|s| s.into()),
            request_id: request_id.map(|s| s.into()),
        })),
    }
}

fn commit(request_id: Option<&str>) -> proto::ClientMessage {
    proto::ClientMessage {
        msg: Some(proto::client_message::Msg::Commit(proto::Commit {
            request_id: request_id.map(|s| s.into()),
        })),
    }
}

fn rollback(request_id: Option<&str>) -> proto::ClientMessage {
    proto::ClientMessage {
        msg: Some(proto::client_message::Msg::Rollback(proto::Rollback {
            request_id: request_id.map(|s| s.into()),
        })),
    }
}

fn batch_msg(stmts: Vec<proto::BatchStatement>, request_id: Option<&str>) -> proto::ClientMessage {
    proto::ClientMessage {
        msg: Some(proto::client_message::Msg::Batch(proto::Batch {
            statements: stmts,
            request_id: request_id.map(|s| s.into()),
        })),
    }
}

fn fetch_msg(stream_id: u64, request_id: Option<&str>) -> proto::ClientMessage {
    proto::ClientMessage {
        msg: Some(proto::client_message::Msg::Fetch(proto::Fetch {
            stream_id,
            request_id: request_id.map(|s| s.into()),
        })),
    }
}

fn close_stream_msg(stream_id: u64, request_id: Option<&str>) -> proto::ClientMessage {
    proto::ClientMessage {
        msg: Some(proto::client_message::Msg::CloseStream(proto::CloseStream {
            stream_id,
            request_id: request_id.map(|s| s.into()),
        })),
    }
}

fn close_msg() -> proto::ClientMessage {
    proto::ClientMessage {
        msg: Some(proto::client_message::Msg::Close(proto::Close {})),
    }
}

fn stmt(query: &str) -> proto::BatchStatement {
    proto::BatchStatement { query: query.into(), params: vec![] }
}

fn stmt_p(query: &str, params: Vec<proto::MapEntry>) -> proto::BatchStatement {
    proto::BatchStatement { query: query.into(), params }
}

// ─── Param builders ───

fn int_param(key: &str, v: i64) -> proto::MapEntry {
    proto::MapEntry {
        key: key.into(),
        value: Some(proto::GraphValue {
            value: Some(proto::graph_value::Value::IntValue(v)),
        }),
    }
}

fn str_param(key: &str, v: &str) -> proto::MapEntry {
    proto::MapEntry {
        key: key.into(),
        value: Some(proto::GraphValue {
            value: Some(proto::graph_value::Value::StringValue(v.into())),
        }),
    }
}

fn float_param(key: &str, v: f64) -> proto::MapEntry {
    proto::MapEntry {
        key: key.into(),
        value: Some(proto::GraphValue {
            value: Some(proto::graph_value::Value::FloatValue(v)),
        }),
    }
}

fn bool_param(key: &str, v: bool) -> proto::MapEntry {
    proto::MapEntry {
        key: key.into(),
        value: Some(proto::GraphValue {
            value: Some(proto::graph_value::Value::BoolValue(v)),
        }),
    }
}

// ─── Assertion helpers ───

fn expect_hello_ok(msg: &proto::ServerMessage) -> &proto::HelloOk {
    match msg.msg.as_ref().unwrap() {
        proto::server_message::Msg::HelloOk(h) => h,
        other => panic!("expected HelloOk, got {other:?}"),
    }
}

fn expect_hello_error(msg: &proto::ServerMessage) -> &proto::HelloError {
    match msg.msg.as_ref().unwrap() {
        proto::server_message::Msg::HelloError(e) => e,
        other => panic!("expected HelloError, got {other:?}"),
    }
}

fn expect_result(msg: &proto::ServerMessage) -> &proto::Result {
    match msg.msg.as_ref().unwrap() {
        proto::server_message::Msg::Result(r) => r,
        other => panic!("expected Result, got {other:?}"),
    }
}

fn expect_error(msg: &proto::ServerMessage) -> &proto::Error {
    match msg.msg.as_ref().unwrap() {
        proto::server_message::Msg::Error(e) => e,
        other => panic!("expected Error, got {other:?}"),
    }
}

fn expect_begin_ok(msg: &proto::ServerMessage) -> &proto::BeginOk {
    match msg.msg.as_ref().unwrap() {
        proto::server_message::Msg::BeginOk(b) => b,
        other => panic!("expected BeginOk, got {other:?}"),
    }
}

fn expect_commit_ok(msg: &proto::ServerMessage) -> &proto::CommitOk {
    match msg.msg.as_ref().unwrap() {
        proto::server_message::Msg::CommitOk(c) => c,
        other => panic!("expected CommitOk, got {other:?}"),
    }
}

fn expect_rollback_ok(msg: &proto::ServerMessage) -> &proto::RollbackOk {
    match msg.msg.as_ref().unwrap() {
        proto::server_message::Msg::RollbackOk(r) => r,
        other => panic!("expected RollbackOk, got {other:?}"),
    }
}

fn expect_batch_result(msg: &proto::ServerMessage) -> &proto::BatchResult {
    match msg.msg.as_ref().unwrap() {
        proto::server_message::Msg::BatchResult(b) => b,
        other => panic!("expected BatchResult, got {other:?}"),
    }
}

fn expect_close_ok(msg: &proto::ServerMessage) {
    match msg.msg.as_ref().unwrap() {
        proto::server_message::Msg::CloseOk(_) => {}
        other => panic!("expected CloseOk, got {other:?}"),
    }
}

fn expect_close_stream_ok(msg: &proto::ServerMessage) -> &proto::CloseStreamOk {
    match msg.msg.as_ref().unwrap() {
        proto::server_message::Msg::CloseStreamOk(cs) => cs,
        other => panic!("expected CloseStreamOk, got {other:?}"),
    }
}

fn br_expect_result(entry: &proto::BatchResultEntry) -> &proto::Result {
    match entry.entry.as_ref().unwrap() {
        proto::batch_result_entry::Entry::Result(r) => r,
        other => panic!("expected batch Result, got {other:?}"),
    }
}

fn br_expect_error(entry: &proto::BatchResultEntry) -> &proto::Error {
    match entry.entry.as_ref().unwrap() {
        proto::batch_result_entry::Entry::Error(e) => e,
        other => panic!("expected batch Error, got {other:?}"),
    }
}

// ─── Value extractors ───

fn val_int(v: &proto::GraphValue) -> i64 {
    match v.value.as_ref().unwrap() {
        proto::graph_value::Value::IntValue(i) => *i,
        other => panic!("expected IntValue, got {other:?}"),
    }
}

fn val_str(v: &proto::GraphValue) -> &str {
    match v.value.as_ref().unwrap() {
        proto::graph_value::Value::StringValue(s) => s,
        other => panic!("expected StringValue, got {other:?}"),
    }
}

fn val_float(v: &proto::GraphValue) -> f64 {
    match v.value.as_ref().unwrap() {
        proto::graph_value::Value::FloatValue(f) => *f,
        other => panic!("expected FloatValue, got {other:?}"),
    }
}

fn val_bool(v: &proto::GraphValue) -> bool {
    match v.value.as_ref().unwrap() {
        proto::graph_value::Value::BoolValue(b) => *b,
        other => panic!("expected BoolValue, got {other:?}"),
    }
}

fn val_node(v: &proto::GraphValue) -> &proto::NodeValue {
    match v.value.as_ref().unwrap() {
        proto::graph_value::Value::NodeValue(n) => n,
        other => panic!("expected NodeValue, got {other:?}"),
    }
}

fn node_prop_str<'a>(node: &'a proto::NodeValue, key: &str) -> &'a str {
    let entry = node.properties.iter().find(|e| e.key == key)
        .unwrap_or_else(|| panic!("property '{key}' not found"));
    val_str(entry.value.as_ref().unwrap())
}

// ─── Streaming helper ───

async fn setup_streaming_data(url: &str, table: &str, count: u64) -> WsStream {
    let mut ws = connect(url).await;
    send(&mut ws, hello(None)).await;
    let _ = recv(&mut ws).await;

    send(&mut ws, execute(&format!(
        "CREATE NODE TABLE {table}(id INT64, PRIMARY KEY(id))"
    ))).await;
    let _ = recv(&mut ws).await;

    let stmts: Vec<proto::BatchStatement> = (1..=count)
        .map(|i| stmt(&format!("CREATE (:{table} {{id: {i}}})")))
        .collect();
    send(&mut ws, batch_msg(stmts, None)).await;
    let _ = recv(&mut ws).await;

    ws
}

// ═══════════════════════════════════════════════════════════════
// Phase 1-2: Auth, Execute, Params, Request ID
// ═══════════════════════════════════════════════════════════════

#[tokio::test]
async fn test_hello_and_close() {
    let dir = tempfile::tempdir().unwrap();
    let urls = start_server(dir.path(), Arc::new(TokenStore::open())).await;
    let mut ws = connect(&urls.ws).await;

    send(&mut ws, hello(None)).await;
    let resp = recv(&mut ws).await;
    let h = expect_hello_ok(&resp);
    assert!(!h.version.is_empty());

    send(&mut ws, close_msg()).await;
    let resp = recv(&mut ws).await;
    expect_close_ok(&resp);
}

#[tokio::test]
async fn test_auth_required() {
    let dir = tempfile::tempdir().unwrap();
    let urls = start_server(dir.path(), Arc::new(TokenStore::from_token("secret123"))).await;
    let mut ws = connect(&urls.ws).await;

    send(&mut ws, hello(Some("wrong"))).await;
    let resp = recv(&mut ws).await;
    let e = expect_hello_error(&resp);
    assert_eq!(e.message, "Invalid token");
}

#[tokio::test]
async fn test_auth_success() {
    let dir = tempfile::tempdir().unwrap();
    let urls = start_server(dir.path(), Arc::new(TokenStore::from_token("secret123"))).await;
    let mut ws = connect(&urls.ws).await;

    send(&mut ws, hello(Some("secret123"))).await;
    let resp = recv(&mut ws).await;
    expect_hello_ok(&resp);
}

#[tokio::test]
async fn test_execute_create_and_query() {
    let dir = tempfile::tempdir().unwrap();
    let urls = start_server(dir.path(), Arc::new(TokenStore::open())).await;
    let mut ws = connect(&urls.ws).await;

    send(&mut ws, hello(None)).await;
    let _ = recv(&mut ws).await;

    send(&mut ws, execute("CREATE NODE TABLE Person(name STRING, age INT64, PRIMARY KEY(name))")).await;
    let resp = recv(&mut ws).await;
    expect_result(&resp);

    send(&mut ws, execute("CREATE (:Person {name: 'Alice', age: 30})")).await;
    let resp = recv(&mut ws).await;
    expect_result(&resp);

    send(&mut ws, execute("CREATE (:Person {name: 'Bob', age: 42})")).await;
    let resp = recv(&mut ws).await;
    expect_result(&resp);

    send(&mut ws, execute("MATCH (p:Person) RETURN p.name, p.age ORDER BY p.name")).await;
    let resp = recv(&mut ws).await;
    let r = expect_result(&resp);
    assert_eq!(r.columns, vec!["p.name", "p.age"]);
    assert_eq!(r.rows.len(), 2);
    assert_eq!(val_str(&r.rows[0].values[0]), "Alice");
    assert_eq!(val_int(&r.rows[0].values[1]), 30);
    assert_eq!(val_str(&r.rows[1].values[0]), "Bob");
    assert_eq!(val_int(&r.rows[1].values[1]), 42);
    assert!(r.timing_ms >= 0.0);
}

#[tokio::test]
async fn test_execute_bad_query() {
    let dir = tempfile::tempdir().unwrap();
    let urls = start_server(dir.path(), Arc::new(TokenStore::open())).await;
    let mut ws = connect(&urls.ws).await;

    send(&mut ws, hello(None)).await;
    let _ = recv(&mut ws).await;

    send(&mut ws, execute("THIS IS NOT CYPHER")).await;
    let resp = recv(&mut ws).await;
    let e = expect_error(&resp);
    assert!(!e.message.is_empty());
}

#[tokio::test]
async fn test_first_message_must_be_hello() {
    let dir = tempfile::tempdir().unwrap();
    let urls = start_server(dir.path(), Arc::new(TokenStore::open())).await;
    let mut ws = connect(&urls.ws).await;

    send(&mut ws, execute("MATCH (n) RETURN n")).await;
    let resp = recv(&mut ws).await;
    let e = expect_hello_error(&resp);
    assert_eq!(e.message, "First message must be hello");
}

#[tokio::test]
async fn test_return_node() {
    let dir = tempfile::tempdir().unwrap();
    let urls = start_server(dir.path(), Arc::new(TokenStore::open())).await;
    let mut ws = connect(&urls.ws).await;

    send(&mut ws, hello(None)).await;
    let _ = recv(&mut ws).await;

    send(&mut ws, execute("CREATE NODE TABLE City(name STRING, PRIMARY KEY(name))")).await;
    let _ = recv(&mut ws).await;

    send(&mut ws, execute("CREATE (:City {name: 'Berlin'})")).await;
    let _ = recv(&mut ws).await;

    send(&mut ws, execute("MATCH (c:City) RETURN c")).await;
    let resp = recv(&mut ws).await;
    let r = expect_result(&resp);

    let node = val_node(&r.rows[0].values[0]);
    assert_eq!(node.label, "City");
    assert_eq!(node_prop_str(node, "name"), "Berlin");
}

#[tokio::test]
async fn test_multi_token_auth() {
    let dir = tempfile::tempdir().unwrap();
    let store = TokenStore::open();
    store.add_token("token_alpha", "app-alpha");
    store.add_token("token_beta", "app-beta");
    let urls = start_server(dir.path(), Arc::new(store)).await;

    let mut ws = connect(&urls.ws).await;
    send(&mut ws, hello(Some("token_alpha"))).await;
    let resp = recv(&mut ws).await;
    expect_hello_ok(&resp);

    let mut ws2 = connect(&urls.ws).await;
    send(&mut ws2, hello(Some("token_beta"))).await;
    let resp2 = recv(&mut ws2).await;
    expect_hello_ok(&resp2);

    let mut ws3 = connect(&urls.ws).await;
    send(&mut ws3, hello(Some("token_gamma"))).await;
    let resp3 = recv(&mut ws3).await;
    expect_hello_error(&resp3);
}

#[tokio::test]
async fn test_open_store_accepts_anything() {
    let dir = tempfile::tempdir().unwrap();
    let urls = start_server(dir.path(), Arc::new(TokenStore::open())).await;

    let mut ws = connect(&urls.ws).await;
    send(&mut ws, hello(None)).await;
    let resp = recv(&mut ws).await;
    expect_hello_ok(&resp);

    let mut ws2 = connect(&urls.ws).await;
    send(&mut ws2, hello(Some("literally-anything"))).await;
    let resp2 = recv(&mut ws2).await;
    expect_hello_ok(&resp2);
}

#[tokio::test]
async fn test_auth_no_token_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let urls = start_server(dir.path(), Arc::new(TokenStore::from_token("secret"))).await;

    let mut ws = connect(&urls.ws).await;
    send(&mut ws, hello(None)).await;
    let resp = recv(&mut ws).await;
    let e = expect_hello_error(&resp);
    assert_eq!(e.message, "Invalid token");
}

#[tokio::test]
async fn test_parameterized_query() {
    let dir = tempfile::tempdir().unwrap();
    let urls = start_server(dir.path(), Arc::new(TokenStore::open())).await;
    let mut ws = connect(&urls.ws).await;

    send(&mut ws, hello(None)).await;
    let _ = recv(&mut ws).await;

    send(&mut ws, execute("CREATE NODE TABLE Person(name STRING, age INT64, PRIMARY KEY(name))")).await;
    let _ = recv(&mut ws).await;

    send(&mut ws, execute("CREATE (:Person {name: 'Alice', age: 30})")).await;
    let _ = recv(&mut ws).await;

    send(&mut ws, execute("CREATE (:Person {name: 'Bob', age: 20})")).await;
    let _ = recv(&mut ws).await;

    send(&mut ws, execute_full(
        "MATCH (p:Person) WHERE p.age > $min_age RETURN p.name ORDER BY p.name",
        vec![int_param("min_age", 25)],
        None, None,
    )).await;
    let resp = recv(&mut ws).await;
    let r = expect_result(&resp);
    assert_eq!(r.columns, vec!["p.name"]);
    assert_eq!(r.rows.len(), 1);
    assert_eq!(val_str(&r.rows[0].values[0]), "Alice");
}

#[tokio::test]
async fn test_parameterized_insert() {
    let dir = tempfile::tempdir().unwrap();
    let urls = start_server(dir.path(), Arc::new(TokenStore::open())).await;
    let mut ws = connect(&urls.ws).await;

    send(&mut ws, hello(None)).await;
    let _ = recv(&mut ws).await;

    send(&mut ws, execute("CREATE NODE TABLE Animal(name STRING, legs INT64, PRIMARY KEY(name))")).await;
    let _ = recv(&mut ws).await;

    send(&mut ws, execute_full(
        "CREATE (:Animal {name: $name, legs: $legs})",
        vec![str_param("name", "Cat"), int_param("legs", 4)],
        None, None,
    )).await;
    let resp = recv(&mut ws).await;
    expect_result(&resp);

    send(&mut ws, execute("MATCH (a:Animal) RETURN a.name, a.legs")).await;
    let resp = recv(&mut ws).await;
    let r = expect_result(&resp);
    assert_eq!(val_str(&r.rows[0].values[0]), "Cat");
    assert_eq!(val_int(&r.rows[0].values[1]), 4);
}

#[tokio::test]
async fn test_param_types() {
    let dir = tempfile::tempdir().unwrap();
    let urls = start_server(dir.path(), Arc::new(TokenStore::open())).await;
    let mut ws = connect(&urls.ws).await;

    send(&mut ws, hello(None)).await;
    let _ = recv(&mut ws).await;

    send(&mut ws, execute("CREATE NODE TABLE Config(key STRING, str_val STRING, int_val INT64, float_val DOUBLE, bool_val BOOLEAN, PRIMARY KEY(key))")).await;
    let _ = recv(&mut ws).await;

    send(&mut ws, execute_full(
        "CREATE (:Config {key: $key, str_val: $s, int_val: $i, float_val: $f, bool_val: $b})",
        vec![
            str_param("key", "test"),
            str_param("s", "hello"),
            int_param("i", 42),
            float_param("f", 3.14),
            bool_param("b", true),
        ],
        None, None,
    )).await;
    let resp = recv(&mut ws).await;
    expect_result(&resp);

    send(&mut ws, execute("MATCH (c:Config) RETURN c.str_val, c.int_val, c.float_val, c.bool_val")).await;
    let resp = recv(&mut ws).await;
    let r = expect_result(&resp);
    assert_eq!(val_str(&r.rows[0].values[0]), "hello");
    assert_eq!(val_int(&r.rows[0].values[1]), 42);
    assert!((val_float(&r.rows[0].values[2]) - 3.14).abs() < 0.001);
    assert_eq!(val_bool(&r.rows[0].values[3]), true);
}

#[tokio::test]
async fn test_request_id_echo() {
    let dir = tempfile::tempdir().unwrap();
    let urls = start_server(dir.path(), Arc::new(TokenStore::open())).await;
    let mut ws = connect(&urls.ws).await;

    send(&mut ws, hello(None)).await;
    let _ = recv(&mut ws).await;

    send(&mut ws, execute_full("RETURN 1", vec![], Some("req-abc-123"), None)).await;
    let resp = recv(&mut ws).await;
    let r = expect_result(&resp);
    assert_eq!(r.request_id.as_deref(), Some("req-abc-123"));
}

#[tokio::test]
async fn test_request_id_absent() {
    let dir = tempfile::tempdir().unwrap();
    let urls = start_server(dir.path(), Arc::new(TokenStore::open())).await;
    let mut ws = connect(&urls.ws).await;

    send(&mut ws, hello(None)).await;
    let _ = recv(&mut ws).await;

    send(&mut ws, execute("RETURN 1")).await;
    let resp = recv(&mut ws).await;
    let r = expect_result(&resp);
    assert!(r.request_id.is_none());
}

#[tokio::test]
async fn test_request_id_on_error() {
    let dir = tempfile::tempdir().unwrap();
    let urls = start_server(dir.path(), Arc::new(TokenStore::open())).await;
    let mut ws = connect(&urls.ws).await;

    send(&mut ws, hello(None)).await;
    let _ = recv(&mut ws).await;

    send(&mut ws, execute_full("THIS IS NOT CYPHER", vec![], Some("req-err-456"), None)).await;
    let resp = recv(&mut ws).await;
    let e = expect_error(&resp);
    assert_eq!(e.request_id.as_deref(), Some("req-err-456"));
}

// ═══════════════════════════════════════════════════════════════
// Phase 3: Transactions + Batch
// ═══════════════════════════════════════════════════════════════

#[tokio::test]
async fn test_begin_commit() {
    let dir = tempfile::tempdir().unwrap();
    let urls = start_server(dir.path(), Arc::new(TokenStore::open())).await;
    let mut ws = connect(&urls.ws).await;

    send(&mut ws, hello(None)).await;
    let _ = recv(&mut ws).await;

    send(&mut ws, execute("CREATE NODE TABLE TxTest(id INT64, PRIMARY KEY(id))")).await;
    let _ = recv(&mut ws).await;

    send(&mut ws, begin(None, None)).await;
    let resp = recv(&mut ws).await;
    expect_begin_ok(&resp);

    send(&mut ws, execute("CREATE (:TxTest {id: 1})")).await;
    let resp = recv(&mut ws).await;
    expect_result(&resp);

    send(&mut ws, commit(None)).await;
    let resp = recv(&mut ws).await;
    expect_commit_ok(&resp);

    send(&mut ws, execute("MATCH (t:TxTest) RETURN t.id")).await;
    let resp = recv(&mut ws).await;
    let r = expect_result(&resp);
    assert_eq!(r.rows.len(), 1);
    assert_eq!(val_int(&r.rows[0].values[0]), 1);
}

#[tokio::test]
async fn test_begin_rollback() {
    let dir = tempfile::tempdir().unwrap();
    let urls = start_server(dir.path(), Arc::new(TokenStore::open())).await;
    let mut ws = connect(&urls.ws).await;

    send(&mut ws, hello(None)).await;
    let _ = recv(&mut ws).await;

    send(&mut ws, execute("CREATE NODE TABLE RbTest(id INT64, PRIMARY KEY(id))")).await;
    let _ = recv(&mut ws).await;

    send(&mut ws, begin(None, None)).await;
    let resp = recv(&mut ws).await;
    expect_begin_ok(&resp);

    send(&mut ws, execute("CREATE (:RbTest {id: 99})")).await;
    let resp = recv(&mut ws).await;
    expect_result(&resp);

    send(&mut ws, rollback(None)).await;
    let resp = recv(&mut ws).await;
    expect_rollback_ok(&resp);

    send(&mut ws, execute("MATCH (t:RbTest) RETURN t.id")).await;
    let resp = recv(&mut ws).await;
    let r = expect_result(&resp);
    assert_eq!(r.rows.len(), 0);
}

#[tokio::test]
async fn test_auto_commit_default() {
    let dir = tempfile::tempdir().unwrap();
    let urls = start_server(dir.path(), Arc::new(TokenStore::open())).await;
    let mut ws = connect(&urls.ws).await;

    send(&mut ws, hello(None)).await;
    let _ = recv(&mut ws).await;

    send(&mut ws, execute("CREATE NODE TABLE AcTest(id INT64, PRIMARY KEY(id))")).await;
    let _ = recv(&mut ws).await;

    send(&mut ws, execute("CREATE (:AcTest {id: 42})")).await;
    let resp = recv(&mut ws).await;
    expect_result(&resp);

    send(&mut ws, execute("MATCH (t:AcTest) RETURN t.id")).await;
    let resp = recv(&mut ws).await;
    let r = expect_result(&resp);
    assert_eq!(val_int(&r.rows[0].values[0]), 42);
}

#[tokio::test]
async fn test_error_in_transaction() {
    let dir = tempfile::tempdir().unwrap();
    let urls = start_server(dir.path(), Arc::new(TokenStore::open())).await;
    let mut ws = connect(&urls.ws).await;

    send(&mut ws, hello(None)).await;
    let _ = recv(&mut ws).await;

    send(&mut ws, execute("CREATE NODE TABLE ErrTx(id INT64, PRIMARY KEY(id))")).await;
    let _ = recv(&mut ws).await;

    send(&mut ws, begin(None, None)).await;
    let resp = recv(&mut ws).await;
    expect_begin_ok(&resp);

    send(&mut ws, execute("NOT VALID CYPHER")).await;
    let resp = recv(&mut ws).await;
    expect_error(&resp);

    send(&mut ws, rollback(None)).await;
    let resp = recv(&mut ws).await;
    expect_rollback_ok(&resp);
}

#[tokio::test]
async fn test_batch_success() {
    let dir = tempfile::tempdir().unwrap();
    let urls = start_server(dir.path(), Arc::new(TokenStore::open())).await;
    let mut ws = connect(&urls.ws).await;

    send(&mut ws, hello(None)).await;
    let _ = recv(&mut ws).await;

    send(&mut ws, execute("CREATE NODE TABLE BatchTest(id INT64, PRIMARY KEY(id))")).await;
    let _ = recv(&mut ws).await;

    send(&mut ws, batch_msg(vec![
        stmt("CREATE (:BatchTest {id: 1})"),
        stmt("CREATE (:BatchTest {id: 2})"),
        stmt("CREATE (:BatchTest {id: 3})"),
    ], None)).await;
    let resp = recv(&mut ws).await;
    let br = expect_batch_result(&resp);
    assert_eq!(br.results.len(), 3);
    for entry in &br.results {
        br_expect_result(entry);
    }

    send(&mut ws, execute("MATCH (t:BatchTest) RETURN t.id ORDER BY t.id")).await;
    let resp = recv(&mut ws).await;
    let r = expect_result(&resp);
    assert_eq!(r.rows.len(), 3);
    assert_eq!(val_int(&r.rows[0].values[0]), 1);
    assert_eq!(val_int(&r.rows[1].values[0]), 2);
    assert_eq!(val_int(&r.rows[2].values[0]), 3);
}

#[tokio::test]
async fn test_batch_stops_on_error() {
    let dir = tempfile::tempdir().unwrap();
    let urls = start_server(dir.path(), Arc::new(TokenStore::open())).await;
    let mut ws = connect(&urls.ws).await;

    send(&mut ws, hello(None)).await;
    let _ = recv(&mut ws).await;

    send(&mut ws, batch_msg(vec![
        stmt("RETURN 1"),
        stmt("NOT VALID CYPHER"),
        stmt("RETURN 3"),
    ], None)).await;
    let resp = recv(&mut ws).await;
    let br = expect_batch_result(&resp);
    assert_eq!(br.results.len(), 2);
    br_expect_result(&br.results[0]);
    br_expect_error(&br.results[1]);
}

#[tokio::test]
async fn test_batch_empty() {
    let dir = tempfile::tempdir().unwrap();
    let urls = start_server(dir.path(), Arc::new(TokenStore::open())).await;
    let mut ws = connect(&urls.ws).await;

    send(&mut ws, hello(None)).await;
    let _ = recv(&mut ws).await;

    send(&mut ws, batch_msg(vec![], None)).await;
    let resp = recv(&mut ws).await;
    let br = expect_batch_result(&resp);
    assert_eq!(br.results.len(), 0);
}

#[tokio::test]
async fn test_transaction_request_id() {
    let dir = tempfile::tempdir().unwrap();
    let urls = start_server(dir.path(), Arc::new(TokenStore::open())).await;
    let mut ws = connect(&urls.ws).await;

    send(&mut ws, hello(None)).await;
    let _ = recv(&mut ws).await;

    send(&mut ws, begin(None, Some("b1"))).await;
    let resp = recv(&mut ws).await;
    let b = expect_begin_ok(&resp);
    assert_eq!(b.request_id.as_deref(), Some("b1"));

    send(&mut ws, rollback(Some("rb1"))).await;
    let resp = recv(&mut ws).await;
    let r = expect_rollback_ok(&resp);
    assert_eq!(r.request_id.as_deref(), Some("rb1"));

    send(&mut ws, begin(None, Some("b2"))).await;
    let _ = recv(&mut ws).await;

    send(&mut ws, commit(Some("c1"))).await;
    let resp = recv(&mut ws).await;
    let c = expect_commit_ok(&resp);
    assert_eq!(c.request_id.as_deref(), Some("c1"));
}

#[tokio::test]
async fn test_batch_request_id() {
    let dir = tempfile::tempdir().unwrap();
    let urls = start_server(dir.path(), Arc::new(TokenStore::open())).await;
    let mut ws = connect(&urls.ws).await;

    send(&mut ws, hello(None)).await;
    let _ = recv(&mut ws).await;

    send(&mut ws, batch_msg(vec![stmt("RETURN 1")], Some("batch-42"))).await;
    let resp = recv(&mut ws).await;
    let br = expect_batch_result(&resp);
    assert_eq!(br.request_id.as_deref(), Some("batch-42"));
}

#[tokio::test]
async fn test_disconnect_rollback() {
    let dir = tempfile::tempdir().unwrap();
    let urls = start_server(dir.path(), Arc::new(TokenStore::open())).await;

    {
        let mut ws = connect(&urls.ws).await;
        send(&mut ws, hello(None)).await;
        let _ = recv(&mut ws).await;

        send(&mut ws, execute("CREATE NODE TABLE DcTest(id INT64, PRIMARY KEY(id))")).await;
        let _ = recv(&mut ws).await;

        send(&mut ws, begin(None, None)).await;
        let resp = recv(&mut ws).await;
        expect_begin_ok(&resp);

        send(&mut ws, execute("CREATE (:DcTest {id: 777})")).await;
        let resp = recv(&mut ws).await;
        expect_result(&resp);
    }

    tokio::time::sleep(Duration::from_millis(100)).await;

    let mut ws2 = connect(&urls.ws).await;
    send(&mut ws2, hello(None)).await;
    let _ = recv(&mut ws2).await;

    send(&mut ws2, execute("MATCH (t:DcTest) RETURN t.id")).await;
    let resp = recv(&mut ws2).await;
    let r = expect_result(&resp);
    assert_eq!(r.rows.len(), 0);
}

#[tokio::test]
async fn test_commit_without_begin() {
    let dir = tempfile::tempdir().unwrap();
    let urls = start_server(dir.path(), Arc::new(TokenStore::open())).await;
    let mut ws = connect(&urls.ws).await;

    send(&mut ws, hello(None)).await;
    let _ = recv(&mut ws).await;

    send(&mut ws, commit(None)).await;
    let resp = recv(&mut ws).await;
    let e = expect_error(&resp);
    assert!(e.message.contains("No active transaction"));
}

#[tokio::test]
async fn test_rollback_without_begin() {
    let dir = tempfile::tempdir().unwrap();
    let urls = start_server(dir.path(), Arc::new(TokenStore::open())).await;
    let mut ws = connect(&urls.ws).await;

    send(&mut ws, hello(None)).await;
    let _ = recv(&mut ws).await;

    send(&mut ws, rollback(None)).await;
    let resp = recv(&mut ws).await;
    let e = expect_error(&resp);
    assert!(e.message.contains("No active transaction"));
}

#[tokio::test]
async fn test_begin_read_only() {
    let dir = tempfile::tempdir().unwrap();
    let urls = start_server(dir.path(), Arc::new(TokenStore::open())).await;
    let mut ws = connect(&urls.ws).await;

    send(&mut ws, hello(None)).await;
    let _ = recv(&mut ws).await;

    send(&mut ws, execute("CREATE NODE TABLE RoTest(id INT64, PRIMARY KEY(id))")).await;
    let _ = recv(&mut ws).await;

    send(&mut ws, execute("CREATE (:RoTest {id: 1})")).await;
    let _ = recv(&mut ws).await;

    send(&mut ws, begin(Some("read"), None)).await;
    let resp = recv(&mut ws).await;
    expect_begin_ok(&resp);

    send(&mut ws, execute("MATCH (t:RoTest) RETURN t.id")).await;
    let resp = recv(&mut ws).await;
    let r = expect_result(&resp);
    assert_eq!(val_int(&r.rows[0].values[0]), 1);

    send(&mut ws, execute("CREATE (:RoTest {id: 2})")).await;
    let resp = recv(&mut ws).await;
    expect_error(&resp);

    send(&mut ws, rollback(None)).await;
    let resp = recv(&mut ws).await;
    expect_rollback_ok(&resp);
}

#[tokio::test]
async fn test_batch_with_params() {
    let dir = tempfile::tempdir().unwrap();
    let urls = start_server(dir.path(), Arc::new(TokenStore::open())).await;
    let mut ws = connect(&urls.ws).await;

    send(&mut ws, hello(None)).await;
    let _ = recv(&mut ws).await;

    send(&mut ws, execute("CREATE NODE TABLE BpTest(name STRING, age INT64, PRIMARY KEY(name))")).await;
    let _ = recv(&mut ws).await;

    send(&mut ws, batch_msg(vec![
        stmt_p("CREATE (:BpTest {name: $name, age: $age})", vec![str_param("name", "Alice"), int_param("age", 30)]),
        stmt_p("CREATE (:BpTest {name: $name, age: $age})", vec![str_param("name", "Bob"), int_param("age", 25)]),
    ], None)).await;
    let resp = recv(&mut ws).await;
    let br = expect_batch_result(&resp);
    assert_eq!(br.results.len(), 2);
    for entry in &br.results {
        br_expect_result(entry);
    }

    send(&mut ws, execute("MATCH (t:BpTest) RETURN t.name ORDER BY t.name")).await;
    let resp = recv(&mut ws).await;
    let r = expect_result(&resp);
    assert_eq!(val_str(&r.rows[0].values[0]), "Alice");
    assert_eq!(val_str(&r.rows[1].values[0]), "Bob");
}

#[tokio::test]
async fn test_double_begin() {
    let dir = tempfile::tempdir().unwrap();
    let urls = start_server(dir.path(), Arc::new(TokenStore::open())).await;
    let mut ws = connect(&urls.ws).await;

    send(&mut ws, hello(None)).await;
    let _ = recv(&mut ws).await;

    send(&mut ws, begin(None, None)).await;
    let resp = recv(&mut ws).await;
    expect_begin_ok(&resp);

    send(&mut ws, begin(None, None)).await;
    let resp = recv(&mut ws).await;
    let e = expect_error(&resp);
    assert!(e.message.contains("Transaction already active"));

    send(&mut ws, rollback(None)).await;
    let resp = recv(&mut ws).await;
    expect_rollback_ok(&resp);
}

#[tokio::test]
async fn test_invalid_begin_mode() {
    let dir = tempfile::tempdir().unwrap();
    let urls = start_server(dir.path(), Arc::new(TokenStore::open())).await;
    let mut ws = connect(&urls.ws).await;

    send(&mut ws, hello(None)).await;
    let _ = recv(&mut ws).await;

    send(&mut ws, begin(Some("banana"), None)).await;
    let resp = recv(&mut ws).await;
    let e = expect_error(&resp);
    assert!(e.message.contains("Invalid transaction mode"));

    send(&mut ws, begin(None, None)).await;
    let resp = recv(&mut ws).await;
    expect_begin_ok(&resp);

    send(&mut ws, rollback(None)).await;
    let _ = recv(&mut ws).await;
}

// ═══════════════════════════════════════════════════════════════
// Phase 4: Streaming
// ═══════════════════════════════════════════════════════════════

#[tokio::test]
async fn test_streaming_basic() {
    let dir = tempfile::tempdir().unwrap();
    let urls = start_server(dir.path(), Arc::new(TokenStore::open())).await;
    let mut ws = setup_streaming_data(&urls.ws, "StreamA", 5).await;

    send(&mut ws, execute_full(
        "MATCH (n:StreamA) RETURN n.id ORDER BY n.id", vec![], None, Some(2),
    )).await;
    let resp = recv(&mut ws).await;
    let r = expect_result(&resp);
    assert_eq!(r.rows.len(), 2);
    assert_eq!(r.has_more, Some(true));
    let stream_id = r.stream_id.unwrap();
    let mut all_ids: Vec<i64> = r.rows.iter().map(|row| val_int(&row.values[0])).collect();

    send(&mut ws, fetch_msg(stream_id, None)).await;
    let resp = recv(&mut ws).await;
    let r = expect_result(&resp);
    assert_eq!(r.rows.len(), 2);
    assert_eq!(r.has_more, Some(true));
    all_ids.extend(r.rows.iter().map(|row| val_int(&row.values[0])));

    send(&mut ws, fetch_msg(stream_id, None)).await;
    let resp = recv(&mut ws).await;
    let r = expect_result(&resp);
    assert_eq!(r.rows.len(), 1);
    assert!(r.has_more.is_none());
    assert!(r.stream_id.is_none());
    all_ids.extend(r.rows.iter().map(|row| val_int(&row.values[0])));

    assert_eq!(all_ids, vec![1, 2, 3, 4, 5]);
}

#[tokio::test]
async fn test_streaming_all_fit() {
    let dir = tempfile::tempdir().unwrap();
    let urls = start_server(dir.path(), Arc::new(TokenStore::open())).await;
    let mut ws = setup_streaming_data(&urls.ws, "StreamB", 3).await;

    send(&mut ws, execute_full(
        "MATCH (n:StreamB) RETURN n.id ORDER BY n.id", vec![], None, Some(10),
    )).await;
    let resp = recv(&mut ws).await;
    let r = expect_result(&resp);
    assert_eq!(r.rows.len(), 3);
    assert!(r.stream_id.is_none());
    assert!(r.has_more.is_none());
}

#[tokio::test]
async fn test_streaming_close_stream() {
    let dir = tempfile::tempdir().unwrap();
    let urls = start_server(dir.path(), Arc::new(TokenStore::open())).await;
    let mut ws = setup_streaming_data(&urls.ws, "StreamC", 5).await;

    send(&mut ws, execute_full(
        "MATCH (n:StreamC) RETURN n.id ORDER BY n.id", vec![], None, Some(2),
    )).await;
    let resp = recv(&mut ws).await;
    let r = expect_result(&resp);
    let stream_id = r.stream_id.unwrap();

    send(&mut ws, close_stream_msg(stream_id, None)).await;
    let resp = recv(&mut ws).await;
    let cs = expect_close_stream_ok(&resp);
    assert_eq!(cs.stream_id, stream_id);

    send(&mut ws, fetch_msg(stream_id, None)).await;
    let resp = recv(&mut ws).await;
    let e = expect_error(&resp);
    assert!(e.message.contains("Unknown stream_id"));
}

#[tokio::test]
async fn test_streaming_multiple_cursors() {
    let dir = tempfile::tempdir().unwrap();
    let urls = start_server(dir.path(), Arc::new(TokenStore::open())).await;
    let mut ws = setup_streaming_data(&urls.ws, "StreamD", 4).await;

    send(&mut ws, execute("CREATE NODE TABLE StreamD2(val STRING, PRIMARY KEY(val))")).await;
    let _ = recv(&mut ws).await;
    send(&mut ws, batch_msg(vec![
        stmt("CREATE (:StreamD2 {val: 'a'})"),
        stmt("CREATE (:StreamD2 {val: 'b'})"),
        stmt("CREATE (:StreamD2 {val: 'c'})"),
    ], None)).await;
    let _ = recv(&mut ws).await;

    send(&mut ws, execute_full(
        "MATCH (n:StreamD) RETURN n.id ORDER BY n.id", vec![], None, Some(2),
    )).await;
    let resp1 = recv(&mut ws).await;
    let r1 = expect_result(&resp1);
    let sid1 = r1.stream_id.unwrap();

    send(&mut ws, execute_full(
        "MATCH (n:StreamD2) RETURN n.val ORDER BY n.val", vec![], None, Some(1),
    )).await;
    let resp2 = recv(&mut ws).await;
    let r2 = expect_result(&resp2);
    let sid2 = r2.stream_id.unwrap();
    assert_ne!(sid1, sid2);

    send(&mut ws, fetch_msg(sid2, None)).await;
    let resp = recv(&mut ws).await;
    let r = expect_result(&resp);
    assert_eq!(val_str(&r.rows[0].values[0]), "b");

    send(&mut ws, fetch_msg(sid1, None)).await;
    let resp = recv(&mut ws).await;
    let r = expect_result(&resp);
    assert_eq!(r.rows.len(), 2);
}

#[tokio::test]
async fn test_streaming_unknown_stream_id() {
    let dir = tempfile::tempdir().unwrap();
    let urls = start_server(dir.path(), Arc::new(TokenStore::open())).await;
    let mut ws = connect(&urls.ws).await;
    send(&mut ws, hello(None)).await;
    let _ = recv(&mut ws).await;

    send(&mut ws, fetch_msg(999, None)).await;
    let resp = recv(&mut ws).await;
    let e = expect_error(&resp);
    assert!(e.message.contains("Unknown stream_id"));
}

#[tokio::test]
async fn test_streaming_close_unknown() {
    let dir = tempfile::tempdir().unwrap();
    let urls = start_server(dir.path(), Arc::new(TokenStore::open())).await;
    let mut ws = connect(&urls.ws).await;
    send(&mut ws, hello(None)).await;
    let _ = recv(&mut ws).await;

    send(&mut ws, close_stream_msg(999, None)).await;
    let resp = recv(&mut ws).await;
    let e = expect_error(&resp);
    assert!(e.message.contains("Unknown stream_id"));
}

#[tokio::test]
async fn test_streaming_request_id_echo() {
    let dir = tempfile::tempdir().unwrap();
    let urls = start_server(dir.path(), Arc::new(TokenStore::open())).await;
    let mut ws = setup_streaming_data(&urls.ws, "StreamE", 5).await;

    send(&mut ws, execute_full(
        "MATCH (n:StreamE) RETURN n.id ORDER BY n.id", vec![], Some("exec-1"), Some(2),
    )).await;
    let resp = recv(&mut ws).await;
    let r = expect_result(&resp);
    assert_eq!(r.request_id.as_deref(), Some("exec-1"));
    let stream_id = r.stream_id.unwrap();

    send(&mut ws, fetch_msg(stream_id, Some("fetch-1"))).await;
    let resp = recv(&mut ws).await;
    let r = expect_result(&resp);
    assert_eq!(r.request_id.as_deref(), Some("fetch-1"));

    send(&mut ws, close_stream_msg(stream_id, Some("close-1"))).await;
    let resp = recv(&mut ws).await;
    let cs = expect_close_stream_ok(&resp);
    assert_eq!(cs.request_id.as_deref(), Some("close-1"));
}

#[tokio::test]
async fn test_streaming_no_fetch_size() {
    let dir = tempfile::tempdir().unwrap();
    let urls = start_server(dir.path(), Arc::new(TokenStore::open())).await;
    let mut ws = setup_streaming_data(&urls.ws, "StreamF", 5).await;

    send(&mut ws, execute("MATCH (n:StreamF) RETURN n.id ORDER BY n.id")).await;
    let resp = recv(&mut ws).await;
    let r = expect_result(&resp);
    assert_eq!(r.rows.len(), 5);
    assert!(r.stream_id.is_none());
    assert!(r.has_more.is_none());
}

#[tokio::test]
async fn test_streaming_fetch_size_zero() {
    let dir = tempfile::tempdir().unwrap();
    let urls = start_server(dir.path(), Arc::new(TokenStore::open())).await;
    let mut ws = setup_streaming_data(&urls.ws, "StreamH", 3).await;

    send(&mut ws, execute_full(
        "MATCH (n:StreamH) RETURN n.id", vec![], Some("fs0"), Some(0),
    )).await;
    let resp = recv(&mut ws).await;
    let e = expect_error(&resp);
    assert!(e.message.contains("fetch_size must be >= 1"));
    assert_eq!(e.request_id.as_deref(), Some("fs0"));

    send(&mut ws, execute("MATCH (n:StreamH) RETURN n.id ORDER BY n.id")).await;
    let resp = recv(&mut ws).await;
    let r = expect_result(&resp);
    assert_eq!(r.rows.len(), 3);
}

#[tokio::test]
async fn test_streaming_with_params() {
    let dir = tempfile::tempdir().unwrap();
    let urls = start_server(dir.path(), Arc::new(TokenStore::open())).await;
    let mut ws = setup_streaming_data(&urls.ws, "StreamI", 5).await;

    send(&mut ws, execute_full(
        "MATCH (n:StreamI) WHERE n.id > $min RETURN n.id ORDER BY n.id",
        vec![int_param("min", 2)],
        None, Some(2),
    )).await;
    let resp = recv(&mut ws).await;
    let r = expect_result(&resp);
    assert_eq!(r.rows.len(), 2);
    assert_eq!(r.has_more, Some(true));
    let stream_id = r.stream_id.unwrap();
    let mut all_ids: Vec<i64> = r.rows.iter().map(|row| val_int(&row.values[0])).collect();

    send(&mut ws, fetch_msg(stream_id, None)).await;
    let resp = recv(&mut ws).await;
    let r = expect_result(&resp);
    all_ids.extend(r.rows.iter().map(|row| val_int(&row.values[0])));

    assert_eq!(all_ids, vec![3, 4, 5]);
}

#[tokio::test]
async fn test_streaming_fetch_size_one() {
    let dir = tempfile::tempdir().unwrap();
    let urls = start_server(dir.path(), Arc::new(TokenStore::open())).await;
    let mut ws = setup_streaming_data(&urls.ws, "StreamJ", 3).await;

    send(&mut ws, execute_full(
        "MATCH (n:StreamJ) RETURN n.id ORDER BY n.id", vec![], None, Some(1),
    )).await;
    let resp = recv(&mut ws).await;
    let r = expect_result(&resp);
    assert_eq!(r.rows.len(), 1);
    assert_eq!(val_int(&r.rows[0].values[0]), 1);
    assert_eq!(r.has_more, Some(true));
    let stream_id = r.stream_id.unwrap();

    send(&mut ws, fetch_msg(stream_id, None)).await;
    let resp = recv(&mut ws).await;
    let r = expect_result(&resp);
    assert_eq!(val_int(&r.rows[0].values[0]), 2);
    assert_eq!(r.has_more, Some(true));

    send(&mut ws, fetch_msg(stream_id, None)).await;
    let resp = recv(&mut ws).await;
    let r = expect_result(&resp);
    assert_eq!(val_int(&r.rows[0].values[0]), 3);
    assert!(r.has_more.is_none());
    assert!(r.stream_id.is_none());
}

#[tokio::test]
async fn test_streaming_cursor_timeout() {
    let dir = tempfile::tempdir().unwrap();
    let urls = start_server_with_timeout(
        dir.path(),
        Arc::new(TokenStore::open()),
        Duration::from_millis(500),
    ).await;
    let mut ws = setup_streaming_data(&urls.ws, "StreamG", 5).await;

    send(&mut ws, execute_full(
        "MATCH (n:StreamG) RETURN n.id ORDER BY n.id", vec![], None, Some(2),
    )).await;
    let resp = recv(&mut ws).await;
    let r = expect_result(&resp);
    assert_eq!(r.has_more, Some(true));
    let stream_id = r.stream_id.unwrap();

    tokio::time::sleep(Duration::from_millis(700)).await;

    send(&mut ws, fetch_msg(stream_id, None)).await;
    let resp = recv(&mut ws).await;
    let e = expect_error(&resp);
    assert!(e.message.contains("Unknown stream_id"));
}

// ═══════════════════════════════════════════════════════════════
// Phase 5: HTTP endpoints (JSON mode)
// ═══════════════════════════════════════════════════════════════

fn http_client() -> reqwest::Client {
    reqwest::Client::new()
}

#[tokio::test]
async fn test_http_execute() {
    let dir = tempfile::tempdir().unwrap();
    let urls = start_server(dir.path(), Arc::new(TokenStore::open())).await;
    let client = http_client();

    let resp = client
        .post(format!("{}/v1/execute", urls.http))
        .json(&json!({"query": "RETURN 1 AS x"}))
        .send().await.unwrap();
    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["type"], "result");
    assert_eq!(body["columns"], json!(["x"]));
    assert_eq!(body["rows"], json!([[1]]));
    assert!(body["timing_ms"].as_f64().unwrap() >= 0.0);
}

#[tokio::test]
async fn test_http_execute_with_params() {
    let dir = tempfile::tempdir().unwrap();
    let urls = start_server(dir.path(), Arc::new(TokenStore::open())).await;
    let client = http_client();

    client.post(format!("{}/v1/execute", urls.http))
        .json(&json!({"query": "CREATE NODE TABLE HttpParam(name STRING, age INT64, PRIMARY KEY(name))"}))
        .send().await.unwrap();

    client.post(format!("{}/v1/execute", urls.http))
        .json(&json!({"query": "CREATE (:HttpParam {name: $name, age: $age})", "params": {"name": "Alice", "age": 30}}))
        .send().await.unwrap();

    let resp = client.post(format!("{}/v1/execute", urls.http))
        .json(&json!({"query": "MATCH (p:HttpParam) WHERE p.age > $min RETURN p.name", "params": {"min": 25}}))
        .send().await.unwrap();
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["type"], "result");
    assert_eq!(body["rows"], json!([["Alice"]]));
}

#[tokio::test]
async fn test_http_execute_bad_query() {
    let dir = tempfile::tempdir().unwrap();
    let urls = start_server(dir.path(), Arc::new(TokenStore::open())).await;
    let client = http_client();

    let resp = client.post(format!("{}/v1/execute", urls.http))
        .json(&json!({"query": "THIS IS NOT CYPHER"}))
        .send().await.unwrap();
    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["type"], "error");
    assert!(body["message"].as_str().unwrap().len() > 0);
}

#[tokio::test]
async fn test_http_execute_bad_body() {
    let dir = tempfile::tempdir().unwrap();
    let urls = start_server(dir.path(), Arc::new(TokenStore::open())).await;
    let client = http_client();

    let resp = client.post(format!("{}/v1/execute", urls.http))
        .json(&json!({"params": {"x": 1}}))
        .send().await.unwrap();
    assert_eq!(resp.status(), 400);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["type"], "error");
    assert!(body["message"].as_str().unwrap().contains("Invalid request body"));
}

#[tokio::test]
async fn test_http_execute_no_body() {
    let dir = tempfile::tempdir().unwrap();
    let urls = start_server(dir.path(), Arc::new(TokenStore::open())).await;
    let client = http_client();

    let resp = client.post(format!("{}/v1/execute", urls.http))
        .header("content-type", "application/json")
        .body("{not json}")
        .send().await.unwrap();
    assert_eq!(resp.status(), 400);
}

#[tokio::test]
async fn test_http_batch() {
    let dir = tempfile::tempdir().unwrap();
    let urls = start_server(dir.path(), Arc::new(TokenStore::open())).await;
    let client = http_client();

    client.post(format!("{}/v1/execute", urls.http))
        .json(&json!({"query": "CREATE NODE TABLE HttpBatch(id INT64, PRIMARY KEY(id))"}))
        .send().await.unwrap();

    let resp = client.post(format!("{}/v1/batch", urls.http))
        .json(&json!({"statements": [
            {"query": "CREATE (:HttpBatch {id: 1})"},
            {"query": "CREATE (:HttpBatch {id: 2})"},
            {"query": "CREATE (:HttpBatch {id: 3})"}
        ]}))
        .send().await.unwrap();
    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["type"], "batch_result");
    assert_eq!(body["results"].as_array().unwrap().len(), 3);
    for entry in body["results"].as_array().unwrap() {
        assert_eq!(entry["type"], "result");
    }

    let resp = client.post(format!("{}/v1/execute", urls.http))
        .json(&json!({"query": "MATCH (t:HttpBatch) RETURN t.id ORDER BY t.id"}))
        .send().await.unwrap();
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["rows"], json!([[1], [2], [3]]));
}

#[tokio::test]
async fn test_http_batch_stops_on_error() {
    let dir = tempfile::tempdir().unwrap();
    let urls = start_server(dir.path(), Arc::new(TokenStore::open())).await;
    let client = http_client();

    let resp = client.post(format!("{}/v1/batch", urls.http))
        .json(&json!({"statements": [
            {"query": "RETURN 1"},
            {"query": "NOT VALID CYPHER"},
            {"query": "RETURN 3"}
        ]}))
        .send().await.unwrap();
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["type"], "batch_result");
    let results = body["results"].as_array().unwrap();
    assert_eq!(results.len(), 2);
    assert_eq!(results[0]["type"], "result");
    assert_eq!(results[1]["type"], "error");
}

#[tokio::test]
async fn test_http_batch_empty() {
    let dir = tempfile::tempdir().unwrap();
    let urls = start_server(dir.path(), Arc::new(TokenStore::open())).await;
    let client = http_client();

    let resp = client.post(format!("{}/v1/batch", urls.http))
        .json(&json!({"statements": []}))
        .send().await.unwrap();
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["type"], "batch_result");
    assert_eq!(body["results"].as_array().unwrap().len(), 0);
}

#[tokio::test]
async fn test_http_pipeline() {
    let dir = tempfile::tempdir().unwrap();
    let urls = start_server(dir.path(), Arc::new(TokenStore::open())).await;
    let client = http_client();

    client.post(format!("{}/v1/execute", urls.http))
        .json(&json!({"query": "CREATE NODE TABLE HttpPipe(id INT64, PRIMARY KEY(id))"}))
        .send().await.unwrap();

    let resp = client.post(format!("{}/v1/pipeline", urls.http))
        .json(&json!({"statements": [
            {"query": "CREATE (:HttpPipe {id: 1})"},
            {"query": "CREATE (:HttpPipe {id: 2})"},
            {"query": "MATCH (t:HttpPipe) RETURN t.id ORDER BY t.id"}
        ]}))
        .send().await.unwrap();
    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["type"], "pipeline_result");
    let results = body["results"].as_array().unwrap();
    assert_eq!(results.len(), 3);
    assert_eq!(results[2]["rows"], json!([[1], [2]]));

    let resp = client.post(format!("{}/v1/execute", urls.http))
        .json(&json!({"query": "MATCH (t:HttpPipe) RETURN t.id ORDER BY t.id"}))
        .send().await.unwrap();
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["rows"], json!([[1], [2]]));
}

#[tokio::test]
async fn test_http_pipeline_rollback_on_error() {
    let dir = tempfile::tempdir().unwrap();
    let urls = start_server(dir.path(), Arc::new(TokenStore::open())).await;
    let client = http_client();

    client.post(format!("{}/v1/execute", urls.http))
        .json(&json!({"query": "CREATE NODE TABLE HttpPipeRb(id INT64, PRIMARY KEY(id))"}))
        .send().await.unwrap();

    let resp = client.post(format!("{}/v1/pipeline", urls.http))
        .json(&json!({"statements": [
            {"query": "CREATE (:HttpPipeRb {id: 1})"},
            {"query": "NOT VALID CYPHER"},
            {"query": "CREATE (:HttpPipeRb {id: 2})"}
        ]}))
        .send().await.unwrap();
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["type"], "pipeline_result");
    let results = body["results"].as_array().unwrap();
    assert_eq!(results.len(), 2);
    assert_eq!(results[0]["type"], "result");
    assert_eq!(results[1]["type"], "error");

    let resp = client.post(format!("{}/v1/execute", urls.http))
        .json(&json!({"query": "MATCH (t:HttpPipeRb) RETURN t.id"}))
        .send().await.unwrap();
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["rows"], json!([]));
}

#[tokio::test]
async fn test_http_pipeline_empty() {
    let dir = tempfile::tempdir().unwrap();
    let urls = start_server(dir.path(), Arc::new(TokenStore::open())).await;
    let client = http_client();

    let resp = client.post(format!("{}/v1/pipeline", urls.http))
        .json(&json!({"statements": []}))
        .send().await.unwrap();
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["type"], "pipeline_result");
    assert_eq!(body["results"].as_array().unwrap().len(), 0);
}

#[tokio::test]
async fn test_http_auth_required() {
    let dir = tempfile::tempdir().unwrap();
    let urls = start_server(dir.path(), Arc::new(TokenStore::from_token("secret123"))).await;
    let client = http_client();

    let resp = client.post(format!("{}/v1/execute", urls.http))
        .json(&json!({"query": "RETURN 1"}))
        .send().await.unwrap();
    assert_eq!(resp.status(), 401);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["type"], "error");
    assert_eq!(body["message"], "Unauthorized");
}

#[tokio::test]
async fn test_http_auth_wrong_token() {
    let dir = tempfile::tempdir().unwrap();
    let urls = start_server(dir.path(), Arc::new(TokenStore::from_token("secret123"))).await;
    let client = http_client();

    let resp = client.post(format!("{}/v1/execute", urls.http))
        .header("authorization", "Bearer wrong-token")
        .json(&json!({"query": "RETURN 1"}))
        .send().await.unwrap();
    assert_eq!(resp.status(), 401);
}

#[tokio::test]
async fn test_http_auth_success() {
    let dir = tempfile::tempdir().unwrap();
    let urls = start_server(dir.path(), Arc::new(TokenStore::from_token("secret123"))).await;
    let client = http_client();

    let resp = client.post(format!("{}/v1/execute", urls.http))
        .header("authorization", "Bearer secret123")
        .json(&json!({"query": "RETURN 1 AS x"}))
        .send().await.unwrap();
    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["type"], "result");
    assert_eq!(body["rows"], json!([[1]]));
}

#[tokio::test]
async fn test_http_auth_open_access() {
    let dir = tempfile::tempdir().unwrap();
    let urls = start_server(dir.path(), Arc::new(TokenStore::open())).await;
    let client = http_client();

    let resp = client.post(format!("{}/v1/execute", urls.http))
        .json(&json!({"query": "RETURN 42 AS x"}))
        .send().await.unwrap();
    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["rows"], json!([[42]]));

    let resp = client.post(format!("{}/v1/execute", urls.http))
        .header("authorization", "Bearer anything")
        .json(&json!({"query": "RETURN 42 AS x"}))
        .send().await.unwrap();
    assert_eq!(resp.status(), 200);
}

#[tokio::test]
async fn test_http_auth_all_endpoints() {
    let dir = tempfile::tempdir().unwrap();
    let urls = start_server(dir.path(), Arc::new(TokenStore::from_token("secret123"))).await;
    let client = http_client();

    let resp = client.post(format!("{}/v1/batch", urls.http))
        .json(&json!({"statements": [{"query": "RETURN 1"}]}))
        .send().await.unwrap();
    assert_eq!(resp.status(), 401);

    let resp = client.post(format!("{}/v1/pipeline", urls.http))
        .json(&json!({"statements": [{"query": "RETURN 1"}]}))
        .send().await.unwrap();
    assert_eq!(resp.status(), 401);

    let resp = client.post(format!("{}/v1/batch", urls.http))
        .header("authorization", "Bearer secret123")
        .json(&json!({"statements": [{"query": "RETURN 1"}]}))
        .send().await.unwrap();
    assert_eq!(resp.status(), 200);

    let resp = client.post(format!("{}/v1/pipeline", urls.http))
        .header("authorization", "Bearer secret123")
        .json(&json!({"statements": [{"query": "RETURN 1"}]}))
        .send().await.unwrap();
    assert_eq!(resp.status(), 200);
}

#[tokio::test]
async fn test_http_batch_with_params() {
    let dir = tempfile::tempdir().unwrap();
    let urls = start_server(dir.path(), Arc::new(TokenStore::open())).await;
    let client = http_client();

    client.post(format!("{}/v1/execute", urls.http))
        .json(&json!({"query": "CREATE NODE TABLE HttpBP(name STRING, age INT64, PRIMARY KEY(name))"}))
        .send().await.unwrap();

    let resp = client.post(format!("{}/v1/batch", urls.http))
        .json(&json!({"statements": [
            {"query": "CREATE (:HttpBP {name: $name, age: $age})", "params": {"name": "Alice", "age": 30}},
            {"query": "CREATE (:HttpBP {name: $name, age: $age})", "params": {"name": "Bob", "age": 25}}
        ]}))
        .send().await.unwrap();
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["type"], "batch_result");
    assert_eq!(body["results"].as_array().unwrap().len(), 2);

    let resp = client.post(format!("{}/v1/execute", urls.http))
        .json(&json!({"query": "MATCH (t:HttpBP) RETURN t.name ORDER BY t.name"}))
        .send().await.unwrap();
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["rows"], json!([["Alice"], ["Bob"]]));
}

#[tokio::test]
async fn test_http_pipeline_with_params() {
    let dir = tempfile::tempdir().unwrap();
    let urls = start_server(dir.path(), Arc::new(TokenStore::open())).await;
    let client = http_client();

    client.post(format!("{}/v1/execute", urls.http))
        .json(&json!({"query": "CREATE NODE TABLE HttpPP(name STRING, PRIMARY KEY(name))"}))
        .send().await.unwrap();

    let resp = client.post(format!("{}/v1/pipeline", urls.http))
        .json(&json!({"statements": [
            {"query": "CREATE (:HttpPP {name: $name})", "params": {"name": "X"}},
            {"query": "CREATE (:HttpPP {name: $name})", "params": {"name": "Y"}},
            {"query": "MATCH (t:HttpPP) RETURN t.name ORDER BY t.name"}
        ]}))
        .send().await.unwrap();
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["type"], "pipeline_result");
    assert_eq!(body["results"][2]["rows"], json!([["X"], ["Y"]]));
}

#[tokio::test]
async fn test_http_execute_create_and_query() {
    let dir = tempfile::tempdir().unwrap();
    let urls = start_server(dir.path(), Arc::new(TokenStore::open())).await;
    let client = http_client();

    let resp = client.post(format!("{}/v1/execute", urls.http))
        .json(&json!({"query": "CREATE NODE TABLE HttpCQ(name STRING, age INT64, PRIMARY KEY(name))"}))
        .send().await.unwrap();
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["type"], "result");

    client.post(format!("{}/v1/execute", urls.http))
        .json(&json!({"query": "CREATE (:HttpCQ {name: 'Alice', age: 30})"}))
        .send().await.unwrap();

    client.post(format!("{}/v1/execute", urls.http))
        .json(&json!({"query": "CREATE (:HttpCQ {name: 'Bob', age: 42})"}))
        .send().await.unwrap();

    let resp = client.post(format!("{}/v1/execute", urls.http))
        .json(&json!({"query": "MATCH (p:HttpCQ) RETURN p.name, p.age ORDER BY p.name"}))
        .send().await.unwrap();
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["type"], "result");
    assert_eq!(body["columns"], json!(["p.name", "p.age"]));
    assert_eq!(body["rows"], json!([["Alice", 30], ["Bob", 42]]));
}

// ═══════════════════════════════════════════════════════════════
// Phase 6: HTTP protobuf mode
// ═══════════════════════════════════════════════════════════════

fn proto_execute_bytes(query: &str, params: Vec<proto::MapEntry>) -> Vec<u8> {
    let msg = proto::Execute { query: query.into(), params, request_id: None, fetch_size: None };
    prost::Message::encode_to_vec(&msg)
}

fn proto_batch_bytes(stmts: Vec<proto::BatchStatement>) -> Vec<u8> {
    let msg = proto::Batch { statements: stmts, request_id: None };
    prost::Message::encode_to_vec(&msg)
}

fn decode_proto_resp(bytes: &[u8]) -> proto::ServerMessage {
    <proto::ServerMessage as prost::Message>::decode(bytes).unwrap()
}

#[tokio::test]
async fn test_http_proto_execute() {
    let dir = tempfile::tempdir().unwrap();
    let urls = start_server(dir.path(), Arc::new(TokenStore::open())).await;
    let client = http_client();

    let resp = client.post(format!("{}/v1/execute", urls.http))
        .header("content-type", "application/x-protobuf")
        .body(proto_execute_bytes("RETURN 1 AS x", vec![]))
        .send().await.unwrap();
    assert_eq!(resp.status(), 200);
    let bytes = resp.bytes().await.unwrap();
    let msg = decode_proto_resp(&bytes);
    let r = expect_result(&msg);
    assert_eq!(r.columns, vec!["x"]);
    assert_eq!(r.rows.len(), 1);
    assert_eq!(val_int(&r.rows[0].values[0]), 1);
}

#[tokio::test]
async fn test_http_proto_execute_with_params() {
    let dir = tempfile::tempdir().unwrap();
    let urls = start_server(dir.path(), Arc::new(TokenStore::open())).await;
    let client = http_client();

    // Create table via JSON (simpler setup)
    client.post(format!("{}/v1/execute", urls.http))
        .json(&json!({"query": "CREATE NODE TABLE ProtoParam(name STRING, age INT64, PRIMARY KEY(name))"}))
        .send().await.unwrap();
    client.post(format!("{}/v1/execute", urls.http))
        .json(&json!({"query": "CREATE (:ProtoParam {name: 'Alice', age: 30})"}))
        .send().await.unwrap();

    // Query via protobuf
    let resp = client.post(format!("{}/v1/execute", urls.http))
        .header("content-type", "application/x-protobuf")
        .body(proto_execute_bytes(
            "MATCH (p:ProtoParam) WHERE p.age > $min RETURN p.name",
            vec![int_param("min", 25)],
        ))
        .send().await.unwrap();
    let bytes = resp.bytes().await.unwrap();
    let msg = decode_proto_resp(&bytes);
    let r = expect_result(&msg);
    assert_eq!(r.rows.len(), 1);
    assert_eq!(val_str(&r.rows[0].values[0]), "Alice");
}

#[tokio::test]
async fn test_http_proto_execute_bad_query() {
    let dir = tempfile::tempdir().unwrap();
    let urls = start_server(dir.path(), Arc::new(TokenStore::open())).await;
    let client = http_client();

    let resp = client.post(format!("{}/v1/execute", urls.http))
        .header("content-type", "application/x-protobuf")
        .body(proto_execute_bytes("NOT VALID CYPHER", vec![]))
        .send().await.unwrap();
    assert_eq!(resp.status(), 200);
    let bytes = resp.bytes().await.unwrap();
    let msg = decode_proto_resp(&bytes);
    let e = expect_error(&msg);
    assert!(!e.message.is_empty());
}

#[tokio::test]
async fn test_http_proto_batch() {
    let dir = tempfile::tempdir().unwrap();
    let urls = start_server(dir.path(), Arc::new(TokenStore::open())).await;
    let client = http_client();

    // Setup via JSON
    client.post(format!("{}/v1/execute", urls.http))
        .json(&json!({"query": "CREATE NODE TABLE ProtoBatch(id INT64, PRIMARY KEY(id))"}))
        .send().await.unwrap();

    let resp = client.post(format!("{}/v1/batch", urls.http))
        .header("content-type", "application/x-protobuf")
        .body(proto_batch_bytes(vec![
            stmt("CREATE (:ProtoBatch {id: 1})"),
            stmt("CREATE (:ProtoBatch {id: 2})"),
        ]))
        .send().await.unwrap();
    assert_eq!(resp.status(), 200);
    let bytes = resp.bytes().await.unwrap();
    let msg = decode_proto_resp(&bytes);
    let br = expect_batch_result(&msg);
    assert_eq!(br.results.len(), 2);
    for entry in &br.results {
        br_expect_result(entry);
    }
}

#[tokio::test]
async fn test_http_proto_pipeline() {
    let dir = tempfile::tempdir().unwrap();
    let urls = start_server(dir.path(), Arc::new(TokenStore::open())).await;
    let client = http_client();

    client.post(format!("{}/v1/execute", urls.http))
        .json(&json!({"query": "CREATE NODE TABLE ProtoPipe(id INT64, PRIMARY KEY(id))"}))
        .send().await.unwrap();

    let resp = client.post(format!("{}/v1/pipeline", urls.http))
        .header("content-type", "application/x-protobuf")
        .body(proto_batch_bytes(vec![
            stmt("CREATE (:ProtoPipe {id: 1})"),
            stmt("CREATE (:ProtoPipe {id: 2})"),
            stmt("MATCH (t:ProtoPipe) RETURN t.id ORDER BY t.id"),
        ]))
        .send().await.unwrap();
    assert_eq!(resp.status(), 200);
    let bytes = resp.bytes().await.unwrap();
    let msg = decode_proto_resp(&bytes);
    // Pipeline returns BatchResult on the wire
    let br = expect_batch_result(&msg);
    assert_eq!(br.results.len(), 3);
    let last = br_expect_result(&br.results[2]);
    assert_eq!(last.rows.len(), 2);
    assert_eq!(val_int(&last.rows[0].values[0]), 1);
    assert_eq!(val_int(&last.rows[1].values[0]), 2);
}

#[tokio::test]
async fn test_http_proto_auth() {
    let dir = tempfile::tempdir().unwrap();
    let urls = start_server(dir.path(), Arc::new(TokenStore::from_token("secret123"))).await;
    let client = http_client();

    // No token with protobuf content-type
    let resp = client.post(format!("{}/v1/execute", urls.http))
        .header("content-type", "application/x-protobuf")
        .body(proto_execute_bytes("RETURN 1", vec![]))
        .send().await.unwrap();
    assert_eq!(resp.status(), 401);
    // Error response should also be protobuf
    let bytes = resp.bytes().await.unwrap();
    let msg = decode_proto_resp(&bytes);
    let e = expect_error(&msg);
    assert_eq!(e.message, "Unauthorized");

    // With correct token
    let resp = client.post(format!("{}/v1/execute", urls.http))
        .header("content-type", "application/x-protobuf")
        .header("authorization", "Bearer secret123")
        .body(proto_execute_bytes("RETURN 1 AS x", vec![]))
        .send().await.unwrap();
    assert_eq!(resp.status(), 200);
    let bytes = resp.bytes().await.unwrap();
    let msg = decode_proto_resp(&bytes);
    expect_result(&msg);
}

// ═══════════════════════════════════════════════════════════════
// Phase 7a: Query Rewriter
// ═══════════════════════════════════════════════════════════════

fn is_uuid_v4(s: &str) -> bool {
    if s.len() != 36 { return false; }
    let parts: Vec<&str> = s.split('-').collect();
    if parts.len() != 5 { return false; }
    if parts[0].len() != 8 || parts[1].len() != 4 || parts[2].len() != 4
        || parts[3].len() != 4 || parts[4].len() != 12 { return false; }
    // Version nibble = 4, variant nibble = 8/9/a/b
    parts[2].starts_with('4')
        && matches!(parts[3].as_bytes()[0], b'8' | b'9' | b'a' | b'b')
        && s.replace('-', "").chars().all(|c| c.is_ascii_hexdigit())
}

#[tokio::test]
async fn test_rewriter_uuid_ws() {
    let dir = tempfile::tempdir().unwrap();
    let urls = start_server(dir.path(), Arc::new(TokenStore::open())).await;
    let mut ws = connect(&urls.ws).await;

    send(&mut ws, hello(None)).await;
    let _ = recv(&mut ws).await;

    send(&mut ws, execute("CREATE NODE TABLE RwUuid(id STRING, PRIMARY KEY(id))")).await;
    let _ = recv(&mut ws).await;

    send(&mut ws, execute("CREATE (:RwUuid {id: gen_random_uuid()})")).await;
    let resp = recv(&mut ws).await;
    expect_result(&resp);

    send(&mut ws, execute("MATCH (n:RwUuid) RETURN n.id")).await;
    let resp = recv(&mut ws).await;
    let r = expect_result(&resp);
    assert_eq!(r.rows.len(), 1);
    let uuid = val_str(&r.rows[0].values[0]);
    assert!(is_uuid_v4(uuid), "Expected UUID v4, got: {uuid}");
}

#[tokio::test]
async fn test_rewriter_uuid_http() {
    let dir = tempfile::tempdir().unwrap();
    let urls = start_server(dir.path(), Arc::new(TokenStore::open())).await;
    let client = http_client();

    client.post(format!("{}/v1/execute", urls.http))
        .json(&json!({"query": "CREATE NODE TABLE RwUuidH(id STRING, PRIMARY KEY(id))"}))
        .send().await.unwrap();

    client.post(format!("{}/v1/execute", urls.http))
        .json(&json!({"query": "CREATE (:RwUuidH {id: gen_random_uuid()})"}))
        .send().await.unwrap();

    let resp = client.post(format!("{}/v1/execute", urls.http))
        .json(&json!({"query": "MATCH (n:RwUuidH) RETURN n.id"}))
        .send().await.unwrap();
    let body: serde_json::Value = resp.json().await.unwrap();
    let uuid = body["rows"][0][0].as_str().unwrap();
    assert!(is_uuid_v4(uuid), "Expected UUID v4, got: {uuid}");
}

#[tokio::test]
async fn test_rewriter_timestamp_ws() {
    let dir = tempfile::tempdir().unwrap();
    let urls = start_server(dir.path(), Arc::new(TokenStore::open())).await;
    let mut ws = connect(&urls.ws).await;

    send(&mut ws, hello(None)).await;
    let _ = recv(&mut ws).await;

    send(&mut ws, execute("CREATE NODE TABLE RwTs(id INT64, ts STRING, PRIMARY KEY(id))")).await;
    let _ = recv(&mut ws).await;

    send(&mut ws, execute("CREATE (:RwTs {id: 1, ts: current_timestamp()})")).await;
    let resp = recv(&mut ws).await;
    expect_result(&resp);

    send(&mut ws, execute("MATCH (n:RwTs) RETURN n.ts")).await;
    let resp = recv(&mut ws).await;
    let r = expect_result(&resp);
    let ts = val_str(&r.rows[0].values[0]);
    // ISO 8601: YYYY-MM-DDThh:mm:ss.uuuuuu
    assert!(ts.contains('T'), "Timestamp missing T separator: {ts}");
    assert_eq!(ts.len(), 26, "Timestamp wrong length: {ts}");
    assert_eq!(&ts[4..5], "-");
    assert_eq!(&ts[7..8], "-");
    assert_eq!(&ts[13..14], ":");
    assert_eq!(&ts[16..17], ":");
    assert_eq!(&ts[19..20], ".");
}

#[tokio::test]
async fn test_rewriter_current_date_ws() {
    let dir = tempfile::tempdir().unwrap();
    let urls = start_server(dir.path(), Arc::new(TokenStore::open())).await;
    let mut ws = connect(&urls.ws).await;

    send(&mut ws, hello(None)).await;
    let _ = recv(&mut ws).await;

    send(&mut ws, execute("CREATE NODE TABLE RwDt(id INT64, dt STRING, PRIMARY KEY(id))")).await;
    let _ = recv(&mut ws).await;

    send(&mut ws, execute("CREATE (:RwDt {id: 1, dt: current_date()})")).await;
    let resp = recv(&mut ws).await;
    expect_result(&resp);

    send(&mut ws, execute("MATCH (n:RwDt) RETURN n.dt")).await;
    let resp = recv(&mut ws).await;
    let r = expect_result(&resp);
    let dt = val_str(&r.rows[0].values[0]);
    // ISO 8601: YYYY-MM-DD
    assert_eq!(dt.len(), 10, "Date wrong length: {dt}");
    assert_eq!(&dt[4..5], "-");
    assert_eq!(&dt[7..8], "-");
    let year: i32 = dt[0..4].parse().unwrap();
    assert!(year >= 2024 && year <= 2030, "Year out of range: {year}");
}

#[tokio::test]
async fn test_rewriter_multiple_uuids_distinct() {
    let dir = tempfile::tempdir().unwrap();
    let urls = start_server(dir.path(), Arc::new(TokenStore::open())).await;
    let mut ws = connect(&urls.ws).await;

    send(&mut ws, hello(None)).await;
    let _ = recv(&mut ws).await;

    send(&mut ws, execute("CREATE NODE TABLE RwMulti(name STRING, uid STRING, PRIMARY KEY(name))")).await;
    let _ = recv(&mut ws).await;

    send(&mut ws, execute("CREATE (:RwMulti {name: 'a', uid: gen_random_uuid()})")).await;
    let _ = recv(&mut ws).await;

    send(&mut ws, execute("CREATE (:RwMulti {name: 'b', uid: gen_random_uuid()})")).await;
    let _ = recv(&mut ws).await;

    send(&mut ws, execute("MATCH (n:RwMulti) RETURN n.uid ORDER BY n.name")).await;
    let resp = recv(&mut ws).await;
    let r = expect_result(&resp);
    assert_eq!(r.rows.len(), 2);
    let uuid1 = val_str(&r.rows[0].values[0]);
    let uuid2 = val_str(&r.rows[1].values[0]);
    assert!(is_uuid_v4(uuid1), "First not UUID v4: {uuid1}");
    assert!(is_uuid_v4(uuid2), "Second not UUID v4: {uuid2}");
    assert_ne!(uuid1, uuid2, "UUIDs should be distinct");
}

#[tokio::test]
async fn test_rewriter_string_literal_preserved() {
    let dir = tempfile::tempdir().unwrap();
    let urls = start_server(dir.path(), Arc::new(TokenStore::open())).await;
    let mut ws = connect(&urls.ws).await;

    send(&mut ws, hello(None)).await;
    let _ = recv(&mut ws).await;

    // gen_random_uuid() inside a string literal should NOT be rewritten
    send(&mut ws, execute("RETURN 'gen_random_uuid()' AS x")).await;
    let resp = recv(&mut ws).await;
    let r = expect_result(&resp);
    assert_eq!(val_str(&r.rows[0].values[0]), "gen_random_uuid()");
}

#[tokio::test]
async fn test_rewriter_user_and_generated_params() {
    let dir = tempfile::tempdir().unwrap();
    let urls = start_server(dir.path(), Arc::new(TokenStore::open())).await;
    let mut ws = connect(&urls.ws).await;

    send(&mut ws, hello(None)).await;
    let _ = recv(&mut ws).await;

    send(&mut ws, execute("CREATE NODE TABLE RwMixed(name STRING, uid STRING, PRIMARY KEY(name))")).await;
    let _ = recv(&mut ws).await;

    // User-supplied $name param + rewritten gen_random_uuid()
    send(&mut ws, execute_full(
        "CREATE (:RwMixed {name: $name, uid: gen_random_uuid()})",
        vec![str_param("name", "test_user")],
        None, None,
    )).await;
    let resp = recv(&mut ws).await;
    expect_result(&resp);

    send(&mut ws, execute("MATCH (n:RwMixed) RETURN n.name, n.uid")).await;
    let resp = recv(&mut ws).await;
    let r = expect_result(&resp);
    assert_eq!(r.rows.len(), 1);
    assert_eq!(val_str(&r.rows[0].values[0]), "test_user");
    let uid = val_str(&r.rows[0].values[1]);
    assert!(is_uuid_v4(uid), "Expected UUID v4, got: {uid}");
}

#[tokio::test]
async fn test_rewriter_batch_uuid() {
    let dir = tempfile::tempdir().unwrap();
    let urls = start_server(dir.path(), Arc::new(TokenStore::open())).await;
    let mut ws = connect(&urls.ws).await;

    send(&mut ws, hello(None)).await;
    let _ = recv(&mut ws).await;

    send(&mut ws, execute("CREATE NODE TABLE RwBatch(id INT64, uid STRING, PRIMARY KEY(id))")).await;
    let _ = recv(&mut ws).await;

    // Batch with gen_random_uuid() in each statement
    send(&mut ws, batch_msg(vec![
        stmt("CREATE (:RwBatch {id: 1, uid: gen_random_uuid()})"),
        stmt("CREATE (:RwBatch {id: 2, uid: gen_random_uuid()})"),
        stmt("CREATE (:RwBatch {id: 3, uid: gen_random_uuid()})"),
    ], None)).await;
    let resp = recv(&mut ws).await;
    let br = expect_batch_result(&resp);
    assert_eq!(br.results.len(), 3);
    for entry in &br.results {
        br_expect_result(entry);
    }

    send(&mut ws, execute("MATCH (n:RwBatch) RETURN n.uid ORDER BY n.id")).await;
    let resp = recv(&mut ws).await;
    let r = expect_result(&resp);
    assert_eq!(r.rows.len(), 3);
    let uuids: Vec<&str> = r.rows.iter().map(|row| val_str(&row.values[0])).collect();
    for uid in &uuids {
        assert!(is_uuid_v4(uid), "Expected UUID v4, got: {uid}");
    }
    // All three should be distinct
    assert_ne!(uuids[0], uuids[1]);
    assert_ne!(uuids[1], uuids[2]);
    assert_ne!(uuids[0], uuids[2]);
}

// ═══════════════════════════════════════════════════════════════
// Phase 7b: Journal
// ═══════════════════════════════════════════════════════════════

struct JournalServerCtx {
    ws: String,
    http: String,
    journal_tx: strana::journal::JournalSender,
    journal_dir: std::path::PathBuf,
}

async fn start_server_with_journal(data_dir: &std::path::Path) -> JournalServerCtx {
    let db_path = data_dir.join("db");
    let db = strana::backend::Backend::open(&db_path).expect("Failed to open database");
    let journal_dir = data_dir.join("journal");
    let journal_state = Arc::new(strana::journal::JournalState::new());
    let journal_tx = strana::journal::spawn_journal_writer(
        journal_dir.clone(),
        64 * 1024 * 1024,
        50,
        journal_state.clone(),
    );
    let state = strana::server::AppState {
        db: Arc::new(db),
        tokens: Arc::new(TokenStore::open()),
        cursor_timeout: Duration::from_secs(30),
        journal: Some(journal_tx.clone()),
        journal_state: Some(journal_state),
        data_dir: db_path.clone(),
        snapshot_lock: Arc::new(std::sync::RwLock::new(())),
        retention_config: strana::snapshot::RetentionConfig { daily: 7, weekly: 4, monthly: 3 },
    };
    let app = axum::Router::new()
        .route("/ws", axum::routing::get(strana::server::ws_upgrade))
        .route("/v1/execute", axum::routing::post(strana::http::execute_handler))
        .route("/v1/batch", axum::routing::post(strana::http::batch_handler))
        .route("/v1/pipeline", axum::routing::post(strana::http::pipeline_handler))
        .route("/v1/snapshot", axum::routing::post(strana::http::snapshot_handler))
        .with_state(state);
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let port = addr.port();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    JournalServerCtx {
        ws: format!("ws://127.0.0.1:{port}/ws"),
        http: format!("http://127.0.0.1:{port}"),
        journal_tx,
        journal_dir,
    }
}

fn flush_journal(tx: &strana::journal::JournalSender) {
    let (ack_tx, ack_rx) = std::sync::mpsc::sync_channel(1);
    tx.send(strana::journal::JournalCommand::Flush(ack_tx)).unwrap();
    ack_rx.recv().unwrap();
}

fn read_journal_entries(dir: &std::path::Path) -> Vec<strana::journal::JournalReaderEntry> {
    match strana::journal::JournalReader::open(dir) {
        Ok(reader) => reader.map(|r| r.unwrap()).collect(),
        Err(_) => vec![],
    }
}

#[tokio::test]
async fn test_journal_records_mutations_ws() {
    let dir = tempfile::tempdir().unwrap();
    let ctx = start_server_with_journal(dir.path()).await;
    let mut ws = connect(&ctx.ws).await;

    send(&mut ws, hello(None)).await;
    let _ = recv(&mut ws).await;

    send(&mut ws, execute("CREATE NODE TABLE JnlWs(id INT64, PRIMARY KEY(id))")).await;
    let _ = recv(&mut ws).await;

    send(&mut ws, execute("CREATE (:JnlWs {id: 1})")).await;
    let _ = recv(&mut ws).await;

    flush_journal(&ctx.journal_tx);
    let entries = read_journal_entries(&ctx.journal_dir);
    assert_eq!(entries.len(), 2, "Expected 2 journal entries, got {}", entries.len());
    assert!(entries[0].entry.query.contains("CREATE NODE TABLE"));
    assert!(entries[1].entry.query.contains("CREATE"));
}

#[tokio::test]
async fn test_journal_skips_reads() {
    let dir = tempfile::tempdir().unwrap();
    let ctx = start_server_with_journal(dir.path()).await;
    let mut ws = connect(&ctx.ws).await;

    send(&mut ws, hello(None)).await;
    let _ = recv(&mut ws).await;

    send(&mut ws, execute("CREATE NODE TABLE JnlRd(id INT64, PRIMARY KEY(id))")).await;
    let _ = recv(&mut ws).await;

    send(&mut ws, execute("CREATE (:JnlRd {id: 1})")).await;
    let _ = recv(&mut ws).await;

    // These reads should NOT be journaled
    send(&mut ws, execute("MATCH (n:JnlRd) RETURN n.id")).await;
    let _ = recv(&mut ws).await;

    send(&mut ws, execute("RETURN 42")).await;
    let _ = recv(&mut ws).await;

    flush_journal(&ctx.journal_tx);
    let entries = read_journal_entries(&ctx.journal_dir);
    assert_eq!(entries.len(), 2, "Expected 2 entries (mutations only), got {}", entries.len());
    for entry in &entries {
        assert!(
            entry.entry.query.contains("CREATE"),
            "Non-mutation in journal: {}", entry.entry.query
        );
    }
}

#[tokio::test]
async fn test_journal_tx_commit_journals_all() {
    let dir = tempfile::tempdir().unwrap();
    let ctx = start_server_with_journal(dir.path()).await;
    let mut ws = connect(&ctx.ws).await;

    send(&mut ws, hello(None)).await;
    let _ = recv(&mut ws).await;

    send(&mut ws, execute("CREATE NODE TABLE JnlTxC(id INT64, PRIMARY KEY(id))")).await;
    let _ = recv(&mut ws).await;

    flush_journal(&ctx.journal_tx);
    let before = read_journal_entries(&ctx.journal_dir).len();

    // Begin transaction, insert two, commit
    send(&mut ws, begin(None, None)).await;
    let _ = recv(&mut ws).await;

    send(&mut ws, execute("CREATE (:JnlTxC {id: 1})")).await;
    let _ = recv(&mut ws).await;

    send(&mut ws, execute("CREATE (:JnlTxC {id: 2})")).await;
    let _ = recv(&mut ws).await;

    send(&mut ws, commit(None)).await;
    let _ = recv(&mut ws).await;

    flush_journal(&ctx.journal_tx);
    let entries = read_journal_entries(&ctx.journal_dir);
    let new_entries = entries.len() - before;
    assert_eq!(new_entries, 2, "Expected 2 new entries after commit, got {new_entries}");
}

#[tokio::test]
async fn test_journal_tx_rollback_journals_none() {
    let dir = tempfile::tempdir().unwrap();
    let ctx = start_server_with_journal(dir.path()).await;
    let mut ws = connect(&ctx.ws).await;

    send(&mut ws, hello(None)).await;
    let _ = recv(&mut ws).await;

    send(&mut ws, execute("CREATE NODE TABLE JnlTxR(id INT64, PRIMARY KEY(id))")).await;
    let _ = recv(&mut ws).await;

    flush_journal(&ctx.journal_tx);
    let before = read_journal_entries(&ctx.journal_dir).len();

    // Begin transaction, insert two, ROLLBACK
    send(&mut ws, begin(None, None)).await;
    let _ = recv(&mut ws).await;

    send(&mut ws, execute("CREATE (:JnlTxR {id: 1})")).await;
    let _ = recv(&mut ws).await;

    send(&mut ws, execute("CREATE (:JnlTxR {id: 2})")).await;
    let _ = recv(&mut ws).await;

    send(&mut ws, rollback(None)).await;
    let _ = recv(&mut ws).await;

    flush_journal(&ctx.journal_tx);
    let entries = read_journal_entries(&ctx.journal_dir);
    let new_entries = entries.len() - before;
    assert_eq!(new_entries, 0, "Expected 0 new entries after rollback, got {new_entries}");
}

#[tokio::test]
async fn test_journal_http_mutation() {
    let dir = tempfile::tempdir().unwrap();
    let ctx = start_server_with_journal(dir.path()).await;
    let client = http_client();

    client.post(format!("{}/v1/execute", ctx.http))
        .json(&json!({"query": "CREATE NODE TABLE JnlHttp(id INT64, PRIMARY KEY(id))"}))
        .send().await.unwrap();

    client.post(format!("{}/v1/execute", ctx.http))
        .json(&json!({"query": "CREATE (:JnlHttp {id: 1})"}))
        .send().await.unwrap();

    // Read should NOT be journaled
    client.post(format!("{}/v1/execute", ctx.http))
        .json(&json!({"query": "MATCH (n:JnlHttp) RETURN n.id"}))
        .send().await.unwrap();

    flush_journal(&ctx.journal_tx);
    let entries = read_journal_entries(&ctx.journal_dir);
    assert_eq!(entries.len(), 2, "Expected 2 entries (mutations only), got {}", entries.len());
}

#[tokio::test]
async fn test_journal_http_pipeline_commit() {
    let dir = tempfile::tempdir().unwrap();
    let ctx = start_server_with_journal(dir.path()).await;
    let client = http_client();

    client.post(format!("{}/v1/execute", ctx.http))
        .json(&json!({"query": "CREATE NODE TABLE JnlPipe(id INT64, PRIMARY KEY(id))"}))
        .send().await.unwrap();

    flush_journal(&ctx.journal_tx);
    let before = read_journal_entries(&ctx.journal_dir).len();

    // Pipeline commits on success — both mutations should be journaled
    let resp = client.post(format!("{}/v1/pipeline", ctx.http))
        .json(&json!({"statements": [
            {"query": "CREATE (:JnlPipe {id: 1})"},
            {"query": "CREATE (:JnlPipe {id: 2})"},
            {"query": "MATCH (n:JnlPipe) RETURN n.id"}
        ]}))
        .send().await.unwrap();
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["type"], "pipeline_result");

    flush_journal(&ctx.journal_tx);
    let entries = read_journal_entries(&ctx.journal_dir);
    let new_entries = entries.len() - before;
    assert_eq!(new_entries, 2, "Expected 2 entries from pipeline commit, got {new_entries}");
}

#[tokio::test]
async fn test_journal_http_pipeline_rollback() {
    let dir = tempfile::tempdir().unwrap();
    let ctx = start_server_with_journal(dir.path()).await;
    let client = http_client();

    client.post(format!("{}/v1/execute", ctx.http))
        .json(&json!({"query": "CREATE NODE TABLE JnlPipeRb(id INT64, PRIMARY KEY(id))"}))
        .send().await.unwrap();

    flush_journal(&ctx.journal_tx);
    let before = read_journal_entries(&ctx.journal_dir).len();

    // Pipeline with error — should rollback, nothing journaled
    let resp = client.post(format!("{}/v1/pipeline", ctx.http))
        .json(&json!({"statements": [
            {"query": "CREATE (:JnlPipeRb {id: 1})"},
            {"query": "NOT VALID CYPHER"},
            {"query": "CREATE (:JnlPipeRb {id: 2})"}
        ]}))
        .send().await.unwrap();
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["type"], "pipeline_result");

    flush_journal(&ctx.journal_tx);
    let entries = read_journal_entries(&ctx.journal_dir);
    let new_entries = entries.len() - before;
    assert_eq!(new_entries, 0, "Expected 0 entries after pipeline rollback, got {new_entries}");
}

#[tokio::test]
async fn test_journal_chain_hash_integrity() {
    let dir = tempfile::tempdir().unwrap();
    let ctx = start_server_with_journal(dir.path()).await;
    let mut ws = connect(&ctx.ws).await;

    send(&mut ws, hello(None)).await;
    let _ = recv(&mut ws).await;

    send(&mut ws, execute("CREATE NODE TABLE JnlHash(id INT64, PRIMARY KEY(id))")).await;
    let _ = recv(&mut ws).await;

    for i in 1..=5 {
        send(&mut ws, execute(&format!("CREATE (:JnlHash {{id: {i}}})"))).await;
        let _ = recv(&mut ws).await;
    }

    flush_journal(&ctx.journal_tx);
    let entries = read_journal_entries(&ctx.journal_dir);
    assert_eq!(entries.len(), 6); // 1 CREATE TABLE + 5 CREATEs

    // Verify sequences are monotonically increasing
    for i in 1..entries.len() {
        assert_eq!(
            entries[i].sequence,
            entries[i - 1].sequence + 1,
            "Non-monotonic sequence at index {i}"
        );
    }

    // Verify chain hashes are non-zero after first entry
    for entry in &entries[1..] {
        assert_ne!(entry.chain_hash, [0u8; 32], "Zero chain hash");
    }
}

#[tokio::test]
async fn test_journal_disabled_by_default() {
    let dir = tempfile::tempdir().unwrap();
    let urls = start_server(dir.path(), Arc::new(TokenStore::open())).await;
    let mut ws = connect(&urls.ws).await;

    send(&mut ws, hello(None)).await;
    let _ = recv(&mut ws).await;

    send(&mut ws, execute("CREATE NODE TABLE JnlOff(id INT64, PRIMARY KEY(id))")).await;
    let _ = recv(&mut ws).await;

    send(&mut ws, execute("CREATE (:JnlOff {id: 1})")).await;
    let _ = recv(&mut ws).await;

    // No journal directory should exist when journal is disabled
    let journal_dir = dir.path().join("journal");
    assert!(!journal_dir.exists(), "Journal directory should not exist when disabled");
}

#[tokio::test]
async fn test_journal_batch_mutations() {
    let dir = tempfile::tempdir().unwrap();
    let ctx = start_server_with_journal(dir.path()).await;
    let mut ws = connect(&ctx.ws).await;

    send(&mut ws, hello(None)).await;
    let _ = recv(&mut ws).await;

    send(&mut ws, execute("CREATE NODE TABLE JnlBatch(id INT64, PRIMARY KEY(id))")).await;
    let _ = recv(&mut ws).await;

    flush_journal(&ctx.journal_tx);
    let before = read_journal_entries(&ctx.journal_dir).len();

    // Batch with mix of mutations and a read
    send(&mut ws, batch_msg(vec![
        stmt("CREATE (:JnlBatch {id: 1})"),
        stmt("CREATE (:JnlBatch {id: 2})"),
    ], None)).await;
    let resp = recv(&mut ws).await;
    let br = expect_batch_result(&resp);
    assert_eq!(br.results.len(), 2);

    flush_journal(&ctx.journal_tx);
    let entries = read_journal_entries(&ctx.journal_dir);
    let new_entries = entries.len() - before;
    assert_eq!(new_entries, 2, "Expected 2 batch mutation entries, got {new_entries}");
}

#[tokio::test]
async fn test_journal_params_preserved() {
    let dir = tempfile::tempdir().unwrap();
    let ctx = start_server_with_journal(dir.path()).await;
    let mut ws = connect(&ctx.ws).await;

    send(&mut ws, hello(None)).await;
    let _ = recv(&mut ws).await;

    send(&mut ws, execute("CREATE NODE TABLE JnlParams(name STRING, age INT64, PRIMARY KEY(name))")).await;
    let _ = recv(&mut ws).await;

    send(&mut ws, execute_full(
        "CREATE (:JnlParams {name: $name, age: $age})",
        vec![str_param("name", "Alice"), int_param("age", 30)],
        None, None,
    )).await;
    let _ = recv(&mut ws).await;

    flush_journal(&ctx.journal_tx);
    let entries = read_journal_entries(&ctx.journal_dir);
    // Last entry should have params
    let last = entries.last().unwrap();
    assert!(last.entry.query.contains("CREATE"));
    assert!(!last.entry.params.is_empty(), "Journal entry should have params");

    // Verify param values
    let params = &last.entry.params;
    let name_param = params.iter().find(|p| p.key == "name").unwrap();
    let name_val = name_param.value.as_ref().unwrap();
    match &name_val.value {
        Some(strana::wire::proto::graph_value::Value::StringValue(s)) => {
            assert_eq!(s, "Alice");
        }
        other => panic!("Expected string param, got: {other:?}"),
    }
}

// ═══════════════════════════════════════════════════════════════
// Phase 7c: Snapshots
// ═══════════════════════════════════════════════════════════════

struct SnapshotServerCtx {
    #[allow(dead_code)]
    ws: String,
    http: String,
    journal_tx: strana::journal::JournalSender,
    journal_dir: std::path::PathBuf,
    data_dir: std::path::PathBuf,
}

/// Start a server with production-like layout:
///   DB at data_dir/db, journal at data_dir/journal, snapshots at data_dir/snapshots.
async fn start_snapshot_server(data_dir: &std::path::Path) -> SnapshotServerCtx {
    std::fs::create_dir_all(data_dir).unwrap();
    let db_dir = data_dir.join("db");
    let db = strana::backend::Backend::open(&db_dir).expect("Failed to open database");
    let journal_dir = data_dir.join("journal");
    let journal_state = Arc::new(strana::journal::JournalState::new());
    let journal_tx = strana::journal::spawn_journal_writer(
        journal_dir.clone(),
        64 * 1024 * 1024,
        50,
        journal_state.clone(),
    );
    let state = strana::server::AppState {
        db: Arc::new(db),
        tokens: Arc::new(TokenStore::open()),
        cursor_timeout: Duration::from_secs(30),
        journal: Some(journal_tx.clone()),
        journal_state: Some(journal_state),
        data_dir: data_dir.to_path_buf(),
        snapshot_lock: Arc::new(std::sync::RwLock::new(())),
        retention_config: strana::snapshot::RetentionConfig { daily: 7, weekly: 4, monthly: 3 },
    };
    let app = axum::Router::new()
        .route("/ws", axum::routing::get(strana::server::ws_upgrade))
        .route("/v1/execute", axum::routing::post(strana::http::execute_handler))
        .route("/v1/batch", axum::routing::post(strana::http::batch_handler))
        .route("/v1/pipeline", axum::routing::post(strana::http::pipeline_handler))
        .route("/v1/snapshot", axum::routing::post(strana::http::snapshot_handler))
        .with_state(state);
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let port = addr.port();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    SnapshotServerCtx {
        ws: format!("ws://127.0.0.1:{port}/ws"),
        http: format!("http://127.0.0.1:{port}"),
        journal_tx,
        journal_dir,
        data_dir: data_dir.to_path_buf(),
    }
}

/// Start a minimal server for read verification (no journal). DB at data_dir/db.
async fn start_verify_server(data_dir: &std::path::Path) -> String {
    let db_dir = data_dir.join("db");
    let db = strana::backend::Backend::open(&db_dir).expect("Failed to open database");
    let state = strana::server::AppState {
        db: Arc::new(db),
        tokens: Arc::new(TokenStore::open()),
        cursor_timeout: Duration::from_secs(30),
        journal: None,
        journal_state: None,
        data_dir: data_dir.to_path_buf(),
        snapshot_lock: Arc::new(std::sync::RwLock::new(())),
        retention_config: strana::snapshot::RetentionConfig { daily: 7, weekly: 4, monthly: 3 },
    };
    let app = axum::Router::new()
        .route("/v1/execute", axum::routing::post(strana::http::execute_handler))
        .with_state(state);
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let port = addr.port();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    format!("http://127.0.0.1:{port}")
}

/// Recursive directory copy for tests.
fn copy_dir_all(src: &std::path::Path, dst: &std::path::Path) {
    std::fs::create_dir_all(dst).unwrap();
    for entry in std::fs::read_dir(src).unwrap() {
        let entry = entry.unwrap();
        let src_path = entry.path();
        let dst_path = dst.join(entry.file_name());
        if src_path.is_dir() {
            copy_dir_all(&src_path, &dst_path);
        } else {
            std::fs::copy(&src_path, &dst_path).unwrap();
        }
    }
}

/// Query the server and return rows as JSON.
async fn query_rows(http: &str, query: &str) -> Vec<serde_json::Value> {
    let client = http_client();
    let resp = client
        .post(format!("{http}/v1/execute"))
        .json(&json!({"query": query}))
        .send()
        .await
        .unwrap();
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["type"], "result", "Query failed: {}", body);
    body["rows"].as_array().unwrap().clone()
}

#[tokio::test]
async fn test_snapshot_http_endpoint() {
    let dir = tempfile::tempdir().unwrap();
    let ctx = start_snapshot_server(dir.path()).await;
    let client = http_client();

    // Create some data
    client
        .post(format!("{}/v1/execute", ctx.http))
        .json(&json!({"query": "CREATE NODE TABLE SnapHttp(id INT64, PRIMARY KEY(id))"}))
        .send()
        .await
        .unwrap();
    client
        .post(format!("{}/v1/execute", ctx.http))
        .json(&json!({"query": "CREATE (:SnapHttp {id: 1})"}))
        .send()
        .await
        .unwrap();

    // Take snapshot
    let resp = client
        .post(format!("{}/v1/snapshot", ctx.http))
        .send()
        .await
        .unwrap();
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["type"], "result", "Snapshot failed: {body}");

    let columns = body["columns"].as_array().unwrap();
    assert_eq!(columns[0], "sequence");
    assert_eq!(columns[1], "path");

    let rows = body["rows"].as_array().unwrap();
    assert_eq!(rows.len(), 1);
    let seq = rows[0][0].as_i64().unwrap();
    assert!(seq > 0, "Expected positive sequence, got {seq}");

    // Verify snapshot directory and meta file exist
    let snap_path = rows[0][1].as_str().unwrap();
    let snap_dir = std::path::Path::new(snap_path);
    assert!(snap_dir.exists(), "Snapshot dir should exist: {snap_path}");
    assert!(
        snap_dir.join("snapshot.meta").exists(),
        "snapshot.meta should exist"
    );
    assert!(snap_dir.join("data").exists(), "data/ should exist");
}

#[tokio::test]
async fn test_snapshot_requires_journal() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("snap_nojrnl");
    let db =
        strana::backend::Backend::open(&db_path).expect("Failed to open database");
    let state = strana::server::AppState {
        db: Arc::new(db),
        tokens: Arc::new(TokenStore::open()),
        cursor_timeout: Duration::from_secs(30),
        journal: None,
        journal_state: None,
        data_dir: db_path,
        snapshot_lock: Arc::new(std::sync::RwLock::new(())),
        retention_config: strana::snapshot::RetentionConfig { daily: 7, weekly: 4, monthly: 3 },
    };
    let app = axum::Router::new()
        .route(
            "/v1/snapshot",
            axum::routing::post(strana::http::snapshot_handler),
        )
        .with_state(state);
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let port = addr.port();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

    let client = http_client();
    let resp = client
        .post(format!("http://127.0.0.1:{port}/v1/snapshot"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 400);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["type"], "error");
    assert!(body["message"]
        .as_str()
        .unwrap()
        .contains("Journal is not enabled"));
}

#[tokio::test]
async fn test_snapshot_meta_persisted() {
    let dir = tempfile::tempdir().unwrap();
    let ctx = start_snapshot_server(dir.path()).await;
    let client = http_client();

    client
        .post(format!("{}/v1/execute", ctx.http))
        .json(&json!({"query": "CREATE NODE TABLE SnapMeta(id INT64, PRIMARY KEY(id))"}))
        .send()
        .await
        .unwrap();
    client
        .post(format!("{}/v1/execute", ctx.http))
        .json(&json!({"query": "CREATE (:SnapMeta {id: 1})"}))
        .send()
        .await
        .unwrap();

    let resp = client
        .post(format!("{}/v1/snapshot", ctx.http))
        .send()
        .await
        .unwrap();
    let body: serde_json::Value = resp.json().await.unwrap();
    let snap_path = body["rows"][0][1].as_str().unwrap().to_string();

    // Read and verify snapshot.meta protobuf
    let meta_bytes =
        std::fs::read(std::path::Path::new(&snap_path).join("snapshot.meta")).unwrap();
    let meta = <strana::wire::proto::SnapshotMeta as prost::Message>::decode(
        meta_bytes.as_slice(),
    )
    .unwrap();
    assert!(meta.sequence > 0);
    assert_eq!(meta.chain_hash.len(), 32);
    assert!(meta.timestamp_ms > 0);
    assert!(!meta.version.is_empty());
}

#[tokio::test]
async fn test_snapshot_restore_basic() {
    let dir = tempfile::tempdir().unwrap();
    let ctx = start_snapshot_server(dir.path()).await;
    let client = http_client();

    // Create table + data
    client
        .post(format!("{}/v1/execute", ctx.http))
        .json(&json!({"query": "CREATE NODE TABLE SnapRestore(id INT64, name STRING, PRIMARY KEY(id))"}))
        .send()
        .await
        .unwrap();
    for i in 1..=3 {
        client
            .post(format!("{}/v1/execute", ctx.http))
            .json(&json!({"query": format!("CREATE (:SnapRestore {{id: {i}, name: 'item{i}'}})")}))
            .send()
            .await
            .unwrap();
    }

    // Take snapshot
    let resp = client
        .post(format!("{}/v1/snapshot", ctx.http))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    // Flush journal to disk
    flush_journal(&ctx.journal_tx);

    // Copy snapshots/ and journal/ to a fresh directory for restore
    let restore_dir = tempfile::tempdir().unwrap();
    copy_dir_all(
        &ctx.data_dir.join("snapshots"),
        &restore_dir.path().join("snapshots"),
    );
    copy_dir_all(
        &ctx.data_dir.join("journal"),
        &restore_dir.path().join("journal"),
    );

    // Run restore
    strana::snapshot::restore(restore_dir.path(), None).unwrap();

    // Start a verification server at the restored directory
    let verify_http = start_verify_server(restore_dir.path()).await;

    // Verify all data is present
    let rows = query_rows(
        &verify_http,
        "MATCH (n:SnapRestore) RETURN n.id, n.name ORDER BY n.id",
    )
    .await;
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0][0].as_i64().unwrap(), 1);
    assert_eq!(rows[0][1].as_str().unwrap(), "item1");
    assert_eq!(rows[2][0].as_i64().unwrap(), 3);
    assert_eq!(rows[2][1].as_str().unwrap(), "item3");
}

#[tokio::test]
async fn test_snapshot_restore_with_journal_replay() {
    let dir = tempfile::tempdir().unwrap();
    let ctx = start_snapshot_server(dir.path()).await;
    let client = http_client();

    // Phase 1: Create initial data
    client
        .post(format!("{}/v1/execute", ctx.http))
        .json(&json!({"query": "CREATE NODE TABLE SnapReplay(id INT64, PRIMARY KEY(id))"}))
        .send()
        .await
        .unwrap();
    for i in 1..=3 {
        client
            .post(format!("{}/v1/execute", ctx.http))
            .json(&json!({"query": format!("CREATE (:SnapReplay {{id: {i}}})")}))
            .send()
            .await
            .unwrap();
    }

    // Take snapshot (captures CREATE TABLE + 3 CREATEs)
    let resp = client
        .post(format!("{}/v1/snapshot", ctx.http))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    // Phase 2: Add more data AFTER the snapshot (journal only)
    for i in 4..=6 {
        client
            .post(format!("{}/v1/execute", ctx.http))
            .json(&json!({"query": format!("CREATE (:SnapReplay {{id: {i}}})")}))
            .send()
            .await
            .unwrap();
    }

    // Flush journal to ensure post-snapshot entries are on disk
    flush_journal(&ctx.journal_tx);

    // Copy to fresh dir for restore
    let restore_dir = tempfile::tempdir().unwrap();
    let restore_data = restore_dir.path().join("data");
    std::fs::create_dir_all(&restore_data).unwrap();
    copy_dir_all(
        &ctx.data_dir.join("snapshots"),
        &restore_data.join("snapshots"),
    );
    copy_dir_all(
        &ctx.data_dir.join("journal"),
        &restore_data.join("journal"),
    );

    // Restore: should apply snapshot + replay 3 post-snapshot journal entries
    strana::snapshot::restore(&restore_data, None).unwrap();

    // Verify ALL 6 rows are present
    let verify_http = start_verify_server(&restore_data).await;
    let rows = query_rows(
        &verify_http,
        "MATCH (n:SnapReplay) RETURN n.id ORDER BY n.id",
    )
    .await;
    assert_eq!(
        rows.len(),
        6,
        "Expected 6 rows after restore + replay, got {}",
        rows.len()
    );
    for i in 1..=6i64 {
        assert_eq!(rows[(i - 1) as usize][0].as_i64().unwrap(), i);
    }
}

#[tokio::test]
async fn test_snapshot_restore_empty_journal_replay() {
    // Restore from snapshot with no post-snapshot journal entries to replay
    let dir = tempfile::tempdir().unwrap();
    let ctx = start_snapshot_server(dir.path()).await;
    let client = http_client();

    // Create data
    client
        .post(format!("{}/v1/execute", ctx.http))
        .json(&json!({"query": "CREATE NODE TABLE SnapEmpty(id INT64, PRIMARY KEY(id))"}))
        .send()
        .await
        .unwrap();
    client
        .post(format!("{}/v1/execute", ctx.http))
        .json(&json!({"query": "CREATE (:SnapEmpty {id: 1})"}))
        .send()
        .await
        .unwrap();

    // Take snapshot — all mutations covered
    let resp = client
        .post(format!("{}/v1/snapshot", ctx.http))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    flush_journal(&ctx.journal_tx);

    // Copy to fresh dir
    let restore_dir = tempfile::tempdir().unwrap();
    let restore_data = restore_dir.path().join("data");
    std::fs::create_dir_all(&restore_data).unwrap();
    copy_dir_all(
        &ctx.data_dir.join("snapshots"),
        &restore_data.join("snapshots"),
    );
    copy_dir_all(
        &ctx.data_dir.join("journal"),
        &restore_data.join("journal"),
    );

    // Restore — nothing after snapshot to replay
    strana::snapshot::restore(&restore_data, None).unwrap();

    let verify_http = start_verify_server(&restore_data).await;
    let rows = query_rows(&verify_http, "MATCH (n:SnapEmpty) RETURN n.id").await;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0].as_i64().unwrap(), 1);
}

#[tokio::test]
async fn test_snapshot_gfs_retention_integration() {
    let dir = tempfile::tempdir().unwrap();
    let snapshots_dir = dir.path().join("snapshots");
    let journal_dir = dir.path().join("journal");
    std::fs::create_dir_all(&journal_dir).unwrap();

    // Create 10 snapshots spaced 1 day apart
    for i in 1..=10u64 {
        let snap = snapshots_dir.join(format!("{:016}", i));
        std::fs::create_dir_all(&snap).unwrap();
        let meta = strana::wire::proto::SnapshotMeta {
            sequence: i,
            chain_hash: vec![0u8; 32],
            timestamp_ms: i as i64 * 86_400_000, // 1 day apart
            version: "test".into(),
        };
        std::fs::write(
            snap.join("snapshot.meta"),
            prost::Message::encode_to_vec(&meta),
        )
        .unwrap();
    }

    // Apply retention: keep 3 daily, 2 weekly, 1 monthly
    strana::snapshot::apply_retention(
        &snapshots_dir,
        &journal_dir,
        &strana::snapshot::RetentionConfig {
            daily: 3,
            weekly: 2,
            monthly: 1,
        },
    )
    .unwrap();

    // Count remaining snapshots
    let remaining: Vec<_> = std::fs::read_dir(&snapshots_dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| e.path().join("snapshot.meta").exists())
        .collect();
    // The 3 most recent should be kept as daily; weekly/monthly may overlap
    assert!(
        remaining.len() >= 3,
        "Should keep at least 3 daily snapshots, got {}",
        remaining.len()
    );
    assert!(
        remaining.len() <= 6,
        "Should not keep more than daily+weekly+monthly, got {}",
        remaining.len()
    );

    // The 3 most recent (8, 9, 10) should definitely be kept as daily
    for seq in [8u64, 9, 10] {
        assert!(
            snapshots_dir.join(format!("{:016}", seq)).exists(),
            "Snapshot {seq} should be retained (daily)"
        );
    }
}

#[tokio::test]
async fn test_snapshot_restore_specific_path() {
    // Restore from a specific snapshot path (not the latest)
    let dir = tempfile::tempdir().unwrap();
    let ctx = start_snapshot_server(dir.path()).await;
    let client = http_client();

    // Create table
    client
        .post(format!("{}/v1/execute", ctx.http))
        .json(&json!({"query": "CREATE NODE TABLE SnapSpec(id INT64, PRIMARY KEY(id))"}))
        .send()
        .await
        .unwrap();
    client
        .post(format!("{}/v1/execute", ctx.http))
        .json(&json!({"query": "CREATE (:SnapSpec {id: 1})"}))
        .send()
        .await
        .unwrap();

    // Snapshot 1: has 1 row
    let resp1 = client
        .post(format!("{}/v1/snapshot", ctx.http))
        .send()
        .await
        .unwrap();
    let body1: serde_json::Value = resp1.json().await.unwrap();
    let snap1_path = body1["rows"][0][1].as_str().unwrap().to_string();

    // Add more data
    client
        .post(format!("{}/v1/execute", ctx.http))
        .json(&json!({"query": "CREATE (:SnapSpec {id: 2})"}))
        .send()
        .await
        .unwrap();

    // Snapshot 2: has 2 rows
    let _resp2 = client
        .post(format!("{}/v1/snapshot", ctx.http))
        .send()
        .await
        .unwrap();

    flush_journal(&ctx.journal_tx);

    // Restore from snapshot 1 specifically (not latest)
    let restore_dir = tempfile::tempdir().unwrap();
    copy_dir_all(
        &ctx.data_dir.join("snapshots"),
        &restore_dir.path().join("snapshots"),
    );
    copy_dir_all(
        &ctx.data_dir.join("journal"),
        &restore_dir.path().join("journal"),
    );

    strana::snapshot::restore(
        restore_dir.path(),
        Some(std::path::Path::new(&snap1_path)),
    )
    .unwrap();

    // Should have all data: snapshot 1 (1 row) + journal replay (1 more row + CREATE TABLE)
    let verify_http = start_verify_server(restore_dir.path()).await;
    let rows = query_rows(
        &verify_http,
        "MATCH (n:SnapSpec) RETURN n.id ORDER BY n.id",
    )
    .await;
    // Restoring from snap1 + replaying journal entries after snap1's sequence
    // should recover row id=2 from the journal
    assert_eq!(
        rows.len(),
        2,
        "Expected 2 rows after restoring snap1 + journal replay, got {}",
        rows.len()
    );
}
