# Strana Roadmap

Strategic direction: graphd is an open-source graph database server for AI memory tools (Mem0, Graphiti, Cognee). Powered by LadybugDB (a Kuzu fork) with Cypher, vector HNSW, full-text search, and MERGE. Speaks Bolt protocol so neo4j-driver clients connect directly.

Key insight: Mem0 and Graphiti both already have Kuzu backends that emit compatible Cypher. We fork those backends to use neo4j-driver over Bolt pointing at graphd. No query translation — LadybugDB IS Kuzu.

## Phase 8-hardening: Production Hardening

Security, data safety, and concurrency fixes for Bolt + Neo4j HTTP consolidation.

- **Bolt auth enforcement**: Parse HELLO credentials, validate against TokenStore (fix: bolt4rs extra() accessor + bolt.rs handler)
- **Journal reliability**: Log errors on failed sends, detect writer thread crash via AtomicBool health flag
- **Transaction lock contention**: Wrap TransactionState in Arc, drop map lock before blocking channel operations
- **Transaction reaper**: Periodic cleanup of abandoned HTTP transactions (configurable TTL via --tx-timeout-secs)
- **Graceful shutdown**: SIGTERM/SIGINT handler, journal flush, axum with_graceful_shutdown, Bolt listener drain
- **Bolt version negotiation**: Parse client proposals, reject if 4.4 not offered
- **Bolt connection limits**: Semaphore-based limit (--bolt-max-connections)
- **RwLock poisoning resilience**: Replace .unwrap() with recovery pattern across all lock sites
- **Rewriter param prefix**: Rename generated params to __graphd_* prefix to avoid user collisions
- **Dead code cleanup**: Remove unused fields/functions flagged by compiler

## Phase 8: Bolt Server

Ship graphd with a production Bolt endpoint supporting Neo4j 4.4 and 5.x clients.

**Strategy:** Fork bolt-rs (community Bolt protocol library) and extend with Bolt 5.x support.

**Why bolt-rs over bolt4rs:**
- bolt4rs is experimental Neo4j Labs code, unpublished to crates.io, no maintenance
- bolt-rs is community-maintained, published, supports Bolt 1.0-4.4, last updated May 2024
- Forking bolt-rs unblocks publishing graphd to crates.io (no path dependencies)
- MPL-2.0 license is compatible with graphd's MIT license

**Timeline:** 3-4 weeks (migration + Bolt 5.x + integration + testing)

### 8a. Migration to bolt-rs + Bolt 5.x support

**Current state:** graphd uses bolt4rs (experimental Neo4j Labs project, unpublished, unmaintained).

**Target:** Fork [bolt-rs](https://github.com/0xSiO/bolt-rs) (community-maintained, published to crates.io, supports Bolt 1.0-4.4) and extend with Bolt 5.x protocol support.

bolt-rs provides:
- `bolt-proto`: PackStream serialization, Message/Value types, version negotiation
- `bolt-client`: Async client implementation (not needed, but shows protocol usage)
- Modular architecture: protocol primitives separate from transport
- Already published to crates.io (unblocks graphd publishing)

**Step 1: Migrate graphd from bolt4rs to bolt-rs (Bolt 4.4 only).**

**Timeline: 1-2 days** (bolt-rs has excellent helpers, migration is straightforward)

**Phase 1.1: Update dependencies (30 min)**

In `graphd/Cargo.toml`:
- Remove: `bolt4rs = { path = "../bolt4rs/lib", ... }`
- Add: `bolt-proto = "0.12.0"` (published crate, no fork needed yet)
- Add: `futures-util = "0.3"` (for async Message::from_stream)

**Phase 1.2: Update imports in src/bolt.rs (30 min)**

Replace:
```rust
use bolt4rs::bolt::response::success;
use bolt4rs::bolt::summary::{Success, Summary};
use bolt4rs::messages::{BoltRequest, BoltResponse, Record};
use bolt4rs::{BoltBoolean, BoltFloat, BoltInteger, BoltList, BoltMap, BoltNull, BoltString, BoltType};
```

With:
```rust
use bolt_proto::{Message, Value, ServerState};
use bolt_proto::message::{Hello, Run, Pull, Begin, Discard, Record, Success, Failure};
use std::collections::HashMap;
```

**Phase 1.3: Update message parsing (lines 130-138) (2 hours)**

Current (bolt4rs):
```rust
let req = match BoltRequest::parse(bolt4rs::Version::V4_4, msg_bytes) {
    Ok(r) => r,
    Err(e) => {
        warn!("Invalid Bolt message: {e}");
        break;
    }
};
```

New (bolt-rs):
```rust
// bolt-rs Message::from_stream is async, need to handle in blocking context
let msg = tokio::task::block_in_place(|| {
    tokio::runtime::Handle::current().block_on(async {
        use futures_util::io::Cursor;
        let mut cursor = Cursor::new(msg_bytes.as_ref());
        Message::from_stream(&mut cursor).await
    })
});
let msg = match msg {
    Ok(m) => m,
    Err(e) => {
        warn!("Invalid Bolt message: {e}");
        break;
    }
};
```

Alternative (simpler): Keep read_chunked_message async, change message loop to async.

**Phase 1.4: Update pattern matching (lines 263-512) (1 hour)**

Replace all `BoltRequest::` with `Message::`:
```rust
// OLD
match req {
    BoltRequest::Hello(hello) => { ... }
    BoltRequest::Run(run) => { ... }
    BoltRequest::Pull(pull) => { ... }
    // ...
}

// NEW (identical structure!)
match msg {
    Message::Hello(hello) => { ... }
    Message::Run(run) => { ... }
    Message::Pull(pull) => { ... }
    // ...
}
```

**Phase 1.5: Update HELLO handler (lines 265-301) (30 min)**

Extract credentials from metadata HashMap:
```rust
// OLD
match extract_credentials(hello.extra()) { ... }

// NEW
fn extract_credentials(metadata: &HashMap<String, Value>) -> Option<&str> {
    metadata.get("credentials")
        .and_then(|v| if let Value::String(s) = v { Some(s.as_str()) } else { None })
}
match extract_credentials(hello.metadata()) { ... }
```

Build SUCCESS response:
```rust
// OLD
let metadata = success::MetaBuilder::new()
    .server(&format!("Neo4j/5.x.0-graphd-{}", env!("CARGO_PKG_VERSION")))
    .connection_id("bolt-1")
    .build();
let summary = Summary::Success(Success { metadata });
vec![Bytes::from(summary.to_bytes().unwrap_or_default())]

// NEW
let mut metadata = HashMap::new();
metadata.insert("server".to_string(), Value::from(format!("Neo4j/5.x.0-graphd-{}", env!("CARGO_PKG_VERSION"))));
metadata.insert("connection_id".to_string(), Value::from("bolt-1"));
let success = Message::Success(Success::new(metadata));
success.into_chunks().unwrap_or_default().into_iter().flatten().collect()
```

**Phase 1.6: Update RUN handler (lines 304-356) (30 min)**

Access query and parameters:
```rust
// OLD
let query_str = run.query.to_string();
let json_params = bolt_params_to_json(&run.parameters);

// NEW (cleaner accessors!)
let query_str = run.query();
let json_params = bolt_params_to_json(run.parameters());
```

Build SUCCESS response with fields:
```rust
// OLD
let metadata = success::MetaBuilder::new()
    .fields(cols.into_iter())
    .build();

// NEW
let mut metadata = HashMap::new();
metadata.insert("fields".to_string(), Value::from(cols.into_iter().map(Value::from).collect::<Vec<_>>()));
let success = Message::Success(Success::new(metadata));
success.into_chunks()?.into_iter().flatten().collect()
```

**Phase 1.7: Update PULL handler (lines 359-403) (1 hour)**

Extract 'n' parameter:
```rust
// OLD
let max_records = pull.extra.get::<i64>("n").unwrap_or(1000) as usize;

// NEW
let max_records = pull.metadata()
    .get("n")
    .and_then(|v| if let Value::Integer(i) = v { Some(*i as usize) } else { None })
    .unwrap_or(1000);
```

Build RECORD responses:
```rust
// OLD
let values: Vec<BoltType> = row.iter()
    .map(|v| graph_value_to_bolt(&values::from_lbug_value(v)))
    .collect();
let record = Record { data: BoltList { value: values } };
let response = BoltResponse::Record(record);
responses.push(response.to_bytes()?);

// NEW
let values: Vec<Value> = row.iter()
    .map(|v| graph_value_to_bolt(&values::from_lbug_value(v)))
    .collect();
let record = Message::Record(Record::new(values));
for chunk in record.into_chunks()? {
    responses.push(chunk);
}
```

Build SUCCESS with has_more:
```rust
// OLD
let metadata = success::MetaBuilder::new()
    .done(!has_more)
    .has_more(has_more)
    .build();

// NEW
let mut metadata = HashMap::new();
metadata.insert("has_more".to_string(), Value::from(has_more));
let success = Message::Success(Success::new(metadata));
responses.extend(success.into_chunks()?);
```

**Phase 1.8: Update value conversion functions (lines 517-607) (1 hour)**

Replace `bolt_params_to_json` (simpler with HashMap):
```rust
// OLD
fn bolt_params_to_json(params: &BoltMap) -> Option<serde_json::Value> {
    if params.value.is_empty() { return None; }
    let mut map = serde_json::Map::new();
    for (k, v) in &params.value {
        map.insert(k.value.clone(), bolt_type_to_json(v));
    }
    Some(serde_json::Value::Object(map))
}

// NEW
fn bolt_params_to_json(params: &HashMap<String, Value>) -> Option<serde_json::Value> {
    if params.is_empty() { return None; }
    let mut map = serde_json::Map::new();
    for (k, v) in params {
        map.insert(k.clone(), bolt_value_to_json(v));
    }
    Some(serde_json::Value::Object(map))
}
```

Replace `bolt_type_to_json`:
```rust
// OLD
fn bolt_type_to_json(bolt: &BoltType) -> serde_json::Value {
    match bolt {
        BoltType::String(s) => serde_json::Value::String(s.value.clone()),
        BoltType::Integer(i) => serde_json::json!(i.value),
        // ...
    }
}

// NEW (simpler pattern matching!)
fn bolt_value_to_json(bolt: &Value) -> serde_json::Value {
    match bolt {
        Value::String(s) => serde_json::Value::String(s.clone()),
        Value::Integer(i) => serde_json::json!(i),
        Value::Float(f) => serde_json::json!(f),
        Value::Boolean(b) => serde_json::Value::Bool(*b),
        Value::Null => serde_json::Value::Null,
        Value::List(list) => serde_json::Value::Array(list.iter().map(bolt_value_to_json).collect()),
        Value::Map(map) => {
            let mut obj = serde_json::Map::new();
            for (k, v) in map {
                obj.insert(k.clone(), bolt_value_to_json(v));
            }
            serde_json::Value::Object(obj)
        }
        _ => serde_json::Value::Null, // DateTime etc.
    }
}
```

Replace `graph_value_to_bolt` (return Value instead of BoltType):
```rust
// OLD
fn graph_value_to_bolt(gv: &GraphValue) -> BoltType {
    match gv {
        GraphValue::Null => BoltType::Null(BoltNull {}),
        GraphValue::Bool(b) => BoltType::Boolean(BoltBoolean { value: *b }),
        GraphValue::Int(i) => BoltType::Integer(BoltInteger { value: *i }),
        // ...
    }
}

// NEW (much cleaner with From traits!)
fn graph_value_to_bolt(gv: &GraphValue) -> Value {
    match gv {
        GraphValue::Null => Value::Null,
        GraphValue::Bool(b) => Value::from(*b),
        GraphValue::Int(i) => Value::from(*i),
        GraphValue::Float(f) => Value::from(*f),
        GraphValue::String(s) => Value::from(s.clone()),
        GraphValue::List(items) => Value::from(items.iter().map(graph_value_to_bolt).collect::<Vec<_>>()),
        GraphValue::Map(entries) => {
            let map: HashMap<String, Value> = entries.iter()
                .map(|(k, v)| (k.clone(), graph_value_to_bolt(v)))
                .collect();
            Value::from(map)
        }
        GraphValue::Tagged(_tagged) => {
            // Serialize tagged values as JSON, convert to Value
            let json = serde_json::to_value(gv).unwrap_or(serde_json::Value::Null);
            json_to_bolt(&json)
        }
    }
}
```

**Phase 1.9: Update FAILURE responses (1 hour)**

Replace all `failure_bytes()` calls:
```rust
// OLD
fn failure_bytes(code: &str, message: &str) -> Bytes {
    let failure = bolt4rs::bolt::summary::Failure::new(code.to_string(), message.to_string());
    let summary: Summary<success::Meta> = Summary::Failure(failure);
    Bytes::from(summary.to_bytes().unwrap_or_default())
}

// NEW
fn failure_message(code: &str, message: &str) -> Vec<Bytes> {
    let mut metadata = HashMap::new();
    metadata.insert("code".to_string(), Value::from(code));
    metadata.insert("message".to_string(), Value::from(message));
    let failure = Message::Failure(Failure::new(metadata));
    failure.into_chunks().unwrap_or_default()
}
```

Update all call sites:
```rust
// OLD
vec![failure_bytes("Neo.ClientError.Security.Unauthorized", "Invalid credentials")]

// NEW
failure_message("Neo.ClientError.Security.Unauthorized", "Invalid credentials")
```

**Phase 1.10: Update unit tests (lines 705-1066) (1 hour)**

Replace all test helper types:
```rust
// OLD
BoltType::String(BoltString { value: "hello".into() })
BoltType::Integer(BoltInteger { value: 42 })

// NEW
Value::from("hello")
Value::from(42i64)
```

Update assertions:
```rust
// OLD
match graph_value_to_bolt(&gv) {
    BoltType::String(s) => assert_eq!(s.value, "hello"),
    other => panic!("Expected String, got {other:?}"),
}

// NEW
match graph_value_to_bolt(&gv) {
    Value::String(s) => assert_eq!(s, "hello"),
    other => panic!("Expected String, got {other:?}"),
}
```

**Phase 1.11: Test and verify (2-3 hours)**

Run tests:
```bash
cd /Users/russellromney/Documents/Github/strana
make test          # Unit + integration tests
make e2e-py        # Python driver tests
make e2e-js        # JavaScript driver tests
```

Test manually:
```bash
make up            # Start graphd
# In another terminal:
python3 tests/e2e/test_e2e.py -v
```

**Phase 1.12: Cleanup (30 min)**

- Remove unused bolt4rs imports
- Remove `../bolt4rs` directory reference
- Update comments referencing bolt4rs
- Run `cargo fmt` and `cargo clippy`

**Step 2: Fork bolt-rs and add Bolt 5.x support (FUTURE WORK).**

**Only do this after Step 1 is complete and tested.** Step 1 migrates to bolt-rs using existing 4.4 support from crates.io (no fork needed).

**Timeline: 1-2 weeks** (protocol extension + testing + publishing)

**Phase 2.1: Fork bolt-rs (1 hour)**

```bash
cd /Users/russellromney/Documents/Github
# Already cloned at /Users/russellromney/Documents/Github/bolt-rs
cd bolt-rs
gh repo fork --remote  # Fork to your GitHub account
git remote -v          # Verify: origin -> your fork, upstream -> 0xSiO/bolt-rs
git checkout -b bolt-5-support
```

Update metadata in all Cargo.toml files:
- `bolt-proto/Cargo.toml`: Update author, repository URL, version to 0.8.0
- `bolt-client/Cargo.toml`: Same
- Root `Cargo.toml`: Update workspace metadata

**Phase 2.2: Add Bolt 5.0 message types (2-3 days)**

Create new message files following existing pattern:

`bolt-proto/src/message/logon.rs`:
```rust
use std::collections::HashMap;
use bolt_proto_derive::*;
use crate::{impl_message_with_metadata, impl_try_from_message, Value};

pub(crate) const SIGNATURE_LOGON: u8 = 0x6A;

#[bolt_structure(SIGNATURE_LOGON)]
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct Logon {
    pub(crate) metadata: HashMap<String, Value>,
}

impl_message_with_metadata!(Logon);
impl_try_from_message!(Logon, Logon);
```

`bolt-proto/src/message/logoff.rs`:
```rust
use bolt_proto_derive::*;
use crate::{impl_try_from_message, impl_empty_message_tests};

pub(crate) const SIGNATURE_LOGOFF: u8 = 0x6B;

#[bolt_structure(SIGNATURE_LOGOFF)]
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub struct Logoff;

impl_try_from_message!(Logoff, Logoff);
impl_empty_message_tests!(Logoff);
```

Add to `bolt-proto/src/message.rs`:
```rust
pub use logon::Logon;
pub use logoff::Logoff;

pub(crate) mod logon;
pub(crate) mod logoff;
pub(crate) const SIGNATURE_LOGON: u8 = 0x6A;
pub(crate) const SIGNATURE_LOGOFF: u8 = 0x6B;

// Add to Message enum
pub enum Message {
    // ... existing variants

    // v5.0-compatible message types
    Logon(Logon),
    Logoff,
}
```

Add deserialization/serialization to `Message::deserialize()` and `Message::marker()`.

**Phase 2.3: Add Bolt 5.1 TELEMETRY (1 day)**

`bolt-proto/src/message/telemetry.rs`:
```rust
pub(crate) const SIGNATURE_TELEMETRY: u8 = 0x54;

#[bolt_structure(SIGNATURE_TELEMETRY)]
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct Telemetry {
    pub(crate) api: i64,  // API version identifier
}

impl Telemetry {
    pub fn new(api: i64) -> Self {
        Self { api }
    }

    pub fn api(&self) -> i64 {
        self.api
    }
}
```

**Phase 2.4: Update version negotiation (1 day)**

Add to `bolt-proto/src/version.rs`:
```rust
pub const V5_0: (u8, u8) = (5, 0);
pub const V5_1: (u8, u8) = (5, 1);
pub const V5_2: (u8, u8) = (5, 2);
pub const V5_3: (u8, u8) = (5, 3);
pub const V5_4: (u8, u8) = (5, 4);
```

Update graphd's handshake code to advertise 5.x:
```rust
// In src/bolt.rs, negotiate_bolt_version()
fn negotiate_bolt_version(versions: &[u8; 16]) -> Option<(u8, u8)> {
    for i in 0..4 {
        let offset = i * 4;
        let major = versions[offset + 3];
        let minor = versions[offset + 2];
        let range = versions[offset + 1];

        // Check for 5.x support
        if major == 5 {
            if minor >= 4 && (minor - range) <= 4 { return Some((5, 4)); }
            if minor >= 3 && (minor - range) <= 3 { return Some((5, 3)); }
            if minor >= 2 && (minor - range) <= 2 { return Some((5, 2)); }
            if minor >= 1 && (minor - range) <= 1 { return Some((5, 1)); }
            if minor >= 0 && (minor - range) <= 0 { return Some((5, 0)); }
        }

        // Check for 4.4 support (fallback)
        if major == 4 && minor >= 4 && (minor - range) <= 4 {
            return Some((4, 4));
        }
    }
    None
}
```

Return negotiated version to client:
```rust
// Write back negotiated version instead of hardcoded 4.4
match negotiate_bolt_version(&versions) {
    Some((major, minor)) => {
        stream.write_all(&[0x00, 0x00, minor, major]).await?;
        (major, minor)
    }
    None => {
        stream.write_all(&[0x00, 0x00, 0x00, 0x00]).await?;
        return Err("No supported Bolt version".into());
    }
}
```

**Phase 2.5: Add version-aware message routing in graphd (2-3 days)**

Track negotiated version in connection state:
```rust
// In bolt_session_worker
let negotiated_version = /* from handshake */;

while let Some(BoltOp::Request(msg)) = rx.blocking_recv() {
    let responses = match negotiated_version {
        (5, _) => handle_bolt_5_message(&msg, ...),
        (4, 4) => handle_bolt_4_message(&msg, ...),
        _ => vec![failure_message("Neo.ClientError.Request.Invalid", "Unsupported version")],
    };
    // ...
}
```

Implement `handle_bolt_5_message`:
```rust
fn handle_bolt_5_message(
    msg: &Message,
    conn: &lbug::Connection,
    tokens: &Arc<TokenStore>,
    authenticated: &mut bool,
    // ... other state
) -> Vec<Bytes> {
    match msg {
        // Bolt 5.0: HELLO no longer has auth, just server info
        Message::Hello(_hello) => {
            let mut metadata = HashMap::new();
            metadata.insert("server".to_string(), Value::from("Neo4j/5.26.0"));
            Message::Success(Success::new(metadata)).into_chunks().unwrap_or_default()
        }

        // Bolt 5.0: LOGON for authentication
        Message::Logon(logon) => {
            if tokens.is_empty() {
                *authenticated = true;
            } else {
                match extract_credentials(logon.metadata()) {
                    Some(token) => match tokens.validate(token) {
                        Ok(_) => *authenticated = true,
                        Err(_) => return failure_message("Neo.ClientError.Security.Unauthorized", "Invalid credentials"),
                    },
                    None => return failure_message("Neo.ClientError.Security.Unauthorized", "Credentials required"),
                }
            }
            Message::Success(Success::new(HashMap::new())).into_chunks().unwrap_or_default()
        }

        // Bolt 5.0: LOGOFF
        Message::Logoff => {
            *authenticated = false;
            Message::Success(Success::new(HashMap::new())).into_chunks().unwrap_or_default()
        }

        // Bolt 5.1: TELEMETRY (just acknowledge, ignore metrics)
        Message::Telemetry(_) => {
            Message::Success(Success::new(HashMap::new())).into_chunks().unwrap_or_default()
        }

        // All other messages (RUN, PULL, BEGIN, etc.) work the same
        _ => handle_bolt_4_message(msg, conn, tokens, authenticated, /* ... */),
    }
}
```

**Phase 2.6: Add feature flags (1 hour)**

Update `bolt-proto/Cargo.toml`:
```toml
[features]
default = ["bolt-4"]
bolt-4 = []
bolt-5 = ["bolt-4"]
```

Conditionally compile 5.x message types:
```rust
#[cfg(feature = "bolt-5")]
pub use logon::Logon;
#[cfg(feature = "bolt-5")]
pub use logoff::Logoff;
```

**Phase 2.7: Test Bolt 5.x support (2-3 days)**

Add unit tests in `bolt-proto/src/message/`:
```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn logon_serialize_deserialize() {
        let mut metadata = HashMap::new();
        metadata.insert("scheme".to_string(), Value::from("basic"));
        metadata.insert("principal".to_string(), Value::from("neo4j"));
        metadata.insert("credentials".to_string(), Value::from("password"));

        let logon = Logon::new(metadata.clone());
        let bytes = Message::Logon(logon.clone()).into_chunks().unwrap();
        // ... test round-trip
    }
}
```

Test against Neo4j 5.x:
```bash
# Start Neo4j 5.x container
docker run -d --name neo4j5 -p 7688:7687 neo4j:5.26.0

# Update e2e tests to connect with 5.x
cd tests/e2e
# Update test_e2e.py to use bolt://localhost:7688
python3 test_e2e.py -v

# Clean up
docker rm -f neo4j5
```

**Step 3: Build graphd's Bolt server on top of bolt-rs.**

Production features to build on top of bolt-rs primitives:

**Authentication:**
- Bolt 4.x: Map HELLO credentials to graphd's SHA-256 token auth — `credentials` field → token, ignore `principal`
- Bolt 5.1+: Handle LOGON message after HELLO — same token validation, different message sequence
- Open access mode when no tokens configured (same as today)

**Transactions:**
- Wire Bolt BEGIN/COMMIT/ROLLBACK to LadybugDB's native transaction support
- Track connection state (READY/STREAMING/TX_READY/TX_STREAMING/FAILED)
- Auto-rollback on disconnect

**Graph type mapping:**
- bolt-rs provides basic types (Bool, Int64, Float64, Utf8)
- Add Node → Bolt Node structure (id, labels, properties)
- Add Rel → Bolt Relationship structure (id, start_id, end_id, type, properties)
- Add Path → Bolt Path structure (nodes, rels, indices)
- Map LadybugDB Arrow types → bolt-rs Value types

**Parameterized queries:**
- Map Bolt parameter dictionaries to LadybugDB PreparedStatement API
- BoltValue → lbug Value conversion layer

**Streaming:**
- RUN returns column names in SUCCESS metadata (no rows)
- PULL {n} returns N records + has_more, or all records if n=-1
- DISCARD {n} skips records

**Server identity spoofing:**
- Return `"server": "Neo4j/5.26.0"` in HELLO SUCCESS metadata
- neo4j drivers reject connections from non-Neo4j servers (Memgraph does this too)

**TLS (bolt+s://):**
- Wrap TCP acceptor with tokio-rustls before handing to bolt-rs
- Accept both plain (`bolt://`) and TLS (`bolt+s://`)
- `--bolt-tls-cert` and `--bolt-tls-key` flags for PEM cert/key
- In cloud deployments, TLS termination can happen at the proxy layer

**Config:** `--bolt-port 7687`, `--bolt-listen 0.0.0.0`, `--bolt-tls-cert`, `--bolt-tls-key`

**Step 4: Publish bolt-rs fork to crates.io.**

Once Bolt 5.x support is stable and tested in graphd:
- Update version in bolt-proto and bolt-client Cargo.toml files (e.g., 0.8.0)
- Update repository URLs to point to your fork
- Add clear attribution in README: "Fork of 0xSiO/bolt-rs with Bolt 5.x support"
- Publish: `cd bolt-proto && cargo publish`, then `cd bolt-client && cargo publish`
- Update graphd Cargo.toml to use published crates instead of git dependency
- This unblocks publishing graphd to crates.io

**Step 5: Contribute upstream (optional).**

After proven in production:
- Submit PR to https://github.com/0xSiO/bolt-rs with Bolt 5.x additions
- Maintain your fork if upstream is inactive
- Keep MPL-2.0 license (compatible with graphd's MIT)

### 8b. Infrastructure integration

Carry forward from existing graphd:
- Write-ahead journal for mutation replay
- Snapshot + S3 backup/restore
- HTTP endpoints for admin: `POST /v1/snapshot`, `GET /health`
- Query rewriter for non-deterministic functions (gen_random_uuid, timestamps)
- CLI flags for journal, snapshot, retention, S3

### Tests

- Handshake + version negotiation (Bolt 4.4 and 5.x)
- HELLO with valid/invalid token (4.x flow)
- LOGON/LOGOFF auth flow (5.1+ flow)
- RUN + PULL simple query, parameterized query
- BEGIN/COMMIT/ROLLBACK transaction lifecycle
- RESET from FAILED state
- GOODBYE clean shutdown
- Streaming: PULL with n, DISCARD
- Graph types: Node, Rel, Path through Bolt encode/decode
- TLS: bolt+s:// connection with self-signed cert, plain bolt:// still works
- Neo4j Python driver integration: `from neo4j import GraphDatabase; driver.execute_query("MATCH ...")`
- DDL over Bolt: CREATE NODE TABLE, CREATE REL TABLE

## Phase 9: GraphJ Replication Integration Tests

Comprehensive test suite proving logical replication works correctly across crash recovery, schema evolution, compression, encryption, and S3 operations.

**Goal:** High confidence that journal replay produces bit-identical database state. Critical for backup/restore, point-in-time recovery, and future read replicas.

### 9a. Determinism verification

**Core principle:** Same journal sequence → same final state, every time.

Verify that replaying a journal to an empty database produces bit-identical state to the original. Run identical workloads, compare resulting database files byte-for-byte. Test with multiple replay runs to ensure no hidden non-determinism.

Key scenarios:
- Replay produces identical database state (file-level comparison)
- Parameter ordering doesn't affect journal entries
- Concurrent writes serialize deterministically
- Repeated replays from same journal yield identical results

### 9b. Crash recovery

Verify journal recovers correct state after process crashes at various points in the write/commit cycle.

Kill graphd during operations and verify recovery on restart:
- Unsealed segment with partial writes recovers all committed data
- Uncommitted transactions disappear on crash (ACID durability)
- Fsync boundaries are honored (data not fsynced = data lost)
- Partial journal entries are detected and skipped safely
- Segment rotation mid-crash leaves consistent sealed/unsealed pair

### 9c. Schema evolution

Verify DDL operations (CREATE/DROP/ALTER TABLE) replay in correct order and produce correct final schema.

Test schema changes interleaved with data operations:
- Table creation followed by inserts replays correctly
- Relationship tables with foreign key constraints enforce referential integrity on replay
- DROP TABLE removes old data, subsequent CREATE with same name starts fresh
- Column additions (ALTER) preserve existing data, new columns appear
- Indexes rebuild correctly on replay

### 9d. Compression & encryption

Verify sealed segments with compression and/or encryption replay correctly and detect tampering.

Test compaction with various protection levels:
- Zstd-compressed segments decompress and replay correctly
- Encrypted segments decrypt with correct key, fail without key
- Header tampering (AAD) detected by AEAD verification
- Body tampering detected by checksums or decryption failure
- Multiple compression levels produce valid segments (size varies, data identical)

### 9e. S3 backup/restore

Verify complete snapshot → S3 → restore workflow preserves data correctly.

Test end-to-end S3 operations:
- Snapshot uploads to S3 with correct naming and metadata
- Restore from S3 downloads and unpacks correctly, all data present
- Incremental snapshots represent point-in-time state accurately
- Compacted journal segments restore without raw segments
- Retention policies delete old snapshots, keep recent ones

Uses Tigris for testing (S3-compatible, no Docker).

### 9f. Point-in-time recovery

Verify ability to restore database to any historical sequence number.

Test partial replay scenarios:
- Replay stops at specified sequence, database contains only writes up to that point
- Multi-segment replay stops mid-segment correctly
- Snapshot + partial journal replay combines correctly (base state + incremental)

### 9g. Corruption detection

Verify all integrity checks (CRC32C, SHA-256 chain, body checksum, AEAD) detect tampering.

Test various corruption scenarios:
- Entry-level corruption detected by CRC32C, bad entries skipped safely
- Chain hash mismatches detected, segment rejected
- Sealed segment body checksum mismatches abort replay
- AEAD authentication detects header tampering
- Magic byte corruption triggers legacy fallback or rejection

### 9h. Large-scale stress

Verify replication scales to production workloads without performance or memory issues.

Test realistic scale:
- 100K+ nodes and relationships replay correctly
- Deep graph structures (1000+ hop paths) maintain integrity
- Concurrent reads work against partially-replayed state
- Memory-bounded replay doesn't OOM, uses streaming correctly

### 9i. Non-determinism regression

Verify query rewriter catches all non-deterministic functions before they reach the journal.

Test function rewriting:
- Timestamp functions replaced with constants in journal
- UUID generation produces same IDs on replay
- Random values deterministic on replay
- Unsupported non-deterministic functions rejected with error (not silently logged)

### Tests summary

**Total: ~40-50 integration tests organized into 9 test modules.**

Run all:
```bash
make test-graphj          # All GraphJ tests
make test-graphj-quick    # Skip slow stress tests
```

### Acceptance criteria

All tests pass before merging any GraphJ-dependent feature (Mem0, Graphiti, cloud deployments). CI runs full suite on every PR.

## Phase 10: Mem0 Integration

Fork Mem0's Kuzu backend (`mem0/memory/kuzu_memory.py`), replace `kuzu` SDK with `neo4j` driver over Bolt. Same Cypher, remote transport.

### 10a. Parameterized queries via bolt-rs

Mem0 passes parameters for every query. graphd's Bolt server must map Bolt parameter dictionaries to LadybugDB PreparedStatement API.

- In the RUN handler, extract Bolt parameter map from message
- Convert bolt-rs Value types → lbug Value types:
  - BoltString→String, BoltInteger→Int64, BoltFloat→Float64, BoltBoolean→Bool, BoltList→List, BoltNull→Null
- Pass converted parameters to `connection.prepare(query).execute(params)`
- For FLOAT[] parameters (embeddings), Mem0 sends Python lists of floats — these arrive as BoltList of BoltFloat, convert to lbug's list-of-float type

### 10b. INTERNAL_ID serialization

Mem0's Kuzu backend uses `id(node)` to get node IDs, then passes them back via `internal_id($table, $offset)` for lookups.

- In the Arrow-to-Bolt conversion in the PULL handler, detect INTERNAL_ID type (Arrow Struct with `table` and `offset` fields)
- Serialize as a Bolt Map: `{"table": int, "offset": int}` using bolt-rs Value::Map
- On the Python side, `result["id"]` will be a dict `{"table": 0, "offset": 5}` — matching what kuzu SDK returns
- The graphd backend can use `result["id"]["table"]` and `result["id"]["offset"]` with zero changes from the Kuzu backend's access pattern

### 10c. Remaining Arrow type mappings

Mem0 queries return types beyond Bool/Int64/Float64/Utf8:

- TIMESTAMP → bolt-rs Value::Integer (epoch millis) or Value::String (ISO format) — match what neo4j driver expects
- LIST of FLOAT (embeddings) → Value::List of Value::Float
- LIST of STRING → Value::List of Value::String
- STRUCT (general) → Value::Map
- NULL values in any column → Value::Null (check for nulls before downcasting)

### 10d. graphd backend for Mem0

New file: `mem0/memory/graphd_memory.py`. Fork of `kuzu_memory.py` with transport swap.

**Connection setup:**
- Replace `kuzu.Database(path)` + `kuzu.Connection(db)` with `neo4j.GraphDatabase.driver(url, auth=("", token))`
- Config class: `GraphdConfig` with `url` (bolt:// endpoint) and `token` fields

**Query execution:**
- Replace `self.graph.execute(query, params)` → `self.driver.execute_query(query, params)`
- Replace `list(results.rows_as_dict())` → `[dict(record) for record in records]`
- neo4j-driver returns `(records, summary, keys)` tuple from `execute_query`

**Schema creation:**
- Same DDL queries (`CREATE NODE TABLE IF NOT EXISTS ...`)
- Run via `execute_query` instead of `conn.execute`

**ID handling:**
- `id(node)` returns `{"table": int, "offset": int}` as a dict (thanks to 9b)
- Access pattern `result["id"]["table"]` and `result["id"]["offset"]` stays identical
- `internal_id($table_id, $offset_id)` queries work unchanged

**Everything else:**
- All Cypher stays identical — MERGE, MATCH, DETACH DELETE, array_cosine_similarity, CAST, current_timestamp, ORDER BY, LIMIT
- LLM calls, embedding calls, BM25 reranking — unchanged, these don't touch the database
- `rank_bm25` dependency stays

**Registration:**
- Add `GraphdConfig` to `mem0/graphs/configs.py`
- Add `"graphd": "mem0.memory.graphd_memory.MemoryGraph"` to factory in `mem0/utils/factory.py`
- Add config validation for provider="graphd"

### 10e. End-to-end test

- Start graphd locally (bolt-rs server with lbug)
- Configure Mem0: `graph_store: {provider: "graphd", config: {url: "bolt://localhost:7687"}}`
- Run each Mem0 graph operation and verify:
  - `add()` — creates Entity nodes and CONNECTED_TO relationships, embeddings stored
  - `search()` — vector similarity search returns ranked results
  - `get_all()` — retrieves all relationships for a user
  - `delete_all()` — removes all nodes for a user
  - `reset()` — clears the graph
- Verify parameterized queries work for all parameter types: strings, ints, floats, float arrays (embeddings)
- Verify ID round-trip: add entities, search returns IDs, subsequent queries use those IDs to find/update nodes

### Tests

- Parameterized query: string, int, float, list-of-float params through Bolt
- INTERNAL_ID round-trip: `RETURN id(n)` → dict → `internal_id($table, $offset)` lookup
- TIMESTAMP serialization: `current_timestamp()` result readable by neo4j driver
- LIST serialization: embedding vectors round-trip through Bolt
- Mem0 add/search/get_all/delete_all/reset against graphd
- Vector similarity: `array_cosine_similarity()` with CAST'd float arrays returns correct ordering

## Phase 11: Graphiti Integration

Same approach as Mem0. Fork Graphiti's Kuzu backend, swap kuzu SDK for neo4j-driver.

- Graphiti's Kuzu backend: `graphiti_core/llm_client/kuzu_client.py` + `graphiti_core/graphiti_kuzu.py`
- Models edges as intermediate nodes (`RelatesToNode_`) — already LadybugDB-compatible
- PR upstream to Graphiti

**Cypher used by Graphiti's Kuzu backend** (all native to LadybugDB):
- `CREATE NODE TABLE`, `CREATE REL TABLE` for schema
- Individual SET: `SET n.name = $name, n.summary = $summary`
- `array_cosine_similarity()` for vector search
- `QUERY_FTS_INDEX` for full-text search
- `CAST()`, `list_concat()`

## Phase 13: Replica Mode

Enable graphd to run as a read-only replica that polls cloud storage for updates.

**Use case:** Scale reads horizontally. Primary writes to cloud storage (S3/Tigris). Replicas poll for new snapshots and journal segments, replay locally, serve read-only queries over Bolt.

### Configuration

```bash
graphd --replica \
  --replica-source s3://bucket/prefix \
  --replica-poll-interval 5s \
  --data-dir ./replica-db
```

Flags:
- `--replica`: Enable read-only replica mode (disables writes)
- `--replica-source <url>`: S3 bucket/prefix or local path to poll
- `--replica-poll-interval <duration>`: How often to check for updates (default: 10s)
- `--replica-lag-warn <duration>`: Warn if replica falls behind by this much (default: 60s)

### Behavior

**Initial sync:**
- On startup, download latest snapshot from replica source
- Extract to local data directory
- Download all journal segments newer than snapshot
- Replay journal to catch up to latest sequence

**Continuous polling:**
- Every poll interval, list objects at replica source
- Download new journal segments not yet applied
- Replay segments in sequence order
- Update local watermark (last applied sequence number)
- Serve read queries from local replayed state

**Read-only enforcement:**
- Reject all write operations (CREATE, SET, DELETE, BEGIN TRANSACTION)
- Allow read operations (MATCH, RETURN)
- Return error on write: "Neo.ClientError.Request.Invalid: Replica is read-only"

**Lag monitoring:**
- Track time since last successful poll
- Track sequence number lag (latest cloud sequence - local sequence)
- Expose metrics: replica_lag_seconds, replica_last_poll_timestamp, replica_current_sequence
- Log warning if lag exceeds threshold

**Failure handling:**
- Network failures: retry with exponential backoff
- Corrupt segments: skip, log error, continue with next segment
- Missing segments: fatal error (gap in replication log)

### Storage layout

Replica source must have:
```
s3://bucket/prefix/
  snapshots/
    snapshot-2024-01-15-120000.tar.zst
    snapshot-2024-01-15-130000.tar.zst
  journal/
    journal-0000000000000000.graphj
    journal-0000000000001000.graphj
    journal-0000000000002000.graphj
```

### Multi-region replicas

Run replicas in multiple regions for low-latency reads:
- Primary in us-east-1 writes to S3
- Replica in eu-west-1 polls S3, serves European reads
- Replica in ap-southeast-1 polls S3, serves Asian reads
- All replicas eventually consistent (bounded by poll interval + network latency)

### Testing

Integration tests:
- Primary writes data, replica polls and sees new data
- Replica rejects write operations
- Network failure recovery (primary continues writing, replica catches up when network returns)
- Snapshot rotation (replica downloads new snapshot, replays incremental journal)
- Lag monitoring and warnings

## Later

- Embedded SDKs (strana-python via pyo3+maturin, strana-node via koffi FFI)
- Neo4j Cypher compatibility layer (query rewriter for Neo4j-dialect Cypher)
- Schema inference (auto-DDL on MERGE into nonexistent table)
- MCP server (Model Context Protocol) for direct LLM ↔ graph interaction
- Cognee verification (third AI memory framework)
- APOC-compatible procedure library (most-used subset)
- Bolt 5.5+ features (routing, multi-database) if needed for cloud deployments
- zstd dictionary training for journal compression (5-10% additional compression on Cypher patterns, requires dictionary versioning and distribution)
