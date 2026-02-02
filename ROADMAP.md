# Strana Roadmap

Strategic direction: graphd is a graph database server for AI memory tools (Mem0, Graphiti, Cognee). Powered by LadybugDB (a Kuzu fork) with Cypher, vector HNSW, full-text search, and MERGE. Speaks Bolt protocol so neo4j-driver clients connect directly.

Key insight: Mem0 and Graphiti both already have Kuzu backends that emit compatible Cypher. We fork those backends to use neo4j-driver over Bolt pointing at graphd. No query translation — LadybugDB IS Kuzu.

Cloud-first: Cinch hosts graphd as a managed service. Open source later.

## Phase 8: Bolt Server

Ship graphd with a production Bolt endpoint.

### 8a. Bolt protocol (using bolt4rs)

LadybugDB's `bolt4rs` crate (github.com/LadybugDB/bolt4rs) provides:
- PackStream serialization, all Bolt 4.x message types, chunked TCP framing
- A reference server implementation (~340 lines) with handshake, RUN, PULL working

**Step 1: Patch bolt4rs with Bolt 5.x protocol support.**

Fork/patch bolt4rs (via git dependency or workspace member). bolt4rs currently implements Bolt 4.0–4.4. Add 5.x support:
- Extend `Version` enum: `V5_0`, `V5_1`, `V5_3`, `V5_4` (enum is already `#[non_exhaustive]`)
- Add message types: `LOGON` (0x6A), `LOGOFF` (0x6B), `TELEMETRY` (0x54)
- Update `add_supported_versions()` to advertise 5.x in handshake
- 5.0: Element IDs as strings (not integers)
- 5.1: LOGON/LOGOFF — auth moves out of HELLO into separate message
- 5.2: Notification control fields in HELLO/BEGIN/RUN metadata
- 5.3: `bolt_agent` metadata in HELLO
- 5.4: TELEMETRY message
- 5.7+: GQL-formatted FAILURE responses (`gql_status`, `description`, `diagnostic_record`)
- Version-aware server reference: auth flow branches on negotiated version (4.x HELLO vs 5.1+ LOGON)

The hard protocol work (PackStream, framing) is already done — 5.x is new message types on the same wire format. Upstream PR later once proven in graphd.

**Step 2: Build graphd's Bolt server on top of bolt4rs.**

Production features beyond what bolt4rs provides:

**Authentication:**
- Bolt 4.x: Map HELLO credentials to graphd's SHA-256 token auth — `credentials` field → token, ignore `principal`
- Bolt 5.1+: Handle LOGON message after HELLO — same token validation, different message sequence
- Open access mode when no tokens configured (same as today)

**Transactions:**
- Wire Bolt BEGIN/COMMIT/ROLLBACK to LadybugDB's native transaction support
- Track connection state (READY/STREAMING/TX_READY/TX_STREAMING/FAILED)
- Auto-rollback on disconnect

**Graph type mapping:**
- bolt4rs already maps basic types (Bool, Int64, Float64, Utf8)
- Add Node → Bolt Node structure (id, labels, properties)
- Add Rel → Bolt Relationship structure (id, start_id, end_id, type, properties)
- Add Path → Bolt Path structure (nodes, rels, indices)

**Proper parameterized queries:**
- Replace bolt4rs server's naive string substitution with LadybugDB's PreparedStatement API

**Streaming:**
- RUN returns column names in SUCCESS metadata (no rows)
- PULL {n} returns N records + has_more, or all records if n=-1
- DISCARD {n} skips records

**Server identity spoofing:**
- Return `"server": "Neo4j/5.26.0"` in HELLO SUCCESS metadata
- neo4j drivers reject connections from non-Neo4j servers (Memgraph does this too)

**TLS (bolt+s://):**
- Wrap TCP acceptor with tokio-rustls before handing to bolt4rs
- Accept both plain (`bolt://`) and TLS (`bolt+s://`)
- `--bolt-tls-cert` and `--bolt-tls-key` flags for PEM cert/key
- In Cinch cloud, TLS termination happens at the proxy layer

**Config:** `--bolt-port 7687`, `--bolt-listen 0.0.0.0`, `--bolt-tls-cert`, `--bolt-tls-key`

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

## Phase 9: Mem0 Integration

Fork Mem0's Kuzu backend (`mem0/memory/kuzu_memory.py`), replace `kuzu` SDK with `neo4j` driver over Bolt. Same Cypher, remote transport.

### 9a. bolt4rs: parameterized queries

bolt4rs reference server currently does naive string substitution for parameters (`$key` → `value.to_string()`). This is broken — integers render as "to_string not implemented", strings don't get quoted. Mem0 passes parameters for every query.

- Remove the string substitution code in the RUN handler
- Instead, pass the Bolt parameter map through to LadybugDB's PreparedStatement API
- BoltType → lbug Value conversion: BoltString→String, BoltInteger→Int64, BoltFloat→Float64, BoltBoolean→Bool, BoltList→List, BoltNull→Null
- For FLOAT[] parameters (embeddings), Mem0 sends Python lists of floats — these arrive as BoltList of BoltFloat, need to convert to lbug's list-of-float type

### 9b. bolt4rs: INTERNAL_ID serialization

Mem0's Kuzu backend uses `id(node)` to get node IDs, then passes them back via `internal_id($table, $offset)` for lookups. Currently bolt4rs maps unknown Arrow types to BoltNull, so IDs are lost.

- In the Arrow-to-Bolt conversion in the PULL handler, detect INTERNAL_ID type (Arrow Struct with `table` and `offset` fields)
- Serialize as a Bolt Map: `{"table": int, "offset": int}`
- On the Python side, `result["id"]` will be a dict `{"table": 0, "offset": 5}` — matching what kuzu SDK returns
- The Strana backend can use `result["id"]["table"]` and `result["id"]["offset"]` with zero changes from the Kuzu backend's access pattern

### 9c. bolt4rs: remaining Arrow type mappings

Mem0 queries return types beyond Bool/Int64/Float64/Utf8:

- TIMESTAMP → BoltInteger (epoch millis) or BoltString (ISO format) — match what neo4j driver expects
- LIST of FLOAT (embeddings) → BoltList of BoltFloat
- LIST of STRING → BoltList of BoltString
- STRUCT (general) → BoltMap
- NULL values in any column → BoltNull (check for nulls before downcasting)

### 9d. Strana backend for Mem0

New file: `mem0/memory/strana_memory.py`. Fork of `kuzu_memory.py` with transport swap.

**Connection setup:**
- Replace `kuzu.Database(path)` + `kuzu.Connection(db)` with `neo4j.GraphDatabase.driver(url, auth=("", token))`
- Config class: `StranaConfig` with `url` (bolt:// endpoint) and `token` fields

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
- Add `StranaConfig` to `mem0/graphs/configs.py`
- Add `"strana": "mem0.memory.strana_memory.MemoryGraph"` to factory in `mem0/utils/factory.py`
- Add config validation for provider="strana"

### 9e. End-to-end test

- Start graphd locally (bolt4rs server with lbug)
- Configure Mem0: `graph_store: {provider: "strana", config: {url: "bolt://localhost:7687"}}`
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

## Phase 10: Graphiti Integration

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

## Phase 11: Cinch Graph Engine

Add graph as a second engine type in cinch-cloud (alongside Redis cache).

- Per-tenant graphd instance (same model as per-tenant Redis)
- Bolt proxy: route by auth token → tenant DB
- TLS termination at proxy
- S3 snapshots for graph data (tar+zstd)
- Dashboard: create/delete graph databases, view schema, run queries
- Pricing: per-tenant, metered by storage + query volume

## Later

- Open source graphd
- Embedded SDKs (strana-python via pyo3+maturin, strana-node via koffi FFI)
- Neo4j Cypher compatibility layer (query rewriter for Neo4j-dialect Cypher)
- Schema inference (auto-DDL on MERGE into nonexistent table)
- Embedded replicas (read replicas synced from cloud via S3 snapshots)
- MCP server (Model Context Protocol) for direct LLM ↔ graph interaction
- Cognee verification (third AI memory framework)
- APOC-compatible procedure library (most-used subset)
- Bolt 5.5+ features (routing, multi-database) if needed for cloud proxy
- Encryption at rest (AES-256-GCM on journal segments and snapshots)
