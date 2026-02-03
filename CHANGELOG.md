# Changelog

## Phase 8: GraphJ File Format

- **Universal journal format** (`src/graphj.rs`): `.graphj` container format replacing raw `.wal` files. 128-byte fixed header + variable body. Supports live segments (unsealed, raw), compacted archives (sealed, optionally compressed/encrypted), and S3 uploads.
- **Built-in compression**: zstd compression (configurable level, default 3) for sealed segments. Flag: `--journal-compress`, `--journal-compress-level`.
- **Built-in encryption**: XChaCha20-Poly1305 AEAD encryption for sealed segments with 32-byte keys. AEAD AAD binds ciphertext to header metadata (first 40 bytes). Flag: `--journal-encryption-key` (64-char hex).
- **Backward compatibility**: Legacy `.wal` files detected by magic byte check. Reader transparently handles mixed `.wal`/`.graphj` directories.
- **Sealing**: Live segments written unsealed (entries appended incrementally), sealed on rotation/shutdown with final sequence range, entry count, body length, and SHA-256 checksum.
- **Reader integration**: `JournalReader::from_sequence_with_key()` decrypts sealed encrypted segments transparently. Restore function accepts optional encryption key.
- **Security**: SHA-256 body checksum + per-entry CRC32C + chain hashing + AEAD authentication. Tamper detection for both header and ciphertext.
- **Config integration**: Encryption key parsed from hex at startup, passed through to journal reader and restore operations. Compression settings stored in `EngineManagerConfig` for future compaction use.
- **Comprehensive testing**: 15 graphj unit tests (header encoding, compression, encryption, compaction, tampering, wrong key, mixed segments). 8 config tests for CLI flags and validation. 15 breaking tests for corruption, edge cases, and error handling (truncated headers, checksum mismatches, invalid sequences, encryption without keys, max field values).
- **Performance benchmarks** (`benches/graphj_bench.rs`): Criterion-based benchmarks measuring header encode/decode, compression at different levels (1-22), encryption/decryption, combined operations, compaction (5 segments), and scaling across data sizes (10-10000 queries). Includes realistic Cypher query generation and throughput measurements.
- 165 unit tests, 165 binary tests, 44 integration tests, 8 config tests, 15 breaking tests (397 total)

## Phase 7-fix: Backup System Bug Fixes

- **Journal chain survives restart**: Added `recover_journal_state()` that scans existing segments on startup to recover last sequence + chain hash, instead of always starting at seq=0/hash=zeros.
- **Restore works after compaction**: `JournalReader` now skips chain validation for pre-start entries (fast-forwards through them), and `restore()` uses the snapshot's chain hash instead of zeros.
- **Snapshot race condition**: Added `snapshot_lock: Arc<RwLock<()>>` to `AppState`. Read lock held during mutation execution + journaling, write lock held during snapshot creation. Prevents mutations from slipping between journal state read and DB copy.
- **Write failure no longer corrupts chain**: Writer now computes new seq/hash first, writes to disk, and only updates in-memory state after successful `write_all`.
- **Retention now runs automatically**: `apply_retention` called after each snapshot creation. Retention config (`--retain-daily/weekly/monthly`) wired from CLI flags through `AppState`.
- **Dead code removed**: Removed unused `clear_dir` function and its test.
- 122 unit tests, 96 integration tests (218 total)

## Phase 7: Backup System

- **Query Rewriter** (`src/rewriter.rs`): State-machine scanner that rewrites non-deterministic Cypher functions (`gen_random_uuid()`, `current_timestamp()`, `current_date()`) into parameters with generated values, enabling deterministic journal replay. Case-insensitive, whitespace-tolerant, respects string literal boundaries.
- **Write-Ahead Journal** (`src/journal.rs`): Append-only binary journal with CRC32C integrity checks and SHA-256 chain hashing. Configurable segment rotation (default 64MB) and fsync interval (default 100ms). Captures all committed mutations (auto-commit, explicit transactions, batch, pipeline) with rewritten queries and resolved parameters.
- **Snapshots + Restore** (`src/snapshot.rs`): Physical snapshots via Kuzu CHECKPOINT + file copy to `{data_dir}/snapshots/{seq:016}/`. Restore copies snapshot data back, replays journal entries from snapshot sequence forward. GFS retention policy (daily/weekly/monthly) with automatic journal segment compaction.
- **CLI flags**: `--journal`, `--journal-segment-mb`, `--journal-fsync-ms`, `--restore`, `--snapshot`, `--retain-daily`, `--retain-weekly`, `--retain-monthly`
- **HTTP endpoint**: `POST /v1/snapshot` triggers snapshot creation
- **Data directory layout**: `data_dir/db` (Kuzu database), `data_dir/journal/` (WAL segments), `data_dir/snapshots/` (point-in-time snapshots)
- 117 unit tests, 96 integration tests (213 total)

## Phase 6: Protobuf Wire Protocol

- Replaced JSON text frames with protobuf binary frames on WebSocket (prost)
- Protobuf schema (`proto/strana.proto`) covering all client/server messages, GraphValue (11 variants), BatchResultEntry
- Wire conversion layer (`src/wire.rs`) translating between proto types and internal serde types — session layer unchanged
- HTTP endpoints support both JSON (default) and protobuf via `Content-Type: application/x-protobuf` header negotiation
- Switched from compiling Kuzu from source to prebuilt shared library (`kuzu-lib/`) via `KUZU_SHARED=1`
- All 44 existing WebSocket tests rewritten for binary protobuf, 6 new HTTP protobuf tests added
- 14 wire round-trip unit tests, 69 integration tests, 59 unit tests (128 unique tests total)

## Phase 5: HTTP Variant

- Stateless HTTP endpoints alongside existing WebSocket protocol
- `POST /v1/execute` — single query, single response
- `POST /v1/batch` — array of statements, sequential execution, stops on first error
- `POST /v1/pipeline` — transactional: BEGIN → execute all → COMMIT (auto-rollback on error)
- Bearer token auth via `Authorization: Bearer <token>` header (same TokenStore as WebSocket)
- Each HTTP request gets its own `kuzu::Connection` — no session state between requests
- HTTP-specific response types (`HttpResult` enum) separate from WebSocket `ServerMessage`
- Blocking execution via `tokio::task::spawn_blocking` for Kuzu operations
- Updated PROTOCOL.md with HTTP API section
- 19 new HTTP integration tests, 44 unit tests, 63 integration tests (151 total)

## Phase 4: Result Streaming

- Cursor-based streaming via `fetch_size` on `execute` — server holds query cursor open, client fetches batches with `fetch`
- New client messages: `fetch`, `close_stream`
- New server message: `close_stream_ok`
- `result` gains optional `stream_id` and `has_more` fields (omitted when not streaming)
- Multiple concurrent cursors per session, each with a unique `stream_id`
- Server-side cursor timeout (default 30s, configurable) — idle cursors automatically cleaned up
- Without `fetch_size`, behavior is identical to pre-streaming (backward compatible)
- Added `has_next()` to kuzu-patched `QueryResult` for partial iteration
- Updated PROTOCOL.md with streaming lifecycle and new message docs
- 44 unit tests, 41 integration tests (129 total)

## Phase 3: Transactions + Batching

- Per-session connections via `spawn_blocking` + channel-based session worker (one Kuzu `Connection` per WebSocket session)
- New client messages: `begin`, `commit`, `rollback`, `batch`
- New server messages: `begin_ok`, `commit_ok`, `rollback_ok`, `batch_result`
- Auto-commit remains the default; explicit `begin` starts a transaction
- `batch`: array of statements executed sequentially, stops on first error
- Disconnect without commit auto-rolls back the transaction
- `commit`/`rollback` outside a transaction returns `error`
- Updated PROTOCOL.md with transaction lifecycle and batch format
- 28 integration tests, 39 unit tests (106 total)

## Phase 2: Fix the Foundation

- Wired `params` to Kuzu `PreparedStatement` — parameterized queries now work (string, int, float, bool, null)
- Added `request_id` field on `execute` — echoed back on `result` and `error` responses
- Simplified backend: removed `GraphDb` trait (single backend), removed unnecessary `Mutex` on `Database`
- Updated PROTOCOL.md with `request_id` behavior and param types
- 16 integration tests, 30 unit tests (76 total)

## Phase 1: Multi-Token Auth

- SHA-256 hashed multi-token auth system with three modes: open access, single-token, multi-token file
- `--token`, `--token-file`, `--generate-token` CLI flags
- Token file format with labels for audit logging
- Renamed binary to `graphd`
- Created README, PROTOCOL.md
