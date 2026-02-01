# Strana Roadmap

## Phase 8: Prepared Statements

Cache compiled queries for repeated execution:

- New client message: `prepare { query, statement_id }` → server compiles and caches
- New server message: `prepare_ok { statement_id }`
- New client message: `execute_prepared { statement_id, params }` → run cached query
- Statements scoped to the session (WebSocket connection). Cleaned up on disconnect.
- HTTP: no prepared statements (stateless — each request is independent)
- Tests: prepare + execute_prepared, invalid statement_id, params binding
- Update PROTOCOL.md

## Later

- Segment-level encryption (AES-256-GCM) + compression (zstd) on journal segments and snapshots
- S3 upload for journal segments and snapshots
- Multi-tenancy + pool manager
- LadybugDB / Bighorn backends
- Python + TypeScript client libraries
- Configurable `buffer_pool_size` and `max_threads` CLI flags
