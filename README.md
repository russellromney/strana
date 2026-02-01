# Strana

Strana is a wire protocol for executing [Cypher](https://opencypher.org/) queries against embedded graph databases over WebSocket and HTTP. `graphd` is the reference server.

Embedded graph databases like [LadybugDB](https://ladybugdb.com/) (formerly [Kuzu](https://kuzudb.com/)) are great for a lot of use cases, but sometimes you need to access your graph over the network. Think [sqld](https://github.com/tursodatabase/libsql/tree/main/libsql-server) for graph databases, inspired by the [Hrana](https://github.com/tursodatabase/libsql/tree/main/libsql-hrana) protocol.

| | |
|---|---|
| **Strana** | the wire protocol ([PROTOCOL.md](PROTOCOL.md)) |
| **graphd** | the server ([crates.io](https://crates.io/crates/graphd)) |
| **strana-python** | Python SDK (coming soon) |

## Install

```bash
# Rust
cargo install graphd

# Python
pip install graphd

# From source
cargo build --release
```

## Quick start

```bash
graphd --data-dir ./my-graph
```

`graphd` is now listening on `127.0.0.1:7688`. Connect via WebSocket (`/ws`) or HTTP (`/v1/execute`).

## Usage

```
graphd [OPTIONS]
```

| Flag | Default | Description |
|------|---------|-------------|
| `-p, --port` | `7688` | Port to listen on |
| `--host` | `127.0.0.1` | Bind address |
| `-d, --data-dir` | `./data` | Path to the database directory |
| `--token <TOKEN>` | — | Require clients to authenticate with this token |
| `--token-file <PATH>` | — | Load hashed tokens from a JSON file |
| `--generate-token` | — | Print a new token and its SHA-256 hash, then exit |
| `--journal` | off | Enable write-ahead journal for backup/replay |
| `--s3-bucket <BUCKET>` | — | Upload snapshots to S3 (Tigris-compatible) |

```bash
# Open access
graphd --data-dir ./my-graph

# With auth
graphd --token my-secret-token

# With journal + S3 snapshots
graphd --journal --s3-bucket my-snapshots
```

The `--token` flag can also be set via the `STRANA_TOKEN` environment variable.

## Protocol

Strana uses protobuf over WebSocket and JSON over HTTP. See [PROTOCOL.md](PROTOCOL.md) for the full specification.

```
Client                              Server
  |---- ws connect /ws ------------->|
  |---- hello { token } ------------>|
  |<--- hello_ok { version } --------|
  |                                   |
  |---- execute { query } ---------->|
  |<--- result { columns, rows } ----|
  |                                   |
  |---- close ---------------------->|
  |<--- close_ok --------------------|
```

### HTTP API

```bash
# Execute a query
curl -X POST http://localhost:7688/v1/execute \
  -H "Content-Type: application/json" \
  -d '{"query": "MATCH (p:Person) RETURN p.name, p.age"}'

# Batch (each statement auto-commits)
curl -X POST http://localhost:7688/v1/batch \
  -H "Content-Type: application/json" \
  -d '{"statements": [{"query": "CREATE (:Person {name: '\''Alice'\'', age: 30})"}]}'

# Pipeline (transactional — all-or-nothing)
curl -X POST http://localhost:7688/v1/pipeline \
  -H "Content-Type: application/json" \
  -d '{"statements": [{"query": "CREATE (:Person {name: '\''Bob'\'', age: 42})"}]}'
```

## Authentication

Three auth modes:

- **Open access** — no `--token` or `--token-file`. All clients accepted.
- **Single-token** — `--token <plaintext>`. Clients send this in `hello` (WS) or `Authorization: Bearer` (HTTP).
- **Multi-token** — `--token-file <path>`. SHA-256 hashed tokens with labels.

```bash
$ graphd --generate-token
Token:  strana_a1b2c3d4e5f6...
Hash:   e3b0c44298fc1c14...
```

## Client libraries

| Language | Package | Status |
|----------|---------|--------|
| Python | `strana-python` | In development |
| TypeScript | — | Planned |

## License

MIT
