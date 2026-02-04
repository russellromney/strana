# graphd

Neo4j-compatible graph database server powered by embedded LadybugDB (formerly Kuzu).

Exposes LadybugDB over Bolt 4.4–5.7 and Neo4j HTTP API. Adds journaling, point-in-time recovery, S3 backups, and read replicas via the `.graphj` format. Think [sqld](https://github.com/tursodatabase/libsql/tree/main/libsql-server) for graph databases.

## Features

- **Neo4j-compatible**: Bolt 4.4–5.7 protocol + Neo4j HTTP API
- **Fast embedded backend**: LadybugDB (formerly Kuzu)
- **Read replicas**: S3-based replicas with continuous journal streaming
- **Logical replication**: GraphJ format with CRC32C + SHA-256 chain hashing
- **Point-in-time recovery**: Snapshots + journal replay
- **Compression & encryption**: zstd compress-on-seal + XChaCha20-Poly1305 encryption
- **S3-compatible backups**: Tigris, R2, Wasabi, MinIO, etc.
- **Token authentication**: SHA-256 hashed multi-token support

## Architecture

```
┌─────────────────────────────────────┐
│  Neo4j Drivers (Python, JS, Go...)  │
└──────────────┬──────────────────────┘
               │ Bolt 4.4–5.7 / HTTP
┌──────────────▼──────────────────────┐
│          graphd (Rust)               │
│  ┌────────────────────────────────┐ │
│  │  Bolt Server + Neo4j HTTP API  │ │
│  └────────────┬───────────────────┘ │
│  ┌────────────▼───────────────────┐ │
│  │    Query Rewriter (non-det)    │ │
│  └────────────┬───────────────────┘ │
│  ┌────────────▼───────────────────┐ │
│  │   Journal Writer (.graphj)     │ │
│  └──────┬─────────────────────────┘ │
│         │  ┌───────────────────┐    │
│         │  │ S3 Journal Upload │───▶ S3
│         │  └───────────────────┘    │
│  ┌──────▼─────────────────────────┐ │
│  │  LadybugDB (embedded Kuzu)     │ │
│  └────────────────────────────────┘ │
└─────────────────────────────────────┘
         │                  │
         ▼                  ▼
    data/db/          data/journal/
  (LadybugDB)         (.graphj files)

                    ┌─────────────────────┐
             S3 ◀───│  graphd --replica    │
                    │  (read-only replica) │
                    └─────────────────────┘
```

## Requirements

- Linux or macOS (Windows via WSL)
- Rust 1.70+

## Install

```bash
# Rust
cargo install graphd

# From source
make setup-lbug  # Download LadybugDB prebuilt library
cargo build --release
```

## Quick start

```bash
graphd --data-dir ./my-graph --token my-secret-token
```

`graphd` is now listening on Bolt port `7687` and HTTP port `7688`. Connect with any Neo4j driver:

```python
from neo4j import GraphDatabase

# Connect with authentication
driver = GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", "my-secret-token"))

# Use explicit transactions for bulk writes (7-10x faster than auto-commit)
with driver.session() as session:
    with session.begin_transaction() as tx:
        for i in range(100):
            tx.run("CREATE (n:Person {id: $id, name: $name})", id=i, name=f"Person{i}")
        tx.commit()
```

## Usage

```
graphd [OPTIONS]
```

### Core Options

| Flag | Default | Description |
|------|---------|-------------|
| `--bolt-port` | `7687` | Port for Bolt protocol |
| `--bolt-host` | `127.0.0.1` | Bolt bind address |
| `--http-port` | `7688` | Port for HTTP API (Neo4j-compatible) |
| `--http-host` | `127.0.0.1` | HTTP bind address |
| `-d, --data-dir` | `./data` | Database directory |
| `--tx-timeout-secs` | `30` | HTTP transaction timeout in seconds |
| `--bolt-max-connections` | `256` | Maximum concurrent Bolt connections |
| `--read-connections` | `4` | Number of concurrent read connections in pool |

### Authentication

| Flag | Description |
|------|-------------|
| `--token <TOKEN>` | Single-token auth (plaintext) |
| `--token-file <PATH>` | Multi-token auth (SHA-256 hashed JSON file) |
| `--generate-token` | Generate a new token + hash, then exit |

### Journal & Backup

| Flag | Description |
|------|-------------|
| `--journal` | Enable write-ahead journal |
| `--journal-compress` | Enable zstd compression for compacted segments |
| `--journal-compress-level` | Compression level 1-22 (default: 3) |
| `--journal-encryption-key` | 64-char hex key for XChaCha20-Poly1305 encryption |
| `--journal-segment-mb` | Segment rotation size in MB (default: 64) |
| `--journal-fsync-ms` | Fsync interval in ms (default: 100) |

### Snapshots & S3

| Flag | Description |
|------|-------------|
| `--restore` | Restore from latest snapshot + replay journal, then exit |
| `--snapshot <PATH>` | Optional: specific snapshot directory for `--restore` |
| `--s3-bucket` | S3 bucket name for snapshot/journal uploads |
| `--s3-prefix` | S3 key prefix (default: `""`) |
| `--retain-daily` | Keep N daily snapshots (default: 7) |
| `--retain-weekly` | Keep N weekly snapshots (default: 4) |
| `--retain-monthly` | Keep N monthly snapshots (default: 3) |

### Replicas

| Flag | Default | Description |
|------|---------|-------------|
| `--replica` | | Enable read-only replica mode |
| `--replica-source` | | Source URL: `s3://bucket/prefix` or `file:///path` |
| `--replica-poll-interval` | `10s` | How often to poll for new journal segments |
| `--replica-lag-warn` | `60s` | Warn if replica falls behind by this duration |

### Examples

```bash
# Basic server
graphd --data-dir ./my-graph

# With authentication
graphd --token my-secret-token

# With journaling + compression
graphd --journal --journal-compress

# Primary with journal + S3 (snapshots + journal segments uploaded continuously)
export AWS_ACCESS_KEY_ID=your-key
export AWS_SECRET_ACCESS_KEY=your-secret
export AWS_REGION=us-east-1
graphd --journal \
  --s3-bucket my-graph-bucket \
  --s3-prefix prod/

# Read replica (polls S3 for new journal segments)
graphd --replica \
  --replica-source s3://my-graph-bucket/prod/ \
  --replica-poll-interval 5s

# Restore from S3 snapshot + journal replay
graphd --restore --s3-bucket my-graph-bucket --s3-prefix prod/
```

## Neo4j Compatibility

### Bolt Protocol

`graphd` implements Bolt 4.4–5.7. Use any Neo4j driver:

```python
# Python
from neo4j import GraphDatabase
driver = GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", "your-token"))
```

```javascript
// JavaScript/TypeScript
const neo4j = require('neo4j-driver');
const driver = neo4j.driver('bolt://localhost:7687', neo4j.auth.basic('neo4j', 'your-token'));
```

```go
// Go
import "github.com/neo4j/neo4j-go-driver/v5/neo4j"
driver, _ := neo4j.NewDriverWithContext("bolt://localhost:7687", neo4j.BasicAuth("neo4j", "your-token", ""))
```

### HTTP API

Neo4j HTTP endpoints (port 7688 by default):

```bash
# Execute query
curl -X POST http://localhost:7688/db/neo4j/tx/commit \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer your-token" \
  -d '{"statements": [{"statement": "CREATE (n:Person {name: $name}) RETURN n", "parameters": {"name": "Alice"}}]}'

# Transaction (begin, execute, commit)
curl -X POST http://localhost:7688/db/neo4j/tx \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer your-token" \
  -d '{"statements": [{"statement": "CREATE (n:Person {name: $name})", "parameters": {"name": "Bob"}}]}'
```

## Authentication

Three modes:

- **Open access**: No `--token` or `--token-file`. All clients accepted.
- **Single-token**: `--token <plaintext>`. Clients authenticate with this token.
- **Multi-token**: `--token-file <path>`. Load SHA-256 hashed tokens from JSON file.

Generate a token:

```bash
$ graphd --generate-token
Token:  graphd_a1b2c3d4e5f6...
Hash:   e3b0c44298fc1c14...
```

Token file format (`tokens.json`):

```json
{
  "e3b0c44298fc1c14...": "production-api",
  "9f86d081884c7d65...": "staging-service"
}
```

## GraphJ Journal Format

`.graphj` (Graph Journal) is the universal format for write-ahead logging and point-in-time recovery.

### Binary Format

- **128-byte fixed header**: magic, version, flags, sequence range, checksums, nonce
- **Variable body**: Raw journal entries or compressed/encrypted payload

Live segments are written **unsealed** (raw, uncompressed). When sealed (rotation, upload, or shutdown), segments are compressed with zstd. Compaction can additionally apply encryption.

### Features

- **Integrity**: CRC32C per entry + SHA-256 chain hashing + body checksum
- **Compression**: zstd (level 3 default, 1-22 supported)
- **Encryption**: XChaCha20-Poly1305 AEAD with AAD binding to header
- **Backward compatibility**: Transparently reads legacy `.wal` files

See [`src/graphj.rs`](src/graphj.rs) for the complete specification.

### Usage

```bash
# Enable journaling
graphd --journal --data-dir ./data

# With compression
graphd --journal --journal-compress --journal-compress-level 6

# With encryption (32-byte key = 64 hex chars)
GRAPHD_JOURNAL_ENCRYPTION_KEY=0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef \
  graphd --journal --journal-compress

# Restore from snapshot
graphd --restore --data-dir ./data

# Create snapshot + upload to S3
graphd --snapshot --s3-bucket my-backups
```

## Roadmap

See [ROADMAP.md](ROADMAP.md) for planned features including:
- Schema inference (auto-DDL)
- Serverless SDKs (Python/Node)
- Additional AI memory framework verification (Cognee)
- MCP server for LLM integration

## Performance

Benchmark results from `cargo bench` (Apple Silicon M1):

### Read Performance
- **MATCH by ID**: ~202 µs (~5K ops/sec)
- **Scan 1K nodes**: ~1.9 ms (~521K rows/sec)
- **Scan 10K nodes**: ~16 ms (~626K rows/sec)
- **Two-hop traversal**: ~1.8 ms (~553 ops/sec)

### Write Performance
- **Single CREATE** (auto-commit): ~4.9 ms (~203 ops/sec)
- **Transaction (10 writes)**: ~7.2 ms (~1.4K writes/sec) — **7x faster than auto-commit**
- **MERGE create**: ~5.1 ms (~196 ops/sec)
- **MERGE update**: ~5.5 ms (~182 ops/sec)
- **Relationship CREATE**: ~5.4 ms (~184 ops/sec)

**Key takeaway**: Use explicit transactions for bulk writes — 10-100x faster than individual auto-commits.

See [`benches/README.md`](benches/README.md) for full benchmark documentation.

## Testing

```bash
# Unit + integration tests
make test

# Driver compatibility tests
make e2e         # Python integration (default, comprehensive)
make e2e-py      # Python integration tests
make e2e-python  # Python standalone driver tests
make e2e-js      # JavaScript driver tests
make e2e-go      # Go driver tests
make e2e-rust    # Rust driver tests (Bolt 5.x pending)
make e2e-all     # All driver tests

# Benchmarks
make bench
```

## Development

```bash
# Setup (download prebuilt LadybugDB library)
make setup-lbug

# Build
cargo build

# Run
cargo run -- --data-dir ./data

# Test
make test
make e2e

# Benchmark
make bench
```

## Changelog

See [CHANGELOG.md](CHANGELOG.md) for version history.

## License

MIT
