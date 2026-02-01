# Strana

Strana is a wire protocol for executing [Cypher](https://opencypher.org/) queries against embedded graph databases over WebSocket. `graphd` is the server that implements it.

Embedded graph databases like [LadybugDB](https://ladybugdb.com/) (formerly [Kuzu](https://kuzudb.com/)) are great for a lot of use cases, but sometimes you need to access your graph over the network. Think [sqld](https://github.com/tursodatabase/libsql/tree/main/libsql-server) for graph databases, inspired by the [Hrana](https://github.com/tursodatabase/libsql/tree/main/libsql-hrana) protocol.

- **strana** — the protocol ([PROTOCOL.md](PROTOCOL.md))
- **graphd** — the server daemon

## Quick start

```bash
cargo build --release
./target/release/graphd
```

`graphd` is now listening on `127.0.0.1:8080`. Connect any WebSocket client to `/ws` and start running Cypher queries.

## Usage

```
graphd [OPTIONS]
```

| Flag | Default | Description |
|------|---------|-------------|
| `-p, --port` | `8080` | Port to listen on |
| `--host` | `127.0.0.1` | Bind address |
| `-d, --data-dir` | `./data` | Path to the database directory |
| `--token <TOKEN>` | — | Require clients to authenticate with this token |
| `--token-file <PATH>` | — | Load hashed tokens from a JSON file |
| `--generate-token` | — | Print a new token and its SHA-256 hash, then exit |

```bash
# Open access (no auth)
graphd --port 9090 --data-dir ./my-graph

# Single token
graphd --token my-secret-token

# Multi-token (from file)
graphd --generate-token          # grab the hash
graphd --token-file tokens.json  # start with token file
```

The `--token` flag can also be set via the `STRANA_TOKEN` environment variable.

## Protocol

Strana uses a simple JSON-over-WebSocket protocol. See [PROTOCOL.md](PROTOCOL.md) for the full specification.

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

### Example session

```json
→ {"type": "hello", "token": "my-secret-token"}
← {"type": "hello_ok", "version": "0.1.0"}

→ {"type": "execute", "query": "CREATE NODE TABLE Person(name STRING, age INT64, PRIMARY KEY(name))"}
← {"type": "result", "columns": [], "rows": [], "timing_ms": 1.2}

→ {"type": "execute", "query": "CREATE (:Person {name: 'Alice', age: 30})"}
← {"type": "result", "columns": [], "rows": [], "timing_ms": 0.5}

→ {"type": "execute", "query": "MATCH (p:Person) RETURN p.name, p.age"}
← {"type": "result", "columns": ["p.name", "p.age"], "rows": [["Alice", 30]], "timing_ms": 0.3}

→ {"type": "close"}
← {"type": "close_ok"}
```

## Authentication

Strana supports three auth modes:

- **Open access** — no `--token` or `--token-file` flag. All clients accepted.
- **Single-token** — `--token <plaintext>`. Clients must send this token in `hello`.
- **Multi-token** — `--token-file <path>`. A JSON file of SHA-256 hashed tokens with labels.

Generate a token and its hash:

```bash
$ graphd --generate-token
Token:  strana_a1b2c3d4e5f6...
Hash:   e3b0c44298fc1c14...
```

Token file format:

```json
{
  "tokens": [
    { "hash": "<sha256-hex>", "label": "my-app" },
    { "hash": "<sha256-hex>", "label": "ci-runner" }
  ]
}
```

## Tests

```bash
cargo test
```

## Client libraries

_Coming soon — Python and TypeScript clients are planned._

## License

MIT
