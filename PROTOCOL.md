# Strana Protocol Specification v0.1

Strana is a wire protocol for executing Cypher queries against embedded graph databases over WebSocket.

## Transport

### WebSocket

WebSocket (RFC 6455). All messages are **binary frames** containing protobuf-encoded payloads (see `proto/strana.proto`). Text frames are rejected.

### HTTP

HTTP endpoints accept both JSON (`application/json`, default) and protobuf (`application/x-protobuf`). The server inspects the `Content-Type` header to determine encoding. Responses use the same encoding as the request.

## Connection Lifecycle

```
Client                              Server
  |---- WebSocket upgrade ----------->|
  |---- hello { token } ------------->|
  |<--- hello_ok { version } ---------|
  |                                    |
  |---- execute { query } ----------->|  (auto-commit)
  |<--- result { columns, rows } -----|
  |                                    |
  |---- execute { fetch_size } ------>|  (streaming)
  |<--- result { stream_id, has_more }|
  |---- fetch { stream_id } --------->|
  |<--- result { stream_id, has_more }|
  |---- fetch { stream_id } --------->|
  |<--- result { rows } --------------|  (final batch)
  |                                    |
  |---- close_stream { stream_id } -->|  (early close)
  |<--- close_stream_ok { stream_id }-|
  |                                    |
  |---- begin ----------------------->|  (explicit transaction)
  |<--- begin_ok ---------------------|
  |---- execute { query } ----------->|
  |<--- result { columns, rows } -----|
  |---- commit ---------------------->|
  |<--- commit_ok --------------------|
  |                                    |
  |---- batch { statements } -------->|
  |<--- batch_result { results } -----|
  |                                    |
  |---- close ----------------------->|
  |<--- close_ok ---------------------|
  |---- WebSocket close -------------->|
```

1. Client opens a WebSocket connection to the server.
2. Client MUST send `hello` as the first message.
3. Server responds with `hello_ok` or `hello_error`.
4. If `hello_error`, the server closes the WebSocket.
5. Client sends `execute` messages. Server responds with `result` or `error` for each.
6. Client sends `close` to end the session. Server responds with `close_ok` and closes the WebSocket.
7. If the client disconnects without `close`, the server cleans up silently.

## Authentication

Strana supports three authentication modes:

### Open access (no auth)

Start the server without `--token` or `--token-file`. All clients are accepted regardless of whether they provide a token.

### Single-token

Start the server with `--token <plaintext>`. Clients must provide this exact token in their `hello` message.

### Multi-token (token file)

Start the server with `--token-file <path>`. The file is a JSON document containing SHA-256 hashed tokens:

```json
{
  "tokens": [
    { "hash": "a1b2c3...", "label": "my-app" },
    { "hash": "d4e5f6...", "label": "ci-runner" }
  ]
}
```

| Field   | Type   | Description                                      |
|---------|--------|--------------------------------------------------|
| `hash`  | string | SHA-256 hex digest of the plaintext token        |
| `label` | string | Human-readable label (logged on successful auth) |

Clients send the plaintext token in `hello`; the server hashes it and looks up the hash. The `label` is logged but never sent to the client.

### Generating tokens

Use `--generate-token` to generate a random token and its SHA-256 hash:

```
$ graphd --generate-token
Token:  strana_a1b2c3d4e5f6...
Hash:   e3b0c44298fc1c14...
```

Add the hash to your token file, distribute the plaintext token to clients.

`--token` and `--token-file` are mutually exclusive.

## Messages

### Wire Format

WebSocket messages use protobuf binary encoding. The schema is defined in `proto/strana.proto`. `ClientMessage` wraps all client-to-server messages as a `oneof`; `ServerMessage` wraps all server-to-client messages.

HTTP endpoints accept JSON (default) or protobuf. JSON uses a `"type"` discriminator field; protobuf uses the `oneof` tag. The message semantics are identical across both encodings.

### Client Messages

#### `hello`

Sent once, immediately after WebSocket upgrade.

**JSON:**
```json
{
  "type": "hello",
  "token": "my-secret-token"
}
```

**Protobuf:** `ClientMessage { hello: Hello { token: "my-secret-token" } }`

| Field   | Type   | Required | Description                    |
|---------|--------|----------|--------------------------------|
| `token` | string | no       | Authentication token. Omit for unauthenticated mode. |

#### `execute`

Run a Cypher query.

```json
{
  "type": "execute",
  "query": "MATCH (p:Person) WHERE p.age > $min_age RETURN p.name, p.age",
  "params": {
    "min_age": 25
  },
  "request_id": "req-1",
  "fetch_size": 100
}
```

| Field        | Type    | Required | Description                         |
|--------------|---------|----------|-------------------------------------|
| `type`       | string  | yes      | `"execute"`                         |
| `query`      | string  | yes      | Cypher query string                 |
| `params`     | object  | no       | Named parameters. Keys are parameter names (without `$`). Values are JSON scalars (string, number, boolean, null). |
| `request_id` | string  | no       | Client-assigned identifier. If provided, the server echoes it back on the corresponding `result` or `error` response. |
| `fetch_size` | integer | no       | Maximum rows to return per batch. Must be >= 1. If omitted, all rows are returned in a single response. If present, enables cursor-based streaming (see [Streaming Lifecycle](#streaming-lifecycle)). |

#### `begin`

Begin an explicit transaction. Without `begin`, each `execute` auto-commits.

```json
{
  "type": "begin",
  "mode": "read",
  "request_id": "b1"
}
```

| Field        | Type   | Required | Description                         |
|--------------|--------|----------|-------------------------------------|
| `type`       | string | yes      | `"begin"`                           |
| `mode`       | string | no       | `"read"` for read-only transaction. Omit for read-write (default). |
| `request_id` | string | no       | Echoed back on `begin_ok` or `error`. |

#### `commit`

Commit the active transaction.

```json
{
  "type": "commit",
  "request_id": "c1"
}
```

| Field        | Type   | Required | Description                         |
|--------------|--------|----------|-------------------------------------|
| `type`       | string | yes      | `"commit"`                          |
| `request_id` | string | no       | Echoed back on `commit_ok` or `error`. |

#### `rollback`

Roll back the active transaction.

```json
{
  "type": "rollback",
  "request_id": "r1"
}
```

| Field        | Type   | Required | Description                         |
|--------------|--------|----------|-------------------------------------|
| `type`       | string | yes      | `"rollback"`                        |
| `request_id` | string | no       | Echoed back on `rollback_ok` or `error`. |

#### `batch`

Execute an array of statements sequentially. Stops on the first error.

```json
{
  "type": "batch",
  "statements": [
    {"query": "CREATE (:Person {name: 'Alice', age: 30})"},
    {"query": "CREATE (:Person {name: $name, age: $age})", "params": {"name": "Bob", "age": 42}}
  ],
  "request_id": "batch-1"
}
```

| Field        | Type             | Required | Description                         |
|--------------|------------------|----------|-------------------------------------|
| `type`       | string           | yes      | `"batch"`                           |
| `statements` | BatchStatement[] | yes      | Array of statements to execute sequentially. |
| `request_id` | string           | no       | Echoed back on `batch_result`.      |

Each `BatchStatement`:

| Field    | Type   | Required | Description        |
|----------|--------|----------|--------------------|
| `query`  | string | yes      | Cypher query       |
| `params` | object | no       | Named parameters   |

#### `fetch`

Retrieve the next batch of rows from an open cursor.

```json
{
  "type": "fetch",
  "stream_id": 1,
  "request_id": "f1"
}
```

| Field        | Type    | Required | Description                         |
|--------------|---------|----------|-------------------------------------|
| `type`       | string  | yes      | `"fetch"`                           |
| `stream_id`  | integer | yes      | Server-assigned stream identifier from a prior `result`. |
| `request_id` | string  | no       | Echoed back on `result` or `error`. |

#### `close_stream`

Release a server-side cursor before it is fully consumed.

```json
{
  "type": "close_stream",
  "stream_id": 1,
  "request_id": "cs1"
}
```

| Field        | Type    | Required | Description                         |
|--------------|---------|----------|-------------------------------------|
| `type`       | string  | yes      | `"close_stream"`                    |
| `stream_id`  | integer | yes      | Server-assigned stream identifier.  |
| `request_id` | string  | no       | Echoed back on `close_stream_ok` or `error`. |

#### `close`

End the session gracefully.

```json
{
  "type": "close"
}
```

### Server Messages

#### `hello_ok`

Authentication succeeded.

```json
{
  "type": "hello_ok",
  "version": "0.1.0"
}
```

| Field     | Type   | Description                 |
|-----------|--------|-----------------------------|
| `type`    | string | `"hello_ok"`                |
| `version` | string | Server protocol version     |

#### `hello_error`

Authentication failed or server cannot accept the connection.

```json
{
  "type": "hello_error",
  "message": "Invalid token"
}
```

The server closes the WebSocket after sending this message.

#### `result`

Query executed successfully. Also used for `fetch` responses.

```json
{
  "type": "result",
  "columns": ["p.name", "p.age"],
  "rows": [
    ["Alice", 30],
    ["Bob", 42]
  ],
  "timing_ms": 1.2,
  "request_id": "req-1",
  "stream_id": 1,
  "has_more": true
}
```

| Field        | Type       | Description                                    |
|--------------|------------|------------------------------------------------|
| `type`       | string     | `"result"`                                     |
| `columns`    | string[]   | Column names from the query's RETURN clause    |
| `rows`       | Value[][]  | Array of rows. Each row is an array of values. |
| `timing_ms`  | number     | Query execution time in milliseconds. `0` for `fetch` responses. |
| `request_id` | string     | Present only if `request_id` was sent.         |
| `stream_id`  | integer    | Present only when the cursor has more rows. Server-assigned identifier for subsequent `fetch` calls. |
| `has_more`   | boolean    | Present and `true` only when the cursor has more rows. Absent when all rows have been returned. |

#### `error`

Query failed. The connection remains open.

```json
{
  "type": "error",
  "message": "Syntax error: expected RETURN at line 1",
  "request_id": "req-1"
}
```

| Field        | Type   | Description         |
|--------------|--------|---------------------|
| `type`       | string | `"error"`           |
| `message`    | string | Human-readable error description |
| `request_id` | string | Present only if `request_id` was sent on `execute`. |

#### `begin_ok`

Transaction started.

```json
{
  "type": "begin_ok",
  "request_id": "b1"
}
```

| Field        | Type   | Description                                    |
|--------------|--------|------------------------------------------------|
| `type`       | string | `"begin_ok"`                                   |
| `request_id` | string | Present only if `request_id` was sent on `begin`. |

#### `commit_ok`

Transaction committed.

```json
{
  "type": "commit_ok",
  "request_id": "c1"
}
```

| Field        | Type   | Description                                    |
|--------------|--------|------------------------------------------------|
| `type`       | string | `"commit_ok"`                                  |
| `request_id` | string | Present only if `request_id` was sent on `commit`. |

#### `rollback_ok`

Transaction rolled back.

```json
{
  "type": "rollback_ok",
  "request_id": "r1"
}
```

| Field        | Type   | Description                                    |
|--------------|--------|------------------------------------------------|
| `type`       | string | `"rollback_ok"`                                |
| `request_id` | string | Present only if `request_id` was sent on `rollback`. |

#### `batch_result`

Result of a batch execution.

```json
{
  "type": "batch_result",
  "results": [
    {"type": "result", "columns": [], "rows": [], "timing_ms": 0.3},
    {"type": "result", "columns": [], "rows": [], "timing_ms": 0.2},
    {"type": "error", "message": "Syntax error"}
  ],
  "request_id": "batch-1"
}
```

| Field        | Type               | Description                                    |
|--------------|--------------------|------------------------------------------------|
| `type`       | string             | `"batch_result"`                               |
| `results`    | BatchResultEntry[] | One entry per statement that was attempted. If a statement errors, it is the last entry (execution stops). |
| `request_id` | string             | Present only if `request_id` was sent on `batch`. |

Each `BatchResultEntry` is either a `result` or `error`:

| Entry type | Fields                              |
|------------|-------------------------------------|
| `result`   | `type`, `columns`, `rows`, `timing_ms` |
| `error`    | `type`, `message`                   |

#### `close_stream_ok`

Acknowledgement that a cursor has been released.

```json
{
  "type": "close_stream_ok",
  "stream_id": 1,
  "request_id": "cs1"
}
```

| Field        | Type    | Description                                    |
|--------------|---------|------------------------------------------------|
| `type`       | string  | `"close_stream_ok"`                            |
| `stream_id`  | integer | The stream that was closed.                    |
| `request_id` | string  | Present only if `request_id` was sent on `close_stream`. |

#### `close_ok`

Acknowledgement of client's `close`.

```json
{
  "type": "close_ok"
}
```

The server closes the WebSocket after sending this message.

## Transaction Lifecycle

By default, every `execute` runs in auto-commit mode: each query is its own transaction. To group queries into an explicit transaction:

1. Send `begin` to start a transaction.
2. Send one or more `execute` messages. Queries run within the transaction.
3. Send `commit` to make changes permanent, or `rollback` to discard.

Behavior rules:
- `commit` or `rollback` outside a transaction returns `error`.
- `begin` while a transaction is already active returns `error`. The existing transaction is unaffected.
- A failed `execute` within a transaction returns `error` but the transaction stays open. The client must `rollback` or `commit`.
- A failed `commit` returns `error` and the transaction remains active. The client should `rollback` to clean up.
- The `mode` field on `begin` only accepts `"read"` or omission (for read-write). Any other value returns `error`.
- If the client disconnects without committing, the transaction is automatically rolled back.
- `batch` runs outside transactions in auto-commit mode (each statement auto-commits independently). To run a batch inside a transaction, send `begin` first, then `batch`, then `commit`.

## Streaming Lifecycle

When `execute` includes a `fetch_size`, the server returns at most that many rows and holds the query cursor open server-side. The client retrieves subsequent batches with `fetch`.

```
Client                              Server
  |---- execute { fetch_size: 2 } -->|
  |<--- result { stream_id: 1,      |  (first batch, cursor open)
  |       has_more: true, rows: [...]}
  |---- fetch { stream_id: 1 } ---->|
  |<--- result { stream_id: 1,      |  (middle batch)
  |       has_more: true, rows: [...]}
  |---- fetch { stream_id: 1 } ---->|
  |<--- result { rows: [...] } ------|  (final batch, cursor closed)
```

Behavior rules:
- If all rows fit within `fetch_size`, the response has no `stream_id` or `has_more` (identical to a non-streaming execute).
- When `has_more` is `true`, the response includes a `stream_id`. Use it in subsequent `fetch` calls.
- When the cursor is exhausted, the final response omits `stream_id` and `has_more`. The cursor is automatically released.
- `fetch` with an unknown `stream_id` returns `error`.
- `close_stream` releases a cursor before it is fully consumed. Returns `close_stream_ok`. Subsequent `fetch` calls with that `stream_id` return `error`.
- `close_stream` with an unknown `stream_id` returns `error`.
- Multiple concurrent cursors are supported within a single session. Each gets a unique `stream_id`.
- Idle cursors are automatically cleaned up after a server-configured timeout (default 30 seconds). A `fetch` after timeout returns `error` with an unknown `stream_id` message.
- When the client disconnects, all open cursors are released.
- `fetch` responses have `timing_ms: 0` (timing is only measured on the initial `execute`).

## Value Encoding

Values in `rows` are encoded as JSON. Graph-specific types use tagged objects with a `"$type"` field.

### Scalar Types

| Kuzu Type           | JSON Encoding         | Example           |
|---------------------|-----------------------|-------------------|
| Null                | `null`                | `null`            |
| Bool                | boolean               | `true`            |
| Int8–Int64, UInt8–UInt64 | number           | `42`              |
| Int128              | string (exceeds JSON number range) | `"170141183460469231731687303715884105727"` |
| Float, Double       | number                | `3.14`            |
| Decimal             | string                | `"123.45"`        |
| String              | string                | `"hello"`         |
| Blob                | string (base64)       | `"aGVsbG8="`      |
| UUID                | string                | `"550e8400-e29b-41d4-a716-446655440000"` |
| Date                | string (ISO 8601)     | `"2024-01-15"`    |
| Timestamp*          | string (ISO 8601)     | `"2024-01-15T09:30:00Z"` |
| Interval            | string (ISO 8601 duration) | `"P1Y2M3D"` |

### Node

```json
{
  "$type": "node",
  "id": {"table": 0, "offset": 5},
  "label": "Person",
  "properties": {
    "name": "Alice",
    "age": 30
  }
}
```

| Field        | Type   | Description                          |
|--------------|--------|--------------------------------------|
| `$type`      | string | `"node"`                             |
| `id`         | object | Internal ID: `{"table": int, "offset": int}` |
| `label`      | string | Node label                           |
| `properties` | object | Key-value property map               |

### Relationship

```json
{
  "$type": "rel",
  "id": {"table": 2, "offset": 10},
  "label": "KNOWS",
  "src": {"table": 0, "offset": 5},
  "dst": {"table": 0, "offset": 8},
  "properties": {
    "since": "2020-01-01"
  }
}
```

| Field        | Type   | Description                          |
|--------------|--------|--------------------------------------|
| `$type`      | string | `"rel"`                              |
| `id`         | object | Internal ID                          |
| `label`      | string | Relationship type                    |
| `src`        | object | Source node internal ID              |
| `dst`        | object | Destination node internal ID         |
| `properties` | object | Key-value property map               |

### Recursive Relationship (Path)

```json
{
  "$type": "path",
  "nodes": [
    {"$type": "node", "id": {"table": 0, "offset": 5}, "label": "Person", "properties": {"name": "Alice"}},
    {"$type": "node", "id": {"table": 0, "offset": 8}, "label": "Person", "properties": {"name": "Bob"}}
  ],
  "rels": [
    {"$type": "rel", "id": {"table": 2, "offset": 10}, "label": "KNOWS", "src": {"table": 0, "offset": 5}, "dst": {"table": 0, "offset": 8}, "properties": {}}
  ]
}
```

### Collections

| Kuzu Type | JSON Encoding | Example |
|-----------|---------------|---------|
| List      | array         | `[1, 2, 3]` |
| Array     | array         | `[1, 2, 3]` |
| Map       | object        | `{"key": "value"}` |
| Struct    | object        | `{"name": "Alice", "age": 30}` |

### Union

```json
{
  "$type": "union",
  "tag": "string",
  "value": "hello"
}
```

## HTTP API

Strana also provides stateless HTTP endpoints using the same JSON value encoding as WebSocket. No session state is maintained between HTTP requests.

### Authentication

HTTP endpoints use `Authorization: Bearer <token>` header authentication. The same token store (single-token, multi-token file) applies. In open access mode, the header may be omitted.

Unauthorized requests receive HTTP 401 with:

```json
{"type": "error", "message": "Unauthorized"}
```

### `POST /v1/execute`

Execute a single Cypher query.

**Request:**

```json
{
  "query": "MATCH (p:Person) WHERE p.age > $min RETURN p.name",
  "params": {"min": 25}
}
```

| Field    | Type   | Required | Description                         |
|----------|--------|----------|-------------------------------------|
| `query`  | string | yes      | Cypher query string                 |
| `params` | object | no       | Named parameters (same as WebSocket `execute`) |

**Response (200 — success):**

```json
{
  "type": "result",
  "columns": ["p.name"],
  "rows": [["Alice"]],
  "timing_ms": 1.2
}
```

**Response (200 — query error):**

```json
{
  "type": "error",
  "message": "Syntax error: expected RETURN at line 1"
}
```

**Response (400 — bad request):**

```json
{
  "type": "error",
  "message": "Invalid request body: ..."
}
```

### `POST /v1/batch`

Execute an array of statements sequentially. Each statement auto-commits independently. Stops on the first error.

**Request:**

```json
{
  "statements": [
    {"query": "CREATE (:Person {name: 'Alice', age: 30})"},
    {"query": "CREATE (:Person {name: $name, age: $age})", "params": {"name": "Bob", "age": 42}}
  ]
}
```

| Field        | Type             | Required | Description                         |
|--------------|------------------|----------|-------------------------------------|
| `statements` | StatementEntry[] | yes      | Array of statements to execute.     |

Each `StatementEntry`:

| Field    | Type   | Required | Description        |
|----------|--------|----------|--------------------|
| `query`  | string | yes      | Cypher query       |
| `params` | object | no       | Named parameters   |

**Response (200):**

```json
{
  "type": "batch_result",
  "results": [
    {"type": "result", "columns": [], "rows": [], "timing_ms": 0.3},
    {"type": "error", "message": "Syntax error"}
  ]
}
```

### `POST /v1/pipeline`

Execute an array of statements in a single transaction. If all succeed, the transaction is committed. On the first error, the transaction is rolled back.

**Request:** Same format as `/v1/batch`.

**Response (200 — all succeed):**

```json
{
  "type": "pipeline_result",
  "results": [
    {"type": "result", "columns": [], "rows": [], "timing_ms": 0.3},
    {"type": "result", "columns": ["x"], "rows": [[1]], "timing_ms": 0.1}
  ]
}
```

**Response (200 — error causes rollback):**

```json
{
  "type": "pipeline_result",
  "results": [
    {"type": "result", "columns": [], "rows": [], "timing_ms": 0.3},
    {"type": "error", "message": "Syntax error"}
  ]
}
```

When an error occurs, the response includes results up to and including the error. All preceding mutations are rolled back.

### HTTP vs WebSocket

| Feature           | WebSocket                | HTTP                     |
|-------------------|--------------------------|--------------------------|
| Session state     | Yes (per connection)     | No (stateless)           |
| Transactions      | `begin`/`commit`/`rollback` | `/v1/pipeline` only   |
| Streaming         | `fetch_size` + `fetch`   | Not supported            |
| Prepared stmts    | Planned                  | Not supported            |
| Auth              | `hello { token }`        | `Authorization: Bearer`  |
| Batching          | `batch`                  | `/v1/batch` or `/v1/pipeline` |

## Error Handling

- Invalid protobuf: server sends `error` and closes WebSocket.
- Unknown message variant: server sends `error`, connection remains open.
- First message is not `hello`: server sends `hello_error` and closes WebSocket.
- Text frame on WebSocket: server sends `error` ("Text encoding not supported — use binary protobuf") and closes WebSocket.
- Query syntax error: server sends `error`, connection remains open.
- Query execution error: server sends `error`, connection remains open.
- Server internal error: server sends `error` and closes WebSocket.
- HTTP invalid request body (bad JSON or bad protobuf): HTTP 400 with `error`.
- HTTP unauthorized: HTTP 401 with `error`.
