# Strana Benchmarks

Performance benchmarks for graphd and the GraphJ file format.

## Running Benchmarks

All benchmarks:
```bash
make bench
```

Specific benchmark suite:
```bash
DYLD_LIBRARY_PATH=./lbug-lib cargo bench --bench graphj_bench
DYLD_LIBRARY_PATH=./lbug-lib cargo bench --bench throughput_bench
```

## Benchmark Groups

### Header Operations

- **header_encode**: Measure header encoding performance (128-byte binary serialization)
- **header_decode**: Measure header decoding performance

### Compression

- **compression_levels**: Compare zstd compression at levels 1, 3, 6, 9, 15, 22
  - Uses realistic Cypher query data (~10KB)
  - Shows throughput in bytes/second
  - Helps choose optimal compression level for different workloads

- **decompression**: Measure zstd decompression performance
  - Level 3 compressed data (default)

### Encryption

- **encryption**: XChaCha20-Poly1305 AEAD encryption performance
  - Includes Poly1305 authentication tag generation
  - AAD covers first 40 bytes of header

- **decryption**: XChaCha20-Poly1305 AEAD decryption + verification
  - Includes Poly1305 authentication tag verification
  - Tests full decode_body() path

### Combined Operations

- **compress_encrypt**: Full compression + encryption pipeline
  - Processing order: compress → encrypt
  - Realistic end-to-end performance

### Compaction

- **compaction**: Segment merging performance
  - Benchmarks merging 5 segments (~10KB total)
  - Three variants:
    - No compression or encryption
    - Compression only (zstd level 3)
    - Compression + encryption

### Data Sizes

- **data_sizes**: Performance scaling at different data volumes
  - Tests 10, 100, 1000, 10000 Cypher queries
  - Shows how compression and encryption scale
  - Helps understand throughput characteristics

## Interpreting Results

Criterion outputs:
- **time**: Wall-clock time per iteration
- **throughput**: Bytes processed per second (MB/s or GB/s)
- **slope**: Linear regression slope (performance trend)
- **R²**: Goodness of fit (closer to 1.0 is better)

### Expected Performance

On Apple Silicon (M1/M2):
- **Header encode/decode**: ~500-1000 ns (negligible overhead)
- **Compression (level 3)**: ~500-800 MB/s
- **Decompression**: ~2-4 GB/s
- **Encryption**: ~1-2 GB/s (depends on data size)
- **Decryption**: ~1-2 GB/s

Compression ratio for Cypher queries: ~10:1 (varies by repetition and structure)

### Choosing Compression Level

- **Level 1**: Fastest, ~7:1 ratio, use for high-throughput streaming
- **Level 3**: Default, balanced, ~10:1 ratio
- **Level 6-9**: Better compression, ~12-15:1 ratio, use for cold storage
- **Level 15-22**: Maximum compression, ~15-20:1 ratio, use for archival

Higher levels have diminishing returns on Cypher text. Level 3 is optimal for most workloads.

## Benchmark Data

All benchmarks use `generate_cypher_queries()` which creates realistic Cypher MERGE queries with:
- Entity nodes
- Property SETs
- Parameterized values (id, name, embedding, timestamp)

This mirrors actual graphd journal content better than random bytes.

## Continuous Benchmarking

Results are saved in `target/criterion/`. Compare across runs with:

```bash
cargo bench --bench graphj_bench -- --save-baseline before
# make changes
cargo bench --bench graphj_bench -- --baseline before
```

Criterion will show performance regressions/improvements.

---

# LadybugDB Throughput Benchmarks

End-to-end performance benchmarks measuring query execution throughput through graphd to LadybugDB.

## Benchmark Groups

### Simple Queries

- **match_by_id**: Single node lookup by primary key
- **match_all_limit_10**: Scan with LIMIT clause

### Parameterized Queries

- **create_with_params**: Parameterized CREATE with string and int64 bindings
- Tests PreparedStatement performance

### Write Throughput

Measures writes/second at different batch sizes:
- Batch sizes: 1, 10, 100, 1000 nodes
- Shows scaling characteristics
- Helps determine optimal batch size for bulk loading

### Transaction Throughput

- **explicit_transaction_10_writes**: 10 CREATEs in a single transaction
- Measures transaction overhead vs auto-commit

### Relationship Creation

- **create_relationship**: MATCH two nodes, CREATE relationship
- Common pattern in graph workloads
- Tests join + write performance

### MERGE Operations

- **merge_create**: MERGE on non-existent node (creates)
- **merge_update**: MERGE on existing node (updates)
- Critical for upsert patterns in Mem0/Graphiti

### Read Throughput

Measures reads/second with different result set sizes:
- LIMIT 10, 100, 1000, 10000
- Full table scans
- Tests result set iteration overhead

### Complex Queries

- **two_hop_traversal**: Variable-length path traversal (1-2 hops)
- Tests graph algorithms and multi-hop performance

## Expected Performance

On Apple Silicon (M1/M2) with 256MB buffer pool:
- **Simple MATCH by ID**: ~10-50 µs
- **Parameterized CREATE**: ~50-200 µs
- **Batch writes (100)**: ~5-10ms total (~10-20K writes/sec)
- **Transaction (10 writes)**: ~500µs-1ms
- **MERGE create**: ~100-300 µs
- **MERGE update**: ~50-150 µs
- **Relationship CREATE**: ~100-500 µs
- **Scan 1000 nodes**: ~1-5ms

Performance scales with:
- Buffer pool size (more = less disk I/O)
- SSD speed
- Query complexity
- Index usage

## Optimization Tips

Based on benchmark results:

1. **Batch writes**: Group 100-1000 CREATEs in a transaction for 10-100x speedup
2. **Use MERGE wisely**: ~2-3x slower than CREATE, but necessary for upserts
3. **Parameterize queries**: Avoid string formatting, use PreparedStatement
4. **Index primary keys**: LadybugDB does this automatically for PRIMARY KEY columns
5. **Limit result sets**: Only fetch what you need

## Comparing Runs

```bash
# Baseline
DYLD_LIBRARY_PATH=./lbug-lib cargo bench --bench throughput_bench -- --save-baseline main

# After changes
DYLD_LIBRARY_PATH=./lbug-lib cargo bench --bench throughput_bench -- --baseline main
```
