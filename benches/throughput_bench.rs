use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use std::sync::atomic::{AtomicU64, Ordering};

/// Global counter for unique IDs across all benchmarks
static GLOBAL_COUNTER: AtomicU64 = AtomicU64::new(0);

/// Helper to consume all rows from a query result
fn consume_result(mut result: lbug::QueryResult) -> usize {
    let mut count = 0;
    while result.next().is_some() {
        count += 1;
    }
    count
}

/// Create schema in a database
fn create_schema(conn: &lbug::Connection) {
    conn.query("CREATE NODE TABLE IF NOT EXISTS Entity(id STRING, name STRING, created_at INT64, PRIMARY KEY(id))")
        .unwrap();
    conn.query("CREATE REL TABLE IF NOT EXISTS CONNECTED_TO(FROM Entity TO Entity, weight FLOAT, created_at INT64)")
        .unwrap();
}

/// Benchmark simple MATCH queries
fn bench_simple_match(c: &mut Criterion) {
    let _dir = tempfile::tempdir().unwrap();
    let db_path = _dir.path().join("benchmark.db");
    let db = lbug::Database::new(db_path, lbug::SystemConfig::default()).unwrap();
    let conn = lbug::Connection::new(&db).unwrap();
    create_schema(&conn);

    // Insert test data
    for i in 0..1000 {
        let query = format!(
            "CREATE (n:Entity {{id: '{}', name: 'Entity {}', created_at: {}}})",
            i, i, i
        );
        conn.query(&query).unwrap();
    }

    let mut group = c.benchmark_group("simple_match");
    group.throughput(Throughput::Elements(1));

    group.bench_function("match_by_id", |b| {
        b.iter(|| {
            let result = conn.query(black_box("MATCH (n:Entity {id: '500'}) RETURN n")).unwrap();
            consume_result(result);
        });
    });

    group.bench_function("match_all_limit_10", |b| {
        b.iter(|| {
            let result = conn.query(black_box("MATCH (n:Entity) RETURN n LIMIT 10")).unwrap();
            consume_result(result);
        });
    });

    group.finish();
}

/// Benchmark simple write operations
fn bench_simple_writes(c: &mut Criterion) {
    let _dir = tempfile::tempdir().unwrap();
    let db_path = _dir.path().join("benchmark.db");
    let db = lbug::Database::new(db_path, lbug::SystemConfig::default()).unwrap();
    let conn = lbug::Connection::new(&db).unwrap();
    create_schema(&conn);

    let mut group = c.benchmark_group("simple_writes");
    group.throughput(Throughput::Elements(1));

    group.bench_function("create_node", |b| {
        b.iter(|| {
            let counter = GLOBAL_COUNTER.fetch_add(1, Ordering::Relaxed);
            let query = format!(
                "CREATE (n:Entity {{id: '{}', name: 'Entity {}', created_at: {}}})",
                counter, counter, counter
            );
            conn.query(black_box(&query)).unwrap();
        });
    });

    group.finish();
}

/// Benchmark write throughput (inserts)
fn bench_write_throughput(c: &mut Criterion) {
    let mut group = c.benchmark_group("write_throughput");

    for batch_size in [1, 10, 100, 1000] {
        group.throughput(Throughput::Elements(batch_size as u64));

        group.bench_with_input(
            BenchmarkId::new("batch_insert", batch_size),
            &batch_size,
            |b, &size| {
                let _dir = tempfile::tempdir().unwrap();
                let db_path = _dir.path().join("benchmark.db");
                let db = lbug::Database::new(db_path, lbug::SystemConfig::default()).unwrap();
                let conn = lbug::Connection::new(&db).unwrap();
                create_schema(&conn);

                b.iter(|| {
                    for _ in 0..size {
                        let counter = GLOBAL_COUNTER.fetch_add(1, Ordering::Relaxed);
                        let query = format!(
                            "CREATE (n:Entity {{id: '{}', name: 'Entity {}', created_at: {}}})",
                            counter, counter, counter
                        );
                        conn.query(black_box(&query)).unwrap();
                    }
                });
            },
        );
    }

    group.finish();
}

/// Benchmark transaction throughput
fn bench_transaction_throughput(c: &mut Criterion) {
    let _dir = tempfile::tempdir().unwrap();
    let db_path = _dir.path().join("benchmark.db");
    let db = lbug::Database::new(db_path, lbug::SystemConfig::default()).unwrap();
    let conn = lbug::Connection::new(&db).unwrap();
    create_schema(&conn);

    let mut group = c.benchmark_group("transaction_throughput");
    group.throughput(Throughput::Elements(10));

    group.bench_function("explicit_transaction_10_writes", |b| {
        b.iter(|| {
            conn.query("BEGIN TRANSACTION").unwrap();
            for _ in 0..10 {
                let counter = GLOBAL_COUNTER.fetch_add(1, Ordering::Relaxed);
                let query = format!(
                    "CREATE (n:Entity {{id: '{}', name: 'Entity {}', created_at: {}}})",
                    counter, counter, counter
                );
                conn.query(black_box(&query)).unwrap();
            }
            conn.query("COMMIT").unwrap();
        });
    });

    group.finish();
}

/// Benchmark relationship creation
fn bench_relationship_creation(c: &mut Criterion) {
    let _dir = tempfile::tempdir().unwrap();
    let db_path = _dir.path().join("benchmark.db");
    let db = lbug::Database::new(db_path, lbug::SystemConfig::default()).unwrap();
    let conn = lbug::Connection::new(&db).unwrap();
    create_schema(&conn);

    // Create 100 entities first
    for i in 0..100 {
        conn.query(&format!(
            "CREATE (n:Entity {{id: '{}', name: 'Entity {}', created_at: {}}})",
            i, i, i
        )).unwrap();
    }

    let mut group = c.benchmark_group("relationship_creation");
    group.throughput(Throughput::Elements(1));

    group.bench_function("create_relationship", |b| {
        b.iter(|| {
            let counter = GLOBAL_COUNTER.fetch_add(1, Ordering::Relaxed);
            let from = counter % 100;
            let to = (counter + 1) % 100;
            let query = format!(
                "MATCH (a:Entity {{id: '{}'}}), (b:Entity {{id: '{}'}}) CREATE (a)-[:CONNECTED_TO {{weight: 1.0, created_at: {}}}]->(b)",
                from, to, counter
            );
            conn.query(black_box(&query)).unwrap();
        });
    });

    group.finish();
}

/// Benchmark MERGE operations (common in graph DBs)
fn bench_merge_operations(c: &mut Criterion) {
    let _dir = tempfile::tempdir().unwrap();
    let db_path = _dir.path().join("benchmark.db");
    let db = lbug::Database::new(db_path, lbug::SystemConfig::default()).unwrap();
    let conn = lbug::Connection::new(&db).unwrap();
    create_schema(&conn);

    let mut group = c.benchmark_group("merge_operations");
    group.throughput(Throughput::Elements(1));

    group.bench_function("merge_create", |b| {
        b.iter(|| {
            let counter = GLOBAL_COUNTER.fetch_add(1, Ordering::Relaxed);
            let query = format!(
                "MERGE (n:Entity {{id: '{}'}}) SET n.name = 'Entity {}', n.created_at = {}",
                counter, counter, counter
            );
            conn.query(black_box(&query)).unwrap();
        });
    });

    group.bench_function("merge_update", |b| {
        // Pre-create entity
        conn.query("MERGE (n:Entity {id: 'static'}) SET n.name = 'Static', n.created_at = 0").unwrap();

        b.iter(|| {
            let counter = GLOBAL_COUNTER.fetch_add(1, Ordering::Relaxed);
            let query = format!(
                "MERGE (n:Entity {{id: 'static'}}) SET n.name = 'Updated {}', n.created_at = {}",
                counter, counter
            );
            conn.query(black_box(&query)).unwrap();
        });
    });

    group.finish();
}

/// Benchmark read throughput with different result sizes
fn bench_read_throughput(c: &mut Criterion) {
    let _dir = tempfile::tempdir().unwrap();
    let db_path = _dir.path().join("benchmark.db");
    let db = lbug::Database::new(db_path, lbug::SystemConfig::default()).unwrap();
    let conn = lbug::Connection::new(&db).unwrap();
    create_schema(&conn);

    // Insert 10k entities
    for i in 0..10000 {
        conn.query(&format!(
            "CREATE (n:Entity {{id: '{}', name: 'Entity {}', created_at: {}}})",
            i, i, i
        )).unwrap();
    }

    let mut group = c.benchmark_group("read_throughput");

    for limit in [10, 100, 1000, 10000] {
        group.throughput(Throughput::Elements(limit as u64));

        group.bench_with_input(
            BenchmarkId::new("scan_limit", limit),
            &limit,
            |b, &lim| {
                b.iter(|| {
                    let query = format!("MATCH (n:Entity) RETURN n LIMIT {}", lim);
                    let result = conn.query(black_box(&query)).unwrap();
                    let count = consume_result(result);
                    assert_eq!(count, lim);
                });
            },
        );
    }

    group.finish();
}

/// Benchmark complex queries (joins)
fn bench_complex_queries(c: &mut Criterion) {
    let _dir = tempfile::tempdir().unwrap();
    let db_path = _dir.path().join("benchmark.db");
    let db = lbug::Database::new(db_path, lbug::SystemConfig::default()).unwrap();
    let conn = lbug::Connection::new(&db).unwrap();
    create_schema(&conn);

    // Create entities and relationships
    for i in 0..100 {
        conn.query(&format!(
            "CREATE (n:Entity {{id: '{}', name: 'Entity {}', created_at: {}}})",
            i, i, i
        )).unwrap();
    }

    for i in 0..50 {
        let from = i;
        let to = (i + 1) % 100;
        conn.query(&format!(
            "MATCH (a:Entity {{id: '{}'}}), (b:Entity {{id: '{}'}}) CREATE (a)-[:CONNECTED_TO {{weight: {}.0}}]->(b)",
            from, to, i
        )).unwrap();
    }

    let mut group = c.benchmark_group("complex_queries");
    group.throughput(Throughput::Elements(1));

    group.bench_function("two_hop_traversal", |b| {
        b.iter(|| {
            let result = conn.query(black_box(
                "MATCH (a:Entity {id: '0'})-[:CONNECTED_TO*1..2]->(b:Entity) RETURN DISTINCT b"
            )).unwrap();
            consume_result(result);
        });
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_simple_match,
    bench_simple_writes,
    bench_write_throughput,
    bench_transaction_throughput,
    bench_relationship_creation,
    bench_merge_operations,
    bench_read_throughput,
    bench_complex_queries,
);
criterion_main!(benches);
