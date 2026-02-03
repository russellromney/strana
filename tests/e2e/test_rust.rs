// Neo4j Rust Driver Compatibility Tests
//
// Tests graphd's Bolt protocol implementation with the official Neo4j Rust driver.

use neo4rs::{BoltMap, BoltNode, Graph};
use std::env;
use std::sync::Arc;

fn bolt_uri() -> String {
    env::var("GRAPHD_BOLT_URI").unwrap_or_else(|_| "bolt://localhost:7687".to_string())
}

fn test_token() -> String {
    env::var("GRAPHD_TOKEN").unwrap_or_default()
}

macro_rules! test {
    ($name:expr, $f:expr) => {{
        println!("[Rust] Testing: {}", $name);
        match $f.await {
            Ok(_) => {
                println!("[Rust] ✓ {}", $name);
                Ok(())
            }
            Err(e) => {
                eprintln!("[Rust] ✗ {}: {:?}", $name, e);
                Err(e)
            }
        }
    }};
}

async fn setup() -> Result<Arc<Graph>, Box<dyn std::error::Error>> {
    let uri = bolt_uri();
    let token = test_token();
    let graph = Arc::new(
        Graph::new(uri, "neo4j", token)
            .await
            .map_err(|e| format!("Failed to connect: {}", e))?,
    );

    // Create schema if needed
    let _ = graph
        .run(neo4rs::query(
            "CREATE NODE TABLE RustTestNode(id STRING, name STRING, value INT64, count INT64, tx BOOLEAN, since INT64, PRIMARY KEY(id))",
        ))
        .await;
    // Ignore error if table already exists

    let _ = graph
        .run(neo4rs::query(
            "CREATE REL TABLE KNOWS(FROM RustTestNode TO RustTestNode, since INT64)",
        ))
        .await;
    // Ignore error if table already exists

    // Initial cleanup
    graph
        .run(neo4rs::query("MATCH (n:RustTestNode) DETACH DELETE n"))
        .await?;

    Ok(graph)
}

async fn cleanup(graph: &Graph) -> Result<(), Box<dyn std::error::Error>> {
    graph
        .run(neo4rs::query("MATCH (n:RustTestNode) DETACH DELETE n"))
        .await?;
    Ok(())
}

async fn test_basic_connection(graph: &Graph) -> Result<(), Box<dyn std::error::Error>> {
    let mut result = graph.execute(neo4rs::query("RETURN 1 AS num")).await?;

    if let Some(row) = result.next().await? {
        let num: i64 = row.get("num")?;
        assert_eq!(num, 1, "Expected 1");
    } else {
        return Err("No result returned".into());
    }

    Ok(())
}

async fn test_create_node(graph: &Graph) -> Result<(), Box<dyn std::error::Error>> {
    let query = neo4rs::query(
        "CREATE (n:RustTestNode {id: $id, name: $name}) RETURN n"
    )
    .param("id", "rust-1")
    .param("name", "Rust Test Node");

    let mut result = graph.execute(query).await?;

    if let Some(row) = result.next().await? {
        let node: BoltNode = row.get("n")?;
        let props = node.get::<BoltMap>("properties")?;
        let id: String = props.get("id")?;
        let name: String = props.get("name")?;
        assert_eq!(id, "rust-1", "Expected id to match");
        assert_eq!(name, "Rust Test Node", "Expected name to match");
    } else {
        return Err("No result returned".into());
    }

    cleanup(graph).await?;
    Ok(())
}

async fn test_read_node(graph: &Graph) -> Result<(), Box<dyn std::error::Error>> {
    // Create
    let create_query = neo4rs::query("CREATE (n:RustTestNode {id: $id, value: $value})")
        .param("id", "rust-read")
        .param("value", 42);
    graph.run(create_query).await?;

    // Read
    let read_query =
        neo4rs::query("MATCH (n:RustTestNode {id: $id}) RETURN n.value AS value")
            .param("id", "rust-read");

    let mut result = graph.execute(read_query).await?;

    if let Some(row) = result.next().await? {
        let value: i64 = row.get("value")?;
        assert_eq!(value, 42, "Expected value to be 42");
    } else {
        return Err("No result returned".into());
    }

    cleanup(graph).await?;
    Ok(())
}

async fn test_update_node(graph: &Graph) -> Result<(), Box<dyn std::error::Error>> {
    // Create
    let create_query = neo4rs::query("CREATE (n:RustTestNode {id: $id, count: $count})")
        .param("id", "rust-update")
        .param("count", 1);
    graph.run(create_query).await?;

    // Update
    let update_query =
        neo4rs::query("MATCH (n:RustTestNode {id: $id}) SET n.count = $newCount")
            .param("id", "rust-update")
            .param("newCount", 2);
    graph.run(update_query).await?;

    // Verify
    let verify_query =
        neo4rs::query("MATCH (n:RustTestNode {id: $id}) RETURN n.count AS count")
            .param("id", "rust-update");

    let mut result = graph.execute(verify_query).await?;

    if let Some(row) = result.next().await? {
        let count: i64 = row.get("count")?;
        assert_eq!(count, 2, "Expected count to be 2");
    } else {
        return Err("No result returned".into());
    }

    cleanup(graph).await?;
    Ok(())
}

async fn test_delete_node(graph: &Graph) -> Result<(), Box<dyn std::error::Error>> {
    // Create
    let create_query = neo4rs::query("CREATE (n:RustTestNode {id: $id})").param("id", "rust-delete");
    graph.run(create_query).await?;

    // Delete
    let delete_query = neo4rs::query("MATCH (n:RustTestNode {id: $id}) DELETE n")
        .param("id", "rust-delete");
    graph.run(delete_query).await?;

    // Verify
    let verify_query =
        neo4rs::query("MATCH (n:RustTestNode {id: $id}) RETURN count(n) AS count")
            .param("id", "rust-delete");

    let mut result = graph.execute(verify_query).await?;

    if let Some(row) = result.next().await? {
        let count: i64 = row.get("count")?;
        assert_eq!(count, 0, "Expected node to be deleted");
    } else {
        return Err("No result returned".into());
    }

    cleanup(graph).await?;
    Ok(())
}

async fn test_transaction(graph: &Graph) -> Result<(), Box<dyn std::error::Error>> {
    let mut txn = graph.start_txn().await?;

    txn.run_queries(vec![neo4rs::query(
        "CREATE (n:RustTestNode {id: $id, tx: true})",
    )
    .param("id", "rust-tx-commit")])
    .await?;

    txn.commit().await?;

    // Verify
    let verify_query =
        neo4rs::query("MATCH (n:RustTestNode {id: $id}) RETURN n.tx AS tx")
            .param("id", "rust-tx-commit");

    let mut result = graph.execute(verify_query).await?;

    if let Some(row) = result.next().await? {
        let tx: bool = row.get("tx")?;
        assert_eq!(tx, true, "Expected transaction to commit");
    } else {
        return Err("No result returned".into());
    }

    cleanup(graph).await?;
    Ok(())
}

async fn test_relationships(graph: &Graph) -> Result<(), Box<dyn std::error::Error>> {
    // Create nodes and relationship
    let create_query = neo4rs::query(
        r#"
        CREATE (a:RustTestNode {id: 'rust-rel-a'}),
               (b:RustTestNode {id: 'rust-rel-b'}),
               (a)-[:KNOWS {since: 2024}]->(b)
        "#,
    );
    graph.run(create_query).await?;

    // Query
    let query_rel = neo4rs::query(
        r#"
        MATCH (a:RustTestNode {id: 'rust-rel-a'})-[r:KNOWS]->(b:RustTestNode {id: 'rust-rel-b'})
        RETURN r.since AS since
        "#,
    );

    let mut result = graph.execute(query_rel).await?;

    if let Some(row) = result.next().await? {
        let since: i64 = row.get("since")?;
        assert_eq!(since, 2024, "Expected since to be 2024");
    } else {
        return Err("No result returned".into());
    }

    cleanup(graph).await?;
    Ok(())
}

async fn test_batch_operations(graph: &Graph) -> Result<(), Box<dyn std::error::Error>> {
    // Create multiple nodes
    let batch_query = neo4rs::query(
        r#"
        UNWIND range(1, 10) AS i
        CREATE (n:RustTestNode {id: 'rust-batch-' + CAST(i AS STRING), value: i})
        "#,
    );
    graph.run(batch_query).await?;

    // Count
    let count_query = neo4rs::query(
        r#"MATCH (n:RustTestNode) WHERE n.id STARTS WITH "rust-batch-" RETURN count(n) AS count"#,
    );

    let mut result = graph.execute(count_query).await?;

    if let Some(row) = result.next().await? {
        let count: i64 = row.get("count")?;
        assert_eq!(count, 10, "Expected 10 nodes");
    } else {
        return Err("No result returned".into());
    }

    cleanup(graph).await?;
    Ok(())
}

async fn test_parameter_types(graph: &Graph) -> Result<(), Box<dyn std::error::Error>> {
    let query = neo4rs::query(
        r#"
        WITH $stringParam AS s, $intParam AS i, $floatParam AS f, $boolParam AS b
        RETURN s, i, f, b
        "#,
    )
    .param("stringParam", "test")
    .param("intParam", 42)
    .param("floatParam", 3.14)
    .param("boolParam", true);

    let mut result = graph.execute(query).await?;

    if let Some(row) = result.next().await? {
        let s: String = row.get("s")?;
        let i: i64 = row.get("i")?;
        let f: f64 = row.get("f")?;
        let b: bool = row.get("b")?;

        assert_eq!(s, "test", "String param mismatch");
        assert_eq!(i, 42, "Int param mismatch");
        assert!((f - 3.14).abs() < 0.01, "Float param mismatch");
        assert_eq!(b, true, "Bool param mismatch");
    } else {
        return Err("No result returned".into());
    }

    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("[Rust] Setting up...");

    let graph = setup().await?;

    println!("[Rust] Running tests...");

    let mut passed = 0;
    let mut failed = 0;

    macro_rules! run_test {
        ($name:expr, $test:expr) => {
            match test!($name, $test(&graph)) {
                Ok(_) => passed += 1,
                Err(_) => failed += 1,
            }
        };
    }

    run_test!("Basic connection", test_basic_connection);
    run_test!("Create node", test_create_node);
    run_test!("Read node", test_read_node);
    run_test!("Update node", test_update_node);
    run_test!("Delete node", test_delete_node);
    run_test!("Transaction commit", test_transaction);
    run_test!("Relationships", test_relationships);
    run_test!("Batch operations", test_batch_operations);
    run_test!("Parameter types", test_parameter_types);

    println!("\n[Rust] Results: {} passed, {} failed", passed, failed);

    if failed > 0 {
        std::process::exit(1);
    }

    Ok(())
}
