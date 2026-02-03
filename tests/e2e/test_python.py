#!/usr/bin/env python3
"""
Neo4j Python Driver Compatibility Tests

Tests graphd's Bolt protocol implementation with the official Neo4j Python driver.
"""

import os
import sys
from neo4j import GraphDatabase

BOLT_URI = os.environ.get('GRAPHD_BOLT_URI', 'bolt://localhost:7687')
TEST_USER = 'neo4j'
TEST_PASSWORD = os.environ.get('GRAPHD_TOKEN', '')

tests_passed = 0
tests_failed = 0

def log(message):
    print(f"[Python] {message}")

def test(name, fn):
    global tests_passed, tests_failed
    try:
        log(f"Testing: {name}")
        fn()
        tests_passed += 1
        log(f"✓ {name}")
    except Exception as e:
        tests_failed += 1
        log(f"✗ {name}: {e}")
        raise

def cleanup(session):
    session.run("MATCH (n:PythonTestNode) DETACH DELETE n")

def test_basic_connection(session):
    result = session.run("RETURN 1 AS num")
    record = result.single()
    assert record["num"] == 1, "Expected 1"

def test_create_node(session):
    cleanup(session)
    result = session.run(
        "CREATE (n:PythonTestNode {id: $id, name: $name}) RETURN n",
        id='python-1',
        name='Python Test Node'
    )
    record = result.single()
    node = record["n"]

    # graphd returns nodes as dicts with 'properties' key
    if isinstance(node, dict):
        props = node.get('properties', node)
    else:
        # neo4j.graph.Node object
        props = dict(node)

    assert props["id"] == "python-1", f"Expected id to match, got {props}"
    assert props["name"] == "Python Test Node", "Expected name to match"

def test_read_node(session):
    cleanup(session)

    # Create
    session.run(
        "CREATE (n:PythonTestNode {id: $id, value: $value})",
        id='python-read',
        value=42
    )

    # Read
    result = session.run(
        "MATCH (n:PythonTestNode {id: $id}) RETURN n.value AS value",
        id='python-read'
    )
    record = result.single()
    assert record["value"] == 42, "Expected value to be 42"

def test_update_node(session):
    cleanup(session)

    # Create
    session.run(
        "CREATE (n:PythonTestNode {id: $id, count: $count})",
        id='python-update',
        count=1
    )

    # Update
    session.run(
        "MATCH (n:PythonTestNode {id: $id}) SET n.count = $newCount",
        id='python-update',
        newCount=2
    )

    # Verify
    result = session.run(
        "MATCH (n:PythonTestNode {id: $id}) RETURN n.count AS count",
        id='python-update'
    )
    record = result.single()
    assert record["count"] == 2, "Expected count to be 2"

def test_delete_node(session):
    cleanup(session)

    # Create
    session.run(
        "CREATE (n:PythonTestNode {id: $id})",
        id='python-delete'
    )

    # Delete
    session.run(
        "MATCH (n:PythonTestNode {id: $id}) DELETE n",
        id='python-delete'
    )

    # Verify
    result = session.run(
        "MATCH (n:PythonTestNode {id: $id}) RETURN count(n) AS count",
        id='python-delete'
    )
    record = result.single()
    assert record["count"] == 0, "Expected node to be deleted"

def test_transaction(session):
    cleanup(session)

    def create_in_tx(tx):
        tx.run(
            "CREATE (n:PythonTestNode {id: $id, tx: true})",
            id='python-tx-commit'
        )

    session.execute_write(create_in_tx)

    # Verify
    result = session.run(
        "MATCH (n:PythonTestNode {id: $id}) RETURN n.tx AS tx",
        id='python-tx-commit'
    )
    record = result.single()
    assert record["tx"] == True, "Expected transaction to commit"

def test_transaction_rollback(session):
    cleanup(session)

    try:
        with session.begin_transaction() as tx:
            tx.run(
                "CREATE (n:PythonTestNode {id: $id})",
                id='python-tx-rollback'
            )
            raise Exception("Force rollback")
    except Exception as e:
        if str(e) != "Force rollback":
            raise

    # Verify node was not created
    result = session.run(
        "MATCH (n:PythonTestNode {id: $id}) RETURN count(n) AS count",
        id='python-tx-rollback'
    )
    record = result.single()
    assert record["count"] == 0, "Expected transaction to rollback"

def test_relationships(session):
    cleanup(session)

    # Create nodes and relationship
    session.run("""
        CREATE (a:PythonTestNode {id: 'python-rel-a'}),
               (b:PythonTestNode {id: 'python-rel-b'}),
               (a)-[:KNOWS {since: 2024}]->(b)
    """)

    # Query
    result = session.run("""
        MATCH (a:PythonTestNode {id: 'python-rel-a'})-[r:KNOWS]->(b:PythonTestNode {id: 'python-rel-b'})
        RETURN r.since AS since
    """)
    record = result.single()
    assert record["since"] == 2024, "Expected since to be 2024"

def test_batch_operations(session):
    cleanup(session)

    # Create multiple nodes
    session.run("""
        UNWIND range(1, 10) AS i
        CREATE (n:PythonTestNode {id: 'python-batch-' + CAST(i AS STRING), value: i})
    """)

    # Count
    result = session.run(
        'MATCH (n:PythonTestNode) WHERE n.id STARTS WITH "python-batch-" RETURN count(n) AS count'
    )
    record = result.single()
    assert record["count"] == 10, "Expected 10 nodes"

def test_parameter_types(session):
    result = session.run("""
        WITH $stringParam AS s, $intParam AS i, $floatParam AS f, $boolParam AS b
        RETURN s, i, f, b
    """,
        stringParam='test',
        intParam=42,
        floatParam=3.14,
        boolParam=True
    )

    record = result.single()
    assert record["s"] == "test", "String param mismatch"
    assert record["i"] == 42, "Int param mismatch"
    assert abs(record["f"] - 3.14) < 0.01, "Float param mismatch"
    assert record["b"] == True, "Bool param mismatch"

def main():
    log("Setting up...")

    driver = GraphDatabase.driver(BOLT_URI, auth=(TEST_USER, TEST_PASSWORD))

    with driver.session() as session:
        # Create schema if needed
        try:
            session.run("CREATE NODE TABLE PythonTestNode(id STRING, name STRING, value INT64, count INT64, tx BOOLEAN, since INT64, PRIMARY KEY(id))")
            log("Created PythonTestNode table")
        except Exception:
            # Table might already exist
            pass

        # Drop and recreate KNOWS table to ensure it references PythonTestNode
        try:
            session.run("DROP TABLE KNOWS")
        except Exception:
            pass

        try:
            session.run("CREATE REL TABLE KNOWS(FROM PythonTestNode TO PythonTestNode, since INT64)")
            log("Created KNOWS relationship table")
        except Exception as e:
            log(f"Error creating KNOWS table: {e}")
            sys.exit(1)

        # Initial cleanup
        cleanup(session)
        log("Setup complete")

        log("Running tests...")

        test("Basic connection", lambda: test_basic_connection(session))
        test("Create node", lambda: test_create_node(session))
        test("Read node", lambda: test_read_node(session))
        test("Update node", lambda: test_update_node(session))
        test("Delete node", lambda: test_delete_node(session))
        test("Transaction commit", lambda: test_transaction(session))
        test("Transaction rollback", lambda: test_transaction_rollback(session))
        test("Relationships", lambda: test_relationships(session))
        test("Batch operations", lambda: test_batch_operations(session))
        test("Parameter types", lambda: test_parameter_types(session))

        # Final cleanup
        cleanup(session)

    driver.close()

    log(f"\nResults: {tests_passed} passed, {tests_failed} failed")

    if tests_failed > 0:
        sys.exit(1)

if __name__ == '__main__':
    main()
