package main

// Neo4j Go Driver Compatibility Tests
//
// Tests graphd's Bolt protocol implementation with the official Neo4j Go driver.

import (
	"context"
	"fmt"
	"os"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
)

var (
	boltURI  = getEnv("GRAPHD_BOLT_URI", "bolt://localhost:7687")
	testUser = "neo4j"
	testPass = getEnv("GRAPHD_TOKEN", "")

	testsPassed = 0
	testsFailed = 0
)

func getEnv(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}

func log(message string) {
	fmt.Printf("[Go] %s\n", message)
}

func assert(condition bool, message string) {
	if !condition {
		panic(fmt.Sprintf("Assertion failed: %s", message))
	}
}

func test(name string, fn func(ctx context.Context, session neo4j.SessionWithContext)) {
	defer func() {
		if r := recover(); r != nil {
			testsFailed++
			log(fmt.Sprintf("✗ %s: %v", name, r))
		}
	}()

	log(fmt.Sprintf("Testing: %s", name))
	ctx := context.Background()

	driver, err := neo4j.NewDriverWithContext(
		boltURI,
		neo4j.BasicAuth(testUser, testPass, ""),
	)
	if err != nil {
		panic(err)
	}
	defer driver.Close(ctx)

	session := driver.NewSession(ctx, neo4j.SessionConfig{})
	defer session.Close(ctx)

	fn(ctx, session)
	testsPassed++
	log(fmt.Sprintf("✓ %s", name))
}

func cleanup(ctx context.Context, session neo4j.SessionWithContext) {
	session.Run(ctx, "MATCH (n:GoTestNode) DETACH DELETE n", nil)
}

func testBasicConnection(ctx context.Context, session neo4j.SessionWithContext) {
	result, err := session.Run(ctx, "RETURN 1 AS num", nil)
	if err != nil {
		panic(err)
	}

	record, err := result.Single(ctx)
	if err != nil {
		panic(err)
	}

	num, _ := record.Get("num")
	assert(num.(int64) == 1, "Expected 1")
}

func testCreateNode(ctx context.Context, session neo4j.SessionWithContext) {
	defer cleanup(ctx, session)

	result, err := session.Run(ctx,
		"CREATE (n:GoTestNode {id: $id, name: $name}) RETURN n",
		map[string]any{
			"id":   "go-1",
			"name": "Go Test Node",
		})
	if err != nil {
		panic(err)
	}

	record, err := result.Single(ctx)
	if err != nil {
		panic(err)
	}

	node, _ := record.Get("n")
	// Handle both neo4j.Node and map[string]interface{}
	var id, name string
	switch n := node.(type) {
	case neo4j.Node:
		id = n.Props["id"].(string)
		name = n.Props["name"].(string)
	case map[string]interface{}:
		// Check if properties are nested under "properties" key
		if props, ok := n["properties"].(map[string]interface{}); ok {
			id = props["id"].(string)
			name = props["name"].(string)
		} else {
			// Properties at top level
			id = n["id"].(string)
			name = n["name"].(string)
		}
	default:
		panic(fmt.Sprintf("Unexpected node type: %T", node))
	}
	assert(id == "go-1", "Expected id to match")
	assert(name == "Go Test Node", "Expected name to match")
}

func testReadNode(ctx context.Context, session neo4j.SessionWithContext) {
	defer cleanup(ctx, session)

	// Create
	_, err := session.Run(ctx,
		"CREATE (n:GoTestNode {id: $id, value: $value})",
		map[string]any{"id": "go-read", "value": 42})
	if err != nil {
		panic(err)
	}

	// Read
	result, err := session.Run(ctx,
		"MATCH (n:GoTestNode {id: $id}) RETURN n.value AS value",
		map[string]any{"id": "go-read"})
	if err != nil {
		panic(err)
	}

	record, err := result.Single(ctx)
	if err != nil {
		panic(err)
	}

	value, _ := record.Get("value")
	assert(value.(int64) == 42, "Expected value to be 42")
}

func testUpdateNode(ctx context.Context, session neo4j.SessionWithContext) {
	defer cleanup(ctx, session)

	// Create
	_, err := session.Run(ctx,
		"CREATE (n:GoTestNode {id: $id, count: $count})",
		map[string]any{"id": "go-update", "count": 1})
	if err != nil {
		panic(err)
	}

	// Update
	_, err = session.Run(ctx,
		"MATCH (n:GoTestNode {id: $id}) SET n.count = $newCount",
		map[string]any{"id": "go-update", "newCount": 2})
	if err != nil {
		panic(err)
	}

	// Verify
	result, err := session.Run(ctx,
		"MATCH (n:GoTestNode {id: $id}) RETURN n.count AS count",
		map[string]any{"id": "go-update"})
	if err != nil {
		panic(err)
	}

	record, err := result.Single(ctx)
	if err != nil {
		panic(err)
	}

	count, _ := record.Get("count")
	assert(count.(int64) == 2, "Expected count to be 2")
}

func testDeleteNode(ctx context.Context, session neo4j.SessionWithContext) {
	defer cleanup(ctx, session)

	// Create
	_, err := session.Run(ctx,
		"CREATE (n:GoTestNode {id: $id})",
		map[string]any{"id": "go-delete"})
	if err != nil {
		panic(err)
	}

	// Delete
	_, err = session.Run(ctx,
		"MATCH (n:GoTestNode {id: $id}) DELETE n",
		map[string]any{"id": "go-delete"})
	if err != nil {
		panic(err)
	}

	// Verify
	result, err := session.Run(ctx,
		"MATCH (n:GoTestNode {id: $id}) RETURN count(n) AS count",
		map[string]any{"id": "go-delete"})
	if err != nil {
		panic(err)
	}

	record, err := result.Single(ctx)
	if err != nil {
		panic(err)
	}

	count, _ := record.Get("count")
	assert(count.(int64) == 0, "Expected node to be deleted")
}

func testTransaction(ctx context.Context, session neo4j.SessionWithContext) {
	defer cleanup(ctx, session)

	_, err := session.ExecuteWrite(ctx, func(tx neo4j.ManagedTransaction) (any, error) {
		_, err := tx.Run(ctx,
			"CREATE (n:GoTestNode {id: $id, tx: true})",
			map[string]any{"id": "go-tx-commit"})
		return nil, err
	})
	if err != nil {
		panic(err)
	}

	// Verify
	result, err := session.Run(ctx,
		"MATCH (n:GoTestNode {id: $id}) RETURN n.tx AS tx",
		map[string]any{"id": "go-tx-commit"})
	if err != nil {
		panic(err)
	}

	record, err := result.Single(ctx)
	if err != nil {
		panic(err)
	}

	tx, _ := record.Get("tx")
	assert(tx.(bool) == true, "Expected transaction to commit")
}

func testRelationships(ctx context.Context, session neo4j.SessionWithContext) {
	defer cleanup(ctx, session)

	// Create nodes and relationship
	_, err := session.Run(ctx, `
		CREATE (a:GoTestNode {id: 'go-rel-a'}),
		       (b:GoTestNode {id: 'go-rel-b'}),
		       (a)-[:KNOWS {since: 2024}]->(b)
	`, nil)
	if err != nil {
		panic(err)
	}

	// Query
	result, err := session.Run(ctx, `
		MATCH (a:GoTestNode {id: 'go-rel-a'})-[r:KNOWS]->(b:GoTestNode {id: 'go-rel-b'})
		RETURN r.since AS since
	`, nil)
	if err != nil {
		panic(err)
	}

	record, err := result.Single(ctx)
	if err != nil {
		panic(err)
	}

	since, _ := record.Get("since")
	assert(since.(int64) == 2024, "Expected since to be 2024")
}

func testBatchOperations(ctx context.Context, session neo4j.SessionWithContext) {
	defer cleanup(ctx, session)

	// Create multiple nodes
	_, err := session.Run(ctx, `
		UNWIND range(1, 10) AS i
		CREATE (n:GoTestNode {id: 'go-batch-' + CAST(i AS STRING), value: i})
	`, nil)
	if err != nil {
		panic(err)
	}

	// Count
	result, err := session.Run(ctx,
		`MATCH (n:GoTestNode) WHERE n.id STARTS WITH "go-batch-" RETURN count(n) AS count`,
		nil)
	if err != nil {
		panic(err)
	}

	record, err := result.Single(ctx)
	if err != nil {
		panic(err)
	}

	count, _ := record.Get("count")
	assert(count.(int64) == 10, "Expected 10 nodes")
}

func testParameterTypes(ctx context.Context, session neo4j.SessionWithContext) {
	result, err := session.Run(ctx, `
		WITH $stringParam AS s, $intParam AS i, $floatParam AS f, $boolParam AS b
		RETURN s, i, f, b
	`, map[string]any{
		"stringParam": "test",
		"intParam":    int64(42),
		"floatParam":  3.14,
		"boolParam":   true,
	})
	if err != nil {
		panic(err)
	}

	record, err := result.Single(ctx)
	if err != nil {
		panic(err)
	}

	s, _ := record.Get("s")
	i, _ := record.Get("i")
	f, _ := record.Get("f")
	b, _ := record.Get("b")

	assert(s.(string) == "test", "String param mismatch")
	assert(i.(int64) == 42, "Int param mismatch")
	assert(f.(float64) > 3.13 && f.(float64) < 3.15, "Float param mismatch")
	assert(b.(bool) == true, "Bool param mismatch")
}

func main() {
	log("Setting up...")

	// Initial setup and cleanup
	ctx := context.Background()
	driver, err := neo4j.NewDriverWithContext(boltURI, neo4j.BasicAuth(testUser, testPass, ""))
	if err != nil {
		log(fmt.Sprintf("Fatal error: %v", err))
		os.Exit(1)
	}
	session := driver.NewSession(ctx, neo4j.SessionConfig{})

	// Create schema if needed
	_, err = session.Run(ctx, "CREATE NODE TABLE GoTestNode(id STRING, name STRING, value INT64, count INT64, tx BOOLEAN, since INT64, PRIMARY KEY(id))", nil)
	if err != nil {
		// Table might already exist, that's OK
		log("GoTestNode table exists or created")
	}

	// Drop existing KNOWS table if it exists (might be from other test suite)
	_, _ = session.Run(ctx, "DROP TABLE KNOWS", nil)

	_, err = session.Run(ctx, "CREATE REL TABLE KNOWS(FROM GoTestNode TO GoTestNode, since INT64)", nil)
	if err != nil {
		log(fmt.Sprintf("Error creating KNOWS table: %v", err))
		os.Exit(1)
	}
	log("KNOWS relationship table created")

	cleanup(ctx, session)
	session.Close(ctx)
	driver.Close(ctx)

	log("Running tests...")

	test("Basic connection", testBasicConnection)
	test("Create node", testCreateNode)
	test("Read node", testReadNode)
	test("Update node", testUpdateNode)
	test("Delete node", testDeleteNode)
	test("Transaction commit", testTransaction)
	test("Relationships", testRelationships)
	test("Batch operations", testBatchOperations)
	test("Parameter types", testParameterTypes)

	log(fmt.Sprintf("\nResults: %d passed, %d failed", testsPassed, testsFailed))

	if testsFailed > 0 {
		os.Exit(1)
	}
}
