#!/usr/bin/env node
/**
 * Neo4j JavaScript Driver Compatibility Tests
 *
 * Tests graphd's Bolt protocol implementation with the official Neo4j JavaScript driver.
 */

const neo4j = require('neo4j-driver');

const BOLT_URI = process.env.GRAPHD_BOLT_URI || 'bolt://localhost:7687';
const TEST_USER = 'neo4j';
const TEST_PASSWORD = process.env.GRAPHD_TOKEN || '';

let driver;
let session;
let testsPassed = 0;
let testsFailed = 0;

function log(message) {
    console.log(`[JS] ${message}`);
}

function assert(condition, message) {
    if (!condition) {
        throw new Error(`Assertion failed: ${message}`);
    }
}

async function test(name, fn) {
    try {
        log(`Testing: ${name}`);
        await fn();
        testsPassed++;
        log(`✓ ${name}`);
    } catch (error) {
        testsFailed++;
        log(`✗ ${name}: ${error.message}`);
        throw error;
    }
}

async function setup() {
    log('Setting up driver...');
    driver = neo4j.driver(BOLT_URI, neo4j.auth.basic(TEST_USER, TEST_PASSWORD));
    session = driver.session();

    // Create schema if needed
    try {
        await session.run('CREATE NODE TABLE JsTestNode(id STRING, name STRING, value INT64, count INT64, tx BOOLEAN, since INT64, PRIMARY KEY(id))');
        log('Created JsTestNode table');
    } catch (e) {
        // Table might already exist, that's OK
    }

    // Drop existing KNOWS table if it exists (might be from other test suite)
    try {
        await session.run('DROP TABLE KNOWS');
    } catch (e) {
        // Table might not exist, that's OK
    }

    try {
        await session.run('CREATE REL TABLE KNOWS(FROM JsTestNode TO JsTestNode, since INT64)');
        log('Created KNOWS relationship table');
    } catch (e) {
        log(`Error creating KNOWS table: ${e.message}`);
        throw e;
    }

    // Clean up any existing test data
    await session.run('MATCH (n:JsTestNode) DETACH DELETE n');
    log('Setup complete');
}

async function teardown() {
    log('Tearing down...');
    if (session) {
        await session.run('MATCH (n:JsTestNode) DETACH DELETE n');
        await session.close();
    }
    if (driver) {
        await driver.close();
    }
    log('Teardown complete');
}

async function testBasicConnection() {
    await test('Basic connection', async () => {
        const result = await session.run('RETURN 1 AS num');
        const record = result.records[0];
        assert(record.get('num').toNumber() === 1, 'Expected 1');
    });
}

async function testCreateNode() {
    await test('Create node', async () => {
        const result = await session.run(
            'CREATE (n:JsTestNode {id: $id, name: $name}) RETURN n',
            { id: 'js-1', name: 'JavaScript Test Node' }
        );
        const node = result.records[0].get('n');
        assert(node.properties.id === 'js-1', 'Expected id to match');
        assert(node.properties.name === 'JavaScript Test Node', 'Expected name to match');
    });
}

async function testReadNode() {
    await test('Read node', async () => {
        // Create a node first
        await session.run(
            'CREATE (n:JsTestNode {id: $id, value: $value})',
            { id: 'js-read', value: 42 }
        );

        // Read it back
        const result = await session.run(
            'MATCH (n:JsTestNode {id: $id}) RETURN n.value AS value',
            { id: 'js-read' }
        );
        const value = result.records[0].get('value').toNumber();
        assert(value === 42, 'Expected value to be 42');
    });
}

async function testUpdateNode() {
    await test('Update node', async () => {
        // Create
        await session.run(
            'CREATE (n:JsTestNode {id: $id, count: $count})',
            { id: 'js-update', count: 1 }
        );

        // Update
        await session.run(
            'MATCH (n:JsTestNode {id: $id}) SET n.count = $newCount',
            { id: 'js-update', newCount: 2 }
        );

        // Verify
        const result = await session.run(
            'MATCH (n:JsTestNode {id: $id}) RETURN n.count AS count',
            { id: 'js-update' }
        );
        const count = result.records[0].get('count').toNumber();
        assert(count === 2, 'Expected count to be 2');
    });
}

async function testDeleteNode() {
    await test('Delete node', async () => {
        // Create
        await session.run(
            'CREATE (n:JsTestNode {id: $id})',
            { id: 'js-delete' }
        );

        // Delete
        await session.run(
            'MATCH (n:JsTestNode {id: $id}) DELETE n',
            { id: 'js-delete' }
        );

        // Verify deleted
        const result = await session.run(
            'MATCH (n:JsTestNode {id: $id}) RETURN count(n) AS count',
            { id: 'js-delete' }
        );
        const count = result.records[0].get('count').toNumber();
        assert(count === 0, 'Expected node to be deleted');
    });
}

async function testTransaction() {
    await test('Transaction commit', async () => {
        const tx = session.beginTransaction();
        try {
            await tx.run(
                'CREATE (n:JsTestNode {id: $id, tx: true})',
                { id: 'js-tx-commit' }
            );
            await tx.commit();
        } catch (error) {
            await tx.rollback();
            throw error;
        }

        // Verify committed
        const result = await session.run(
            'MATCH (n:JsTestNode {id: $id}) RETURN n.tx AS tx',
            { id: 'js-tx-commit' }
        );
        assert(result.records[0].get('tx') === true, 'Expected transaction to commit');
    });
}

async function testTransactionRollback() {
    await test('Transaction rollback', async () => {
        const tx = session.beginTransaction();
        try {
            await tx.run(
                'CREATE (n:JsTestNode {id: $id})',
                { id: 'js-tx-rollback' }
            );
            await tx.rollback();
        } catch (error) {
            await tx.rollback();
            throw error;
        }

        // Verify rolled back
        const result = await session.run(
            'MATCH (n:JsTestNode {id: $id}) RETURN count(n) AS count',
            { id: 'js-tx-rollback' }
        );
        const count = result.records[0].get('count').toNumber();
        assert(count === 0, 'Expected transaction to rollback');
    });
}

async function testRelationships() {
    await test('Create and query relationships', async () => {
        // Create nodes and relationship
        await session.run(`
            CREATE (a:JsTestNode {id: 'js-rel-a'}),
                   (b:JsTestNode {id: 'js-rel-b'}),
                   (a)-[:KNOWS {since: 2024}]->(b)
        `);

        // Query relationship
        const result = await session.run(`
            MATCH (a:JsTestNode {id: 'js-rel-a'})-[r:KNOWS]->(b:JsTestNode {id: 'js-rel-b'})
            RETURN r.since AS since
        `);
        const since = result.records[0].get('since').toNumber();
        assert(since === 2024, 'Expected since to be 2024');
    });
}

async function testBatchOperations() {
    await test('Batch operations', async () => {
        // Create multiple nodes in one query
        await session.run(`
            UNWIND range(1, 10) AS i
            CREATE (n:JsTestNode {id: 'js-batch-' + CAST(i AS STRING), value: i})
        `);

        // Count them
        const result = await session.run(
            'MATCH (n:JsTestNode) WHERE n.id STARTS WITH "js-batch-" RETURN count(n) AS count'
        );
        const count = result.records[0].get('count').toNumber();
        assert(count === 10, 'Expected 10 nodes');
    });
}

async function testParameterTypes() {
    await test('Parameter types', async () => {
        const result = await session.run(`
            WITH $stringParam AS s, $intParam AS i, $floatParam AS f, $boolParam AS b
            RETURN s, i, f, b
        `, {
            stringParam: 'test',
            intParam: 42,
            floatParam: 3.14,
            boolParam: true
        });

        const record = result.records[0];
        assert(record.get('s') === 'test', 'String param mismatch');

        const intVal = record.get('i');
        const intNum = typeof intVal === 'number' ? intVal : intVal.toNumber();
        assert(intNum === 42, 'Int param mismatch');

        assert(Math.abs(record.get('f') - 3.14) < 0.01, 'Float param mismatch');
        assert(record.get('b') === true, 'Bool param mismatch');
    });
}

async function main() {
    try {
        await setup();

        await testBasicConnection();
        await testCreateNode();
        await testReadNode();
        await testUpdateNode();
        await testDeleteNode();
        await testTransaction();
        await testTransactionRollback();
        await testRelationships();
        await testBatchOperations();
        await testParameterTypes();

        await teardown();

        log(`\nResults: ${testsPassed} passed, ${testsFailed} failed`);
        process.exit(testsFailed > 0 ? 1 : 0);
    } catch (error) {
        log(`\nFatal error: ${error.message}`);
        process.exit(1);
    }
}

main();
