"""
Strana / graphd demo — exercises the major protocol features.

Prerequisites:
    pip install websockets requests

Usage:
    # Start graphd (no auth for the demo):
    graphd --data-dir /tmp/demo-data

    # Run this script:
    python examples/demo.py

    # Or with auth:
    graphd --token my-secret --data-dir /tmp/demo-data
    python examples/demo.py --token my-secret

    # Custom port:
    python examples/demo.py --port 7688
"""

from __future__ import annotations

import argparse
import asyncio
import json
import sys

import requests
import websockets


# ─── Helpers ───

def pp(label: str, msg: dict):
    print(f"  {label}: {json.dumps(msg, indent=2)}")


async def send(ws, msg: dict) -> dict:
    """Send a JSON message over WebSocket, receive and return the response."""
    raw = json.dumps(msg)
    await ws.send(raw)
    resp = json.loads(await ws.recv())
    return resp


# ─── WebSocket demos ───

async def demo_ws(host: str, port: int, token: str | None):
    url = f"ws://{host}:{port}/ws"
    print(f"\n{'='*60}")
    print(f"WebSocket demo — {url}")
    print(f"{'='*60}")

    async with websockets.connect(url) as ws:
        # 1. Hello / auth
        print("\n1. Connect + auth")
        hello = {"type": "hello"}
        if token:
            hello["token"] = token
        resp = await send(ws, hello)
        pp("hello_ok", resp)

        # 2. Schema setup via batch
        print("\n2. Schema setup (batch)")
        resp = await send(ws, {
            "type": "batch",
            "request_id": "setup",
            "statements": [
                {"query": "CREATE NODE TABLE IF NOT EXISTS Person(name STRING, age INT64, PRIMARY KEY(name))"},
                {"query": "CREATE NODE TABLE IF NOT EXISTS City(name STRING, PRIMARY KEY(name))"},
                {"query": "CREATE REL TABLE IF NOT EXISTS LIVES_IN(FROM Person TO City)"},
                {"query": "CREATE REL TABLE IF NOT EXISTS KNOWS(FROM Person TO Person, since INT64)"},
            ],
        })
        pp("batch_result", resp)

        # 3. Insert data with params
        print("\n3. Insert data (execute with params)")
        people = [
            ("Alice", 30), ("Bob", 42), ("Carol", 28),
            ("Dave", 35), ("Eve", 22), ("Frank", 50),
        ]
        for name, age in people:
            resp = await send(ws, {
                "type": "execute",
                "query": "MERGE (:Person {name: $name, age: $age})",
                "params": {"name": name, "age": age},
            })
        print(f"  Inserted {len(people)} people")

        cities = ["New York", "London", "Tokyo"]
        for city in cities:
            await send(ws, {
                "type": "execute",
                "query": "MERGE (:City {name: $name})",
                "params": {"name": city},
            })
        print(f"  Inserted {len(cities)} cities")

        # Relationships
        edges = [
            ("Alice", "New York"), ("Bob", "London"), ("Carol", "Tokyo"),
            ("Dave", "New York"), ("Eve", "London"), ("Frank", "Tokyo"),
        ]
        for person, city in edges:
            await send(ws, {
                "type": "execute",
                "query": "MATCH (p:Person {name: $p}), (c:City {name: $c}) MERGE (p)-[:LIVES_IN]->(c)",
                "params": {"p": person, "c": city},
            })

        knows = [
            ("Alice", "Bob", 2020), ("Alice", "Carol", 2021),
            ("Bob", "Dave", 2019), ("Carol", "Eve", 2022),
            ("Dave", "Frank", 2018),
        ]
        for a, b, since in knows:
            await send(ws, {
                "type": "execute",
                "query": "MATCH (a:Person {name: $a}), (b:Person {name: $b}) MERGE (a)-[:KNOWS {since: $since}]->(b)",
                "params": {"a": a, "b": b, "since": since},
            })
        print(f"  Created {len(edges)} LIVES_IN + {len(knows)} KNOWS relationships")

        # 4. Query — return scalars
        print("\n4. Query — scalars")
        resp = await send(ws, {
            "type": "execute",
            "query": "MATCH (p:Person) WHERE p.age > $min RETURN p.name, p.age ORDER BY p.age DESC",
            "params": {"min": 25},
            "request_id": "q-scalars",
        })
        pp("result", resp)

        # 5. Query — return full nodes and relationships
        print("\n5. Query — nodes + rels")
        resp = await send(ws, {
            "type": "execute",
            "query": "MATCH (a:Person)-[r:KNOWS]->(b:Person) RETURN a, r, b LIMIT 3",
            "request_id": "q-nodes",
        })
        pp("result", resp)

        # 6. Streaming cursors
        print("\n6. Streaming (fetch_size=2)")
        resp = await send(ws, {
            "type": "execute",
            "query": "MATCH (p:Person) RETURN p.name ORDER BY p.name",
            "fetch_size": 2,
            "request_id": "stream-1",
        })
        pp("batch 1", resp)
        batch = 1

        while resp.get("has_more"):
            stream_id = resp["stream_id"]
            resp = await send(ws, {
                "type": "fetch",
                "stream_id": stream_id,
            })
            batch += 1
            pp(f"batch {batch}", resp)

        print(f"  Streamed {batch} batches total")

        # 7. Streaming — early close
        print("\n7. Streaming — early close_stream")
        resp = await send(ws, {
            "type": "execute",
            "query": "MATCH (p:Person) RETURN p.name ORDER BY p.name",
            "fetch_size": 1,
        })
        pp("first batch", resp)
        if resp.get("has_more"):
            close_resp = await send(ws, {
                "type": "close_stream",
                "stream_id": resp["stream_id"],
            })
            pp("close_stream", close_resp)

        # 8. Transactions — commit
        print("\n8. Transaction — commit")
        resp = await send(ws, {"type": "begin", "request_id": "tx1"})
        pp("begin", resp)

        resp = await send(ws, {
            "type": "execute",
            "query": "MERGE (:Person {name: 'Grace', age: 29})",
        })
        pp("insert", resp)

        resp = await send(ws, {"type": "commit", "request_id": "tx1"})
        pp("commit", resp)

        # Verify Grace exists
        resp = await send(ws, {
            "type": "execute",
            "query": "MATCH (p:Person {name: 'Grace'}) RETURN p.name, p.age",
        })
        pp("verify", resp)

        # 9. Transactions — rollback
        print("\n9. Transaction — rollback")
        resp = await send(ws, {"type": "begin"})
        pp("begin", resp)

        resp = await send(ws, {
            "type": "execute",
            "query": "MERGE (:Person {name: 'Heidi', age: 99})",
        })
        pp("insert", resp)

        resp = await send(ws, {"type": "rollback"})
        pp("rollback", resp)

        # Verify Heidi does NOT exist
        resp = await send(ws, {
            "type": "execute",
            "query": "MATCH (p:Person {name: 'Heidi'}) RETURN p.name",
        })
        pp("verify (should be empty)", resp)

        # 10. Error handling
        print("\n10. Error handling")
        resp = await send(ws, {
            "type": "execute",
            "query": "THIS IS NOT VALID CYPHER",
            "request_id": "bad-query",
        })
        pp("error", resp)

        # 11. Graceful close
        print("\n11. Graceful close")
        resp = await send(ws, {"type": "close"})
        pp("close", resp)


# ─── HTTP demos ───

def demo_http(host: str, port: int, token: str | None):
    base = f"http://{host}:{port}"
    headers = {}
    if token:
        headers["Authorization"] = f"Bearer {token}"

    print(f"\n{'='*60}")
    print(f"HTTP demo — {base}")
    print(f"{'='*60}")

    # POST /v1/execute
    print("\n12. HTTP execute")
    resp = requests.post(
        f"{base}/v1/execute",
        json={"query": "MATCH (p:Person) RETURN p.name, p.age ORDER BY p.age LIMIT 3"},
        headers=headers,
    )
    pp("result", resp.json())

    # POST /v1/batch
    print("\n13. HTTP batch")
    resp = requests.post(
        f"{base}/v1/batch",
        json={
            "statements": [
                {"query": "MATCH (p:Person) RETURN count(p) AS total"},
                {"query": "MATCH ()-[r:KNOWS]->() RETURN count(r) AS total"},
            ]
        },
        headers=headers,
    )
    pp("batch_result", resp.json())

    # POST /v1/pipeline (transactional batch)
    print("\n14. HTTP pipeline (transactional)")
    resp = requests.post(
        f"{base}/v1/pipeline",
        json={
            "statements": [
                {"query": "MERGE (:Person {name: 'Ivan', age: 33})"},
                {"query": "MATCH (p:Person {name: 'Ivan'}) RETURN p.name, p.age"},
            ]
        },
        headers=headers,
    )
    pp("pipeline_result", resp.json())

    # Pipeline with error — shows rollback
    print("\n15. HTTP pipeline — error + rollback")
    resp = requests.post(
        f"{base}/v1/pipeline",
        json={
            "statements": [
                {"query": "MERGE (:Person {name: 'Judy', age: 44})"},
                {"query": "INVALID QUERY"},
            ]
        },
        headers=headers,
    )
    pp("pipeline_result (Judy rolled back)", resp.json())

    # Verify Judy was rolled back
    resp = requests.post(
        f"{base}/v1/execute",
        json={"query": "MATCH (p:Person {name: 'Judy'}) RETURN p.name"},
        headers=headers,
    )
    pp("verify Judy absent", resp.json())

    # Health check
    print("\n16. Health check")
    resp = requests.get(f"{base}/health")
    print(f"  GET /health: {resp.status_code} {resp.text}")


# ─── Main ───

def main():
    parser = argparse.ArgumentParser(description="Strana / graphd demo")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=7688)
    parser.add_argument("--token", default=None)
    args = parser.parse_args()

    try:
        asyncio.run(demo_ws(args.host, args.port, args.token))
        demo_http(args.host, args.port, args.token)
    except ConnectionRefusedError:
        print(f"\nError: cannot connect to graphd at {args.host}:{args.port}", file=sys.stderr)
        print("Start it with: graphd --data-dir /tmp/demo-data", file=sys.stderr)
        sys.exit(1)

    print(f"\n{'='*60}")
    print("Demo complete.")
    print(f"{'='*60}")


if __name__ == "__main__":
    main()
