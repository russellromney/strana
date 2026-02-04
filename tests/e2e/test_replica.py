#!/usr/bin/env python3
"""
End-to-end test for graphd replica mode.

Tests:
  1. Replica with local file:// source
  2. Replica with S3/Tigris source (if credentials available)
  3. Initial sync (snapshot + journals)
  4. Incremental replication (polling)
  5. Read-only enforcement

Usage:
    # Via Makefile:
    make e2e-replica

    # Direct:
    DYLD_LIBRARY_PATH=./lbug-lib GRAPHD_BINARY=./target/debug/graphd \
        STRANA_ROOT=. python tests/e2e/test_replica.py -v
"""

import os
import shutil
import signal
import socket
import subprocess
import sys
import tempfile
import time
import unittest

from neo4j import GraphDatabase
from neo4j.exceptions import ClientError
import requests

# Import helpers from test_e2e
sys.path.insert(0, os.path.dirname(__file__))
from test_e2e import (
    find_free_ports,
    load_s3_env,
    cleanup_s3_prefix,
    GRAPHD_BINARY,
    STRANA_ROOT,
)


class DatabaseTemplates:
    """
    Manages pre-populated database templates for tests.

    Creates template databases of different sizes (1MB, 10MB, 100MB) once,
    then allows tests to copy them instead of writing nodes from scratch.
    """

    _templates_dir = None
    _templates = {}  # name -> (node_count, has_snapshot)

    @classmethod
    def setup_templates(cls):
        """Create database templates once for all tests."""
        if cls._templates_dir:
            return  # Already set up

        print("\n=== Setting up database templates ===", file=sys.stderr)
        cls._templates_dir = tempfile.mkdtemp(prefix="graphd_templates_")

        # Create 1MB template (~500 nodes, ~2KB each)
        cls._create_template("1mb", 500)

        # Create 10MB template (~5k nodes)
        cls._create_template("10mb", 5000)

        # Create 100MB template (~50k nodes)
        cls._create_template("100mb", 50000)

        print("=== Templates ready ===\n", file=sys.stderr)

    @classmethod
    def _create_template(cls, name, node_count):
        """Create a single template database with snapshot."""
        print(f"Creating template '{name}' with {node_count} nodes...", file=sys.stderr)

        template_dir = os.path.join(cls._templates_dir, name)
        os.makedirs(template_dir)

        bolt_port, http_port = find_free_ports(2)

        # Start graphd
        proc = GraphdProcess(
            data_dir=template_dir,
            bolt_port=bolt_port,
            http_port=http_port,
            journal=True,
        )
        proc.start(timeout=30)

        # Write data
        driver = GraphDatabase.driver(f"bolt://localhost:{bolt_port}")
        try:
            with driver.session() as session:
                session.run("CREATE NODE TABLE Person(id INT64, name STRING, data STRING, PRIMARY KEY(id))")

                batch_size = 500
                for batch_start in range(0, node_count, batch_size):
                    session.run("BEGIN TRANSACTION")
                    for i in range(batch_start, min(batch_start + batch_size, node_count)):
                        # ~300 bytes per node for realistic disk usage
                        data = f"Payload for node {i} " * 15
                        session.run(
                            "CREATE (p:Person {id: $id, name: $name, data: $data})",
                            id=i,
                            name=f"Person{i:05}",
                            data=data,
                        )
                    session.run("COMMIT")
                    if (batch_start + batch_size) % 1000 == 0 and batch_start > 0:
                        print(f"  {batch_start + batch_size}/{node_count} nodes", file=sys.stderr)

            # Take snapshot
            proc.snapshot()
            time.sleep(0.5)

        finally:
            driver.close()
            proc.stop()

        cls._templates[name] = (node_count, True)
        print(f"Template '{name}' ready ({node_count} nodes)", file=sys.stderr)

    @classmethod
    def copy_template(cls, name, dest_dir):
        """
        Copy a template database to destination directory.

        Returns:
            node_count: Number of nodes in the template
        """
        if name not in cls._templates:
            available = ", ".join(cls._templates.keys())
            raise ValueError(f"Template '{name}' not found. Available: {available}")

        template_dir = os.path.join(cls._templates_dir, name)

        # Copy all files from template to destination
        if os.path.exists(dest_dir):
            shutil.rmtree(dest_dir)
        shutil.copytree(template_dir, dest_dir)

        node_count, has_snapshot = cls._templates[name]
        return node_count

    @classmethod
    def cleanup_templates(cls):
        """Remove all template databases."""
        if cls._templates_dir:
            shutil.rmtree(cls._templates_dir, ignore_errors=True)
            cls._templates_dir = None
            cls._templates = {}
            print("=== Templates cleaned up ===", file=sys.stderr)


class GraphdProcess:
    """Manages a graphd subprocess (primary or replica)."""

    def __init__(
        self,
        data_dir,
        bolt_port,
        http_port,
        journal=False,
        s3_bucket=None,
        s3_prefix="",
        replica=False,
        replica_source=None,
        replica_poll_interval="2s",
        extra_env=None,
    ):
        self.data_dir = data_dir
        self.bolt_port = bolt_port
        self.http_port = http_port
        self.proc = None
        self._stdout = ""
        self._stderr = ""

        self.cmd = [
            GRAPHD_BINARY,
            "--data-dir",
            str(data_dir),
            "--bolt-port",
            str(bolt_port),
            "--http-port",
            str(http_port),
            "--bolt-host",
            "127.0.0.1",
            "--http-host",
            "127.0.0.1",
        ]

        if journal:
            self.cmd.append("--journal")
        if s3_bucket:
            self.cmd.extend(["--s3-bucket", s3_bucket])
        if s3_prefix:
            self.cmd.extend(["--s3-prefix", s3_prefix])
        if replica:
            self.cmd.append("--replica")
            if replica_source:
                self.cmd.extend(["--replica-source", replica_source])
            if replica_poll_interval:
                self.cmd.extend(["--replica-poll-interval", replica_poll_interval])

        self.env = dict(os.environ)
        lbug_lib = os.path.join(STRANA_ROOT, "lbug-lib")
        self.env["DYLD_LIBRARY_PATH"] = lbug_lib
        if extra_env:
            self.env.update(extra_env)

    def start(self, timeout=20):
        """Start graphd and wait for it to become ready."""
        print(f"Starting graphd: {' '.join(self.cmd)}", file=sys.stderr)
        self.proc = subprocess.Popen(
            self.cmd,
            env=self.env,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        self._wait_ready(timeout)

    def _wait_ready(self, timeout):
        deadline = time.time() + timeout
        while time.time() < deadline:
            # Check if process died
            if self.proc.poll() is not None:
                self._capture_output()
                raise RuntimeError(
                    f"graphd exited with code {self.proc.returncode}\n"
                    f"STDOUT:\n{self._stdout}\nSTDERR:\n{self._stderr}"
                )

            # Try to connect to HTTP
            try:
                resp = requests.get(
                    f"http://127.0.0.1:{self.http_port}/health", timeout=1
                )
                if resp.status_code == 200:
                    print(
                        f"graphd ready on bolt:{self.bolt_port} http:{self.http_port}",
                        file=sys.stderr,
                    )
                    return
            except requests.exceptions.RequestException:
                pass

            time.sleep(0.2)

        self._capture_output()
        raise TimeoutError(
            f"graphd did not become ready within {timeout}s\n"
            f"STDOUT:\n{self._stdout}\nSTDERR:\n{self._stderr}"
        )

    def _capture_output(self):
        """Read stdout/stderr from the process."""
        if self.proc.stdout:
            self._stdout = self.proc.stdout.read().decode("utf-8", errors="replace")
        if self.proc.stderr:
            self._stderr = self.proc.stderr.read().decode("utf-8", errors="replace")

    def stop(self):
        """Stop graphd gracefully."""
        if self.proc and self.proc.poll() is None:
            self.proc.send_signal(signal.SIGINT)
            try:
                self.proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self.proc.kill()
                self.proc.wait()
            self._capture_output()

    def snapshot(self):
        """Trigger a snapshot via HTTP API."""
        resp = requests.post(
            f"http://127.0.0.1:{self.http_port}/v1/snapshot", timeout=10
        )
        resp.raise_for_status()
        return resp.json()


class TestReplicaFileSource(unittest.TestCase):
    """Test replica mode with local file:// source."""

    def setUp(self):
        """Set up temp directories for primary and replica."""
        self.primary_dir = tempfile.mkdtemp(prefix="graphd_primary_")
        self.replica_dir = tempfile.mkdtemp(prefix="graphd_replica_")
        self.primary_bolt, self.primary_http, self.replica_bolt, self.replica_http = (
            find_free_ports(4)
        )
        self.primary = None
        self.replica = None

    def tearDown(self):
        """Clean up processes and temp directories."""
        if self.replica:
            self.replica.stop()
        if self.primary:
            self.primary.stop()
        shutil.rmtree(self.primary_dir, ignore_errors=True)
        shutil.rmtree(self.replica_dir, ignore_errors=True)

    def test_replica_initial_sync_and_polling(self):
        """
        Test complete replica flow:
        1. Primary writes initial data
        2. Primary takes snapshot
        3. Primary writes more data after snapshot
        4. Replica starts and syncs (should have all data)
        5. Primary writes new data
        6. Replica polls and sees new data
        7. Replica rejects writes
        """
        # 1. Start primary with journal
        print("\n=== Starting primary ===", file=sys.stderr)
        self.primary = GraphdProcess(
            data_dir=self.primary_dir,
            bolt_port=self.primary_bolt,
            http_port=self.primary_http,
            journal=True,
        )
        self.primary.start()

        # 2. Write initial data to primary
        print("=== Creating schema and writing initial data (Alice, Bob) ===", file=sys.stderr)
        primary_driver = GraphDatabase.driver(f"bolt://localhost:{self.primary_bolt}")
        with primary_driver.session() as session:
            # Create node table first
            session.run("CREATE NODE TABLE Person(name STRING, PRIMARY KEY(name))")
            session.run("CREATE (p:Person {name: 'Alice'})")
            session.run("CREATE (p:Person {name: 'Bob'})")

        # 3. Take snapshot
        print("=== Taking snapshot ===", file=sys.stderr)
        self.primary.snapshot()

        # 4. Write more data after snapshot
        print("=== Writing post-snapshot data (Charlie) ===", file=sys.stderr)
        with primary_driver.session() as session:
            session.run("CREATE (p:Person {name: 'Charlie'})")

        primary_driver.close()

        # Give journal a moment to flush
        time.sleep(0.5)

        # 5. Start replica pointing to primary's data directory
        print("=== Starting replica ===", file=sys.stderr)
        self.replica = GraphdProcess(
            data_dir=self.replica_dir,
            bolt_port=self.replica_bolt,
            http_port=self.replica_http,
            replica=True,
            replica_source=f"file://{self.primary_dir}",
            replica_poll_interval="2s",
        )
        self.replica.start(timeout=30)  # Give more time for initial sync

        # 6. Verify replica has all 3 nodes from initial sync
        print("=== Verifying initial sync (expect 3 nodes) ===", file=sys.stderr)
        replica_driver = GraphDatabase.driver(f"bolt://localhost:{self.replica_bolt}")
        try:
            with replica_driver.session() as session:
                result = session.run("MATCH (p:Person) RETURN p.name ORDER BY p.name")
                names = [record["p.name"] for record in result]
                self.assertEqual(
                    names,
                    ["Alice", "Bob", "Charlie"],
                    "Replica should have all 3 nodes after initial sync",
                )
        except Exception as e:
            print(f"=== Query failed: {e} ===", file=sys.stderr)
            # Stop replica to safely capture output (read() blocks on running process)
            self.replica.stop()
            print(self.replica._stderr, file=sys.stderr)
            self.replica = None
            raise

        # 7. Write more data on primary (while replica is running)
        print("=== Writing incremental data (David) ===", file=sys.stderr)
        primary_driver = GraphDatabase.driver(f"bolt://localhost:{self.primary_bolt}")
        with primary_driver.session() as session:
            session.run("CREATE (p:Person {name: 'David'})")
        primary_driver.close()

        # Give journal time to flush
        time.sleep(0.5)

        # 8. Wait for replica poll interval (2s + buffer)
        print("=== Waiting for replica poll (3s) ===", file=sys.stderr)
        time.sleep(3)

        # 9. Verify replica sees new data
        print("=== Verifying incremental sync (expect 4 nodes) ===", file=sys.stderr)
        with replica_driver.session() as session:
            result = session.run("MATCH (p:Person) RETURN p.name ORDER BY p.name")
            names = [record["p.name"] for record in result]
            self.assertEqual(
                names,
                ["Alice", "Bob", "Charlie", "David"],
                "Replica should see incremental data after polling",
            )

        # 10. Verify replica rejects writes
        print("=== Verifying read-only enforcement ===", file=sys.stderr)
        with replica_driver.session() as session:
            with self.assertRaises(ClientError) as ctx:
                session.run("CREATE (p:Person {name: 'Eve'})")
            self.assertIn(
                "ReadOnlyMode", str(ctx.exception), "Replica should reject writes"
            )

        # 11. Verify replica accepts reads
        with replica_driver.session() as session:
            result = session.run("MATCH (p:Person) RETURN count(p) as cnt")
            count = result.single()["cnt"]
            self.assertEqual(count, 4, "Replica should still allow reads")

        replica_driver.close()

        print("=== Test passed! ===", file=sys.stderr)


class TestReplicaS3Source(unittest.TestCase):
    """Test replica mode with S3/Tigris source."""

    def setUp(self):
        """Set up S3 credentials and temp directories."""
        self.s3_env, self.s3_bucket = load_s3_env()
        if not self.s3_env or not self.s3_bucket:
            self.skipTest("S3/Tigris credentials not available in .env.test")

        self.primary_dir = tempfile.mkdtemp(prefix="graphd_primary_s3_")
        self.replica_dir = tempfile.mkdtemp(prefix="graphd_replica_s3_")
        self.test_prefix = f"test-replica-{int(time.time())}/"
        self.primary_bolt, self.primary_http, self.replica_bolt, self.replica_http = (
            find_free_ports(4)
        )
        self.primary = None
        self.replica = None

    def tearDown(self):
        """Clean up processes, temp directories, and S3 objects."""
        if self.replica:
            self.replica.stop()
        if self.primary:
            self.primary.stop()
        shutil.rmtree(self.primary_dir, ignore_errors=True)
        shutil.rmtree(self.replica_dir, ignore_errors=True)
        if self.s3_env and self.s3_bucket:
            cleanup_s3_prefix(self.s3_bucket, self.test_prefix, self.s3_env)

    def test_replica_s3_initial_sync_and_polling(self):
        """
        Test replica with S3/Tigris source:
        1. Primary writes to S3
        2. Replica downloads from S3
        3. Incremental replication works
        """
        # 1. Start primary with journal + S3 uploads
        print("\n=== Starting primary with S3 uploads ===", file=sys.stderr)
        self.primary = GraphdProcess(
            data_dir=self.primary_dir,
            bolt_port=self.primary_bolt,
            http_port=self.primary_http,
            journal=True,
            s3_bucket=self.s3_bucket,
            s3_prefix=self.test_prefix,
            extra_env=self.s3_env,
        )
        self.primary.start()

        # 2. Write initial data
        print("=== Creating schema and writing initial data (Alice, Bob) ===", file=sys.stderr)
        primary_driver = GraphDatabase.driver(f"bolt://localhost:{self.primary_bolt}")
        with primary_driver.session() as session:
            # Create node table first
            session.run("CREATE NODE TABLE Person(name STRING, PRIMARY KEY(name))")
            session.run("CREATE (p:Person {name: 'Alice'})")
            session.run("CREATE (p:Person {name: 'Bob'})")

        # 3. Take snapshot (uploads to S3)
        print("=== Taking snapshot (uploads to S3) ===", file=sys.stderr)
        self.primary.snapshot()

        # 4. Write more data after snapshot (goes to journal, uploaded to S3)
        print("=== Writing post-snapshot data (Charlie) ===", file=sys.stderr)
        with primary_driver.session() as session:
            session.run("CREATE (p:Person {name: 'Charlie'})")

        primary_driver.close()

        # Give S3 upload time to complete
        time.sleep(2)

        # 5. Start replica downloading from S3
        print("=== Starting replica (S3 source) ===", file=sys.stderr)
        s3_source = f"s3://{self.s3_bucket}/{self.test_prefix}"
        self.replica = GraphdProcess(
            data_dir=self.replica_dir,
            bolt_port=self.replica_bolt,
            http_port=self.replica_http,
            replica=True,
            replica_source=s3_source,
            replica_poll_interval="2s",
            extra_env=self.s3_env,
        )
        self.replica.start(timeout=30)

        # 6. Verify replica has all 3 nodes (snapshot + journal)
        print("=== Verifying S3 initial sync (expect 3 nodes) ===", file=sys.stderr)
        replica_driver = GraphDatabase.driver(f"bolt://localhost:{self.replica_bolt}")
        try:
            with replica_driver.session() as session:
                result = session.run("MATCH (p:Person) RETURN p.name ORDER BY p.name")
                names = [record["p.name"] for record in result]
                self.assertEqual(
                    names,
                    ["Alice", "Bob", "Charlie"],
                    "Replica should download all data from S3",
                )
        except Exception as e:
            print(f"=== Query failed: {e} ===", file=sys.stderr)
            # Stop replica to safely capture output (read() blocks on running process)
            self.replica.stop()
            print(self.replica._stderr, file=sys.stderr)
            self.replica = None
            raise

        # 7. Write incremental data on primary
        print("=== Writing incremental data (David) ===", file=sys.stderr)
        primary_driver = GraphDatabase.driver(f"bolt://localhost:{self.primary_bolt}")
        with primary_driver.session() as session:
            session.run("CREATE (p:Person {name: 'David'})")
        primary_driver.close()

        # Wait for S3 upload + replica poll
        time.sleep(4)

        # 8. Verify replica sees new data from S3
        print("=== Verifying S3 incremental sync (expect 4 nodes) ===", file=sys.stderr)
        with replica_driver.session() as session:
            result = session.run("MATCH (p:Person) RETURN p.name ORDER BY p.name")
            names = [record["p.name"] for record in result]
            self.assertEqual(
                names,
                ["Alice", "Bob", "Charlie", "David"],
                "Replica should poll S3 and see new data",
            )

        # 9. Verify read-only enforcement
        print("=== Verifying S3 replica read-only ===", file=sys.stderr)
        with replica_driver.session() as session:
            with self.assertRaises(ClientError) as ctx:
                session.run("CREATE (p:Person {name: 'Eve'})")
            self.assertIn("ReadOnlyMode", str(ctx.exception))

        replica_driver.close()

        print("=== S3 test passed! ===", file=sys.stderr)


class TestReplicaAdvanced(unittest.TestCase):
    """Advanced replica tests for edge cases and reliability."""

    @classmethod
    def setUpClass(cls):
        """Set up database templates once for all tests in this class."""
        DatabaseTemplates.setup_templates()

    @classmethod
    def tearDownClass(cls):
        """Clean up database templates after all tests."""
        DatabaseTemplates.cleanup_templates()

    def setUp(self):
        """Set up temp directories for primary and replica."""
        self.primary_dir = tempfile.mkdtemp(prefix="graphd_primary_adv_")
        self.replica_dir = tempfile.mkdtemp(prefix="graphd_replica_adv_")
        self.primary_bolt, self.primary_http, self.replica_bolt, self.replica_http = (
            find_free_ports(4)
        )
        self.primary = None
        self.replica = None

    def tearDown(self):
        """Clean up processes and temp directories."""
        if self.replica:
            self.replica.stop()
        if self.primary:
            self.primary.stop()
        shutil.rmtree(self.primary_dir, ignore_errors=True)
        shutil.rmtree(self.replica_dir, ignore_errors=True)

    def test_unsealed_segment_incremental_appends(self):
        """
        Test that replica picks up new entries appended to unsealed segments.

        This is critical: unsealed segments are updated in-place on the primary.
        The replica must copy the latest version on each poll.
        """
        print("\n=== Test: Unsealed segment incremental appends ===", file=sys.stderr)

        # 1. Start primary with journal
        print("Starting primary", file=sys.stderr)
        self.primary = GraphdProcess(
            data_dir=self.primary_dir,
            bolt_port=self.primary_bolt,
            http_port=self.primary_http,
            journal=True,
        )
        self.primary.start()

        # 2. Write initial data (creates unsealed segment-0001)
        print("Writing initial data (Alice, Bob, Charlie)", file=sys.stderr)
        primary_driver = GraphDatabase.driver(f"bolt://localhost:{self.primary_bolt}")
        with primary_driver.session() as session:
            session.run("CREATE NODE TABLE Person(name STRING, PRIMARY KEY(name))")
            session.run("CREATE (p:Person {name: 'Alice'})")
            session.run("CREATE (p:Person {name: 'Bob'})")
            session.run("CREATE (p:Person {name: 'Charlie'})")

        # Give journal time to flush
        time.sleep(0.5)

        # 3. Start replica (syncs Alice, Bob, Charlie from unsealed segment)
        print("Starting replica", file=sys.stderr)
        self.replica = GraphdProcess(
            data_dir=self.replica_dir,
            bolt_port=self.replica_bolt,
            http_port=self.replica_http,
            replica=True,
            replica_source=f"file://{self.primary_dir}",
            replica_poll_interval="2s",
        )
        self.replica.start(timeout=30)

        # 4. Verify replica has initial 3 nodes
        print("Verifying initial sync", file=sys.stderr)
        replica_driver = GraphDatabase.driver(f"bolt://localhost:{self.replica_bolt}")
        with replica_driver.session() as session:
            result = session.run("MATCH (p:Person) RETURN p.name ORDER BY p.name")
            names = [record["p.name"] for record in result]
            self.assertEqual(names, ["Alice", "Bob", "Charlie"])

        # 5. Primary appends to SAME unsealed segment (no rotation yet)
        print("Writing incremental data to unsealed segment (David, Eve)", file=sys.stderr)
        with primary_driver.session() as session:
            session.run("CREATE (p:Person {name: 'David'})")
            session.run("CREATE (p:Person {name: 'Eve'})")
        primary_driver.close()

        time.sleep(0.5)

        # 6. Wait for replica poll
        print("Waiting for replica poll (3s)", file=sys.stderr)
        time.sleep(3)

        # 7. Verify replica sees ALL 5 nodes (including appends to unsealed segment)
        print("Verifying unsealed segment updates", file=sys.stderr)
        with replica_driver.session() as session:
            result = session.run("MATCH (p:Person) RETURN p.name ORDER BY p.name")
            names = [record["p.name"] for record in result]
            self.assertEqual(
                names,
                ["Alice", "Bob", "Charlie", "David", "Eve"],
                "Replica should see appends to unsealed segment",
            )

        replica_driver.close()
        print("=== Unsealed segment test passed! ===", file=sys.stderr)

    def test_concurrent_reads_during_replay(self):
        """
        Test that concurrent reads during replay see consistent state.

        Reader should see either old state (before replay) or new state (after replay),
        never partial state (mid-replay).
        """
        print("\n=== Test: Concurrent reads during replay ===", file=sys.stderr)

        # 1. Start primary and write large dataset (100 entries)
        print("Starting primary and writing 100 entries", file=sys.stderr)
        self.primary = GraphdProcess(
            data_dir=self.primary_dir,
            bolt_port=self.primary_bolt,
            http_port=self.primary_http,
            journal=True,
        )
        self.primary.start()

        primary_driver = GraphDatabase.driver(f"bolt://localhost:{self.primary_bolt}")
        with primary_driver.session() as session:
            session.run("CREATE NODE TABLE Person(name STRING, PRIMARY KEY(name))")
            # Write 100 entries in a transaction (atomic)
            session.run("BEGIN TRANSACTION")
            for i in range(100):
                session.run(f"CREATE (p:Person {{name: 'Person{i:03}'}})")
            session.run("COMMIT")

        # Take snapshot
        print("Taking snapshot", file=sys.stderr)
        self.primary.snapshot()

        # Write 100 more entries
        print("Writing 100 more entries", file=sys.stderr)
        with primary_driver.session() as session:
            session.run("BEGIN TRANSACTION")
            for i in range(100, 200):
                session.run(f"CREATE (p:Person {{name: 'Person{i:03}'}})")
            session.run("COMMIT")
        primary_driver.close()

        time.sleep(0.5)

        # 2. Start replica with short poll interval to catch mid-replay
        print("Starting replica", file=sys.stderr)
        self.replica = GraphdProcess(
            data_dir=self.replica_dir,
            bolt_port=self.replica_bolt,
            http_port=self.replica_http,
            replica=True,
            replica_source=f"file://{self.primary_dir}",
            replica_poll_interval="1s",  # Fast polling
        )
        self.replica.start(timeout=30)

        # 3. Query multiple times during poll window
        print("Running concurrent queries during poll/replay", file=sys.stderr)
        replica_driver = GraphDatabase.driver(f"bolt://localhost:{self.replica_bolt}")

        counts = []
        for i in range(10):
            with replica_driver.session() as session:
                result = session.run("MATCH (p:Person) RETURN count(p) as cnt")
                count = result.single()["cnt"]
                counts.append(count)
            time.sleep(0.3)  # Query every 300ms during ~3s window

        replica_driver.close()

        # 4. Verify consistency: counts should only be 100 or 200, never in between
        print(f"Observed counts: {counts}", file=sys.stderr)
        unique_counts = set(counts)

        # We should only see 100 (before replay) and/or 200 (after replay)
        # Never partial counts like 150 (mid-replay)
        self.assertTrue(
            unique_counts.issubset({100, 200}),
            f"Saw partial replay state! Counts: {counts}. Should only see 100 or 200.",
        )

        # Final count should be 200
        self.assertIn(200, counts, "Should eventually see all 200 entries")

        print("=== Concurrent read test passed! ===", file=sys.stderr)

    def test_poll_interval_timing(self):
        """
        Test that replication latency is within poll interval bounds.

        Latency should be < 2x poll_interval (1 interval + buffer).
        """
        print("\n=== Test: Poll interval timing ===", file=sys.stderr)

        # Use 1s poll interval for faster test
        poll_interval_sec = 1

        # 1. Start primary
        print("Starting primary", file=sys.stderr)
        self.primary = GraphdProcess(
            data_dir=self.primary_dir,
            bolt_port=self.primary_bolt,
            http_port=self.primary_http,
            journal=True,
        )
        self.primary.start()

        primary_driver = GraphDatabase.driver(f"bolt://localhost:{self.primary_bolt}")
        with primary_driver.session() as session:
            session.run("CREATE NODE TABLE Person(name STRING, PRIMARY KEY(name))")

        # Take initial snapshot
        self.primary.snapshot()
        time.sleep(0.5)

        # 2. Start replica
        print(f"Starting replica (poll_interval={poll_interval_sec}s)", file=sys.stderr)
        self.replica = GraphdProcess(
            data_dir=self.replica_dir,
            bolt_port=self.replica_bolt,
            http_port=self.replica_http,
            replica=True,
            replica_source=f"file://{self.primary_dir}",
            replica_poll_interval=f"{poll_interval_sec}s",
        )
        self.replica.start(timeout=30)

        replica_driver = GraphDatabase.driver(f"bolt://localhost:{self.replica_bolt}")

        # 3. Write entry and measure latency
        print("Writing entry and measuring replication latency", file=sys.stderr)

        write_time = time.time()
        with primary_driver.session() as session:
            session.run("CREATE (p:Person {name: 'Alice'})")
        primary_driver.close()

        # Poll until replica sees it
        max_wait = poll_interval_sec * 3  # 3x interval for safety
        deadline = time.time() + max_wait

        while time.time() < deadline:
            with replica_driver.session() as session:
                result = session.run("MATCH (p:Person {name: 'Alice'}) RETURN count(p) as cnt")
                count = result.single()["cnt"]
                if count > 0:
                    latency = time.time() - write_time
                    print(f"Replication latency: {latency:.2f}s", file=sys.stderr)

                    # Assert latency is reasonable (< 2x poll interval)
                    self.assertLess(
                        latency,
                        poll_interval_sec * 2,
                        f"Latency {latency:.2f}s exceeds 2x poll interval ({poll_interval_sec * 2}s)",
                    )
                    break
            time.sleep(0.1)
        else:
            self.fail(f"Replica did not see entry within {max_wait}s")

        replica_driver.close()
        print("=== Poll interval timing test passed! ===", file=sys.stderr)

    def test_multi_table_relationships(self):
        """
        Test replication with multiple tables and relationships.

        Current tests only use single node table. This tests edges (relationships)
        and multi-table schemas.
        """
        print("\n=== Test: Multi-table relationships ===", file=sys.stderr)

        # 1. Start primary
        print("Starting primary", file=sys.stderr)
        self.primary = GraphdProcess(
            data_dir=self.primary_dir,
            bolt_port=self.primary_bolt,
            http_port=self.primary_http,
            journal=True,
        )
        self.primary.start()

        # 2. Create schema with nodes and edges
        print("Creating multi-table schema", file=sys.stderr)
        primary_driver = GraphDatabase.driver(f"bolt://localhost:{self.primary_bolt}")
        with primary_driver.session() as session:
            # Create node tables
            session.run("CREATE NODE TABLE Person(name STRING, age INT64, PRIMARY KEY(name))")
            session.run("CREATE NODE TABLE Company(name STRING, PRIMARY KEY(name))")

            # Create edge table
            session.run("CREATE REL TABLE WORKS_AT(FROM Person TO Company, since INT64)")

            # Create data
            session.run("CREATE (p:Person {name: 'Alice', age: 30})")
            session.run("CREATE (p:Person {name: 'Bob', age: 25})")
            session.run("CREATE (c:Company {name: 'Acme Corp'})")
            session.run("""
                MATCH (p:Person {name: 'Alice'}), (c:Company {name: 'Acme Corp'})
                CREATE (p)-[:WORKS_AT {since: 2020}]->(c)
            """)

        # Snapshot
        print("Taking snapshot", file=sys.stderr)
        self.primary.snapshot()

        # Add more data after snapshot
        print("Adding post-snapshot data", file=sys.stderr)
        with primary_driver.session() as session:
            session.run("CREATE (c:Company {name: 'Beta Inc'})")
            session.run("""
                MATCH (p:Person {name: 'Bob'}), (c:Company {name: 'Beta Inc'})
                CREATE (p)-[:WORKS_AT {since: 2021}]->(c)
            """)
        primary_driver.close()

        time.sleep(0.5)

        # 3. Start replica
        print("Starting replica", file=sys.stderr)
        self.replica = GraphdProcess(
            data_dir=self.replica_dir,
            bolt_port=self.replica_bolt,
            http_port=self.replica_http,
            replica=True,
            replica_source=f"file://{self.primary_dir}",
            replica_poll_interval="2s",
        )
        self.replica.start(timeout=30)

        # 4. Verify schema and data
        print("Verifying replica data", file=sys.stderr)
        replica_driver = GraphDatabase.driver(f"bolt://localhost:{self.replica_bolt}")
        with replica_driver.session() as session:
            # Verify node counts
            result = session.run("MATCH (p:Person) RETURN count(p) as cnt")
            person_count = result.single()["cnt"]
            self.assertEqual(person_count, 2, "Should have 2 persons")

            result = session.run("MATCH (c:Company) RETURN count(c) as cnt")
            company_count = result.single()["cnt"]
            self.assertEqual(company_count, 2, "Should have 2 companies")

            # Verify relationships
            result = session.run("MATCH ()-[r:WORKS_AT]->() RETURN count(r) as cnt")
            rel_count = result.single()["cnt"]
            self.assertEqual(rel_count, 2, "Should have 2 WORKS_AT relationships")

            # Verify relationship properties
            result = session.run("""
                MATCH (p:Person {name: 'Alice'})-[r:WORKS_AT]->(c:Company)
                RETURN c.name as company, r.since as since
            """)
            record = result.single()
            self.assertEqual(record["company"], "Acme Corp")
            self.assertEqual(record["since"], 2020)

        replica_driver.close()
        print("=== Multi-table test passed! ===", file=sys.stderr)

    def test_empty_and_schema_only_segments(self):
        """
        Test that replica handles edge cases: empty segments and schema-only DDL.
        """
        print("\n=== Test: Empty and schema-only segments ===", file=sys.stderr)

        # 1. Start primary
        print("Starting primary", file=sys.stderr)
        self.primary = GraphdProcess(
            data_dir=self.primary_dir,
            bolt_port=self.primary_bolt,
            http_port=self.primary_http,
            journal=True,
        )
        self.primary.start()

        # 2. Create schema with no data (schema-only segment)
        print("Creating schema with no data", file=sys.stderr)
        primary_driver = GraphDatabase.driver(f"bolt://localhost:{self.primary_bolt}")
        with primary_driver.session() as session:
            session.run("CREATE NODE TABLE Person(name STRING, PRIMARY KEY(name))")
            session.run("CREATE NODE TABLE Company(name STRING, PRIMARY KEY(name))")

        # Snapshot with schema but no data
        print("Taking snapshot (schema only)", file=sys.stderr)
        self.primary.snapshot()

        # Add data after snapshot
        print("Adding data after snapshot", file=sys.stderr)
        with primary_driver.session() as session:
            session.run("CREATE (p:Person {name: 'Alice'})")
        primary_driver.close()

        time.sleep(0.5)

        # 3. Start replica
        print("Starting replica", file=sys.stderr)
        self.replica = GraphdProcess(
            data_dir=self.replica_dir,
            bolt_port=self.replica_bolt,
            http_port=self.replica_http,
            replica=True,
            replica_source=f"file://{self.primary_dir}",
            replica_poll_interval="2s",
        )
        self.replica.start(timeout=30)

        # 4. Verify replica has schema and data
        print("Verifying replica", file=sys.stderr)
        replica_driver = GraphDatabase.driver(f"bolt://localhost:{self.replica_bolt}")
        with replica_driver.session() as session:
            # Verify schema exists (query should succeed, not error)
            result = session.run("MATCH (p:Person) RETURN count(p) as cnt")
            count = result.single()["cnt"]
            self.assertEqual(count, 1, "Should have 1 person")

            # Verify Company table exists (even though empty)
            result = session.run("MATCH (c:Company) RETURN count(c) as cnt")
            count = result.single()["cnt"]
            self.assertEqual(count, 0, "Company table should be empty but exist")

        replica_driver.close()
        print("=== Empty/schema-only test passed! ===", file=sys.stderr)

    def test_large_dataset_disk_io(self):
        """
        Test replication with large dataset that forces disk I/O.

        Uses 10k+ nodes to exceed typical buffer cache and ensure:
        - Disk reads/writes happen during replay
        - Writer lock doesn't cause timeouts
        - Concurrent reads work during slow I/O
        - No race conditions with disk operations

        KNOWN ISSUE: This test currently fails due to a race condition in unsealed
        segment replication. When copying unsealed segments, the replica can catch
        the file mid-write, resulting in incomplete copies. This manifests as missing
        nodes (e.g., expecting 11000 but getting 10795). The fix requires proper
        synchronization during file copy operations.
        See: replica.rs:752 (fs::copy without synchronization)
        """
        print("\n=== Test: Large dataset with disk I/O ===", file=sys.stderr)

        # 1. Copy 10MB template (5k nodes pre-snapshot with snapshot already)
        print("Copying 10MB template (5k nodes + snapshot)", file=sys.stderr)
        template_node_count = DatabaseTemplates.copy_template("10mb", self.primary_dir)
        print(f"  Template has {template_node_count} nodes with snapshot", file=sys.stderr)

        # 2. Start primary (already has 5k nodes + snapshot)
        print("Starting primary with template data", file=sys.stderr)
        self.primary = GraphdProcess(
            data_dir=self.primary_dir,
            bolt_port=self.primary_bolt,
            http_port=self.primary_http,
            journal=True,
        )
        self.primary.start()

        # 3. Write 5k more nodes after snapshot (post-snapshot journal entries)
        print("Writing 5k more nodes (post-snapshot, will be replayed)", file=sys.stderr)
        primary_driver = GraphDatabase.driver(f"bolt://localhost:{self.primary_bolt}")
        batch_size = 500
        with primary_driver.session() as session:
            for batch_start in range(5000, 10000, batch_size):
                session.run("BEGIN TRANSACTION")
                for i in range(batch_start, min(batch_start + batch_size, 10000)):
                    data = f"Large data payload for node {i} " * 10
                    session.run(
                        "CREATE (p:Person {id: $id, name: $name, data: $data})",
                        id=i,
                        name=f"Person{i:05}",
                        data=data,
                    )
                session.run("COMMIT")
                if (batch_start + batch_size) % 1000 == 0:
                    print(f"  Written {batch_start + batch_size} nodes", file=sys.stderr)

        primary_driver.close()
        time.sleep(1)

        # 5. Start replica
        print("Starting replica (will replay 5k journal entries)", file=sys.stderr)
        start_time = time.time()
        self.replica = GraphdProcess(
            data_dir=self.replica_dir,
            bolt_port=self.replica_bolt,
            http_port=self.replica_http,
            replica=True,
            replica_source=f"file://{self.primary_dir}",
            replica_poll_interval="2s",
        )
        self.replica.start(timeout=60)  # Longer timeout for large dataset
        init_time = time.time() - start_time
        print(f"  Replica startup took {init_time:.2f}s", file=sys.stderr)

        # 6. Verify replica has all 10k nodes
        print("Verifying replica has 10k nodes", file=sys.stderr)
        replica_driver = GraphDatabase.driver(f"bolt://localhost:{self.replica_bolt}")

        with replica_driver.session() as session:
            result = session.run("MATCH (p:Person) RETURN count(p) as cnt")
            count = result.single()["cnt"]
            self.assertEqual(count, 10000, f"Expected 10k nodes, got {count}")

            # Verify some specific nodes
            result = session.run("MATCH (p:Person {id: 42}) RETURN p.name as name")
            name = result.single()["name"]
            self.assertEqual(name, "Person00042")

            result = session.run("MATCH (p:Person {id: 9999}) RETURN p.name as name")
            name = result.single()["name"]
            self.assertEqual(name, "Person09999")

        # 7. Write more data on primary (1k incremental nodes)
        print("Writing 1k incremental nodes on primary", file=sys.stderr)
        primary_driver = GraphDatabase.driver(f"bolt://localhost:{self.primary_bolt}")
        with primary_driver.session() as session:
            session.run("BEGIN TRANSACTION")
            for i in range(10000, 11000):
                data = f"Large data payload for node {i} " * 10
                session.run(
                    "CREATE (p:Person {id: $id, name: $name, data: $data})",
                    id=i,
                    name=f"Person{i:05}",
                    data=data,
                )
            session.run("COMMIT")

        # Verify primary has all 11k nodes
        with primary_driver.session() as session:
            result = session.run("MATCH (p:Person) RETURN count(p) as cnt")
            primary_count = result.single()["cnt"]
            print(f"  Primary count after incremental write: {primary_count}", file=sys.stderr)
            self.assertEqual(primary_count, 11000, f"Primary should have 11k nodes, got {primary_count}")

        primary_driver.close()

        # Wait for journal flush
        time.sleep(1)

        # 8. Test concurrent reads during incremental replay
        print("Testing concurrent reads during incremental replay", file=sys.stderr)

        # Wait for replica to converge (progressive replication).
        # Replica may copy mid-write, getting partial data, then catch up on subsequent polls.
        max_wait = 15  # seconds
        poll_interval = 2  # seconds
        deadline = time.time() + max_wait

        replica_driver = GraphDatabase.driver(f"bolt://localhost:{self.replica_bolt}")
        while time.time() < deadline:
            with replica_driver.session() as session:
                result = session.run("MATCH (p:Person) RETURN count(p) as cnt")
                replica_count = result.single()["cnt"]
                print(f"  Replica count: {replica_count} (waiting for 11000)", file=sys.stderr)

                if replica_count == 11000:
                    break

            time.sleep(poll_interval)
        else:
            # Timeout - replica didn't catch up
            with replica_driver.session() as session:
                result = session.run("MATCH (p:Person) RETURN count(p) as cnt")
                replica_count = result.single()["cnt"]
                self.fail(f"Replica did not catch up within {max_wait}s. Final count: {replica_count}, expected: 11000")

        print(f"  Replica converged to 11000 nodes", file=sys.stderr)

        replica_driver.close()
        print("=== Large dataset test passed! ===", file=sys.stderr)

    def test_concurrent_reads_during_slow_replay(self):
        """
        Test that readers don't timeout during slow disk-bound incremental replay.

        Creates large dataset, waits for initial sync to complete, then writes
        more data and tests concurrent reads during incremental poll/replay.
        """
        print("\n=== Test: Concurrent reads during slow replay ===", file=sys.stderr)

        # 1. Copy 10MB template (5k nodes pre-snapshot with snapshot)
        print("Copying 10MB template (5k nodes + snapshot)", file=sys.stderr)
        template_node_count = DatabaseTemplates.copy_template("10mb", self.primary_dir)
        print(f"  Template has {template_node_count} nodes with snapshot", file=sys.stderr)

        # 2. Start primary with template data
        print("Starting primary with template data", file=sys.stderr)
        self.primary = GraphdProcess(
            data_dir=self.primary_dir,
            bolt_port=self.primary_bolt,
            http_port=self.primary_http,
            journal=True,
        )
        self.primary.start()

        # 3. Start replica (will do initial sync)
        print("Starting replica (initial sync)", file=sys.stderr)
        self.replica = GraphdProcess(
            data_dir=self.replica_dir,
            bolt_port=self.replica_bolt,
            http_port=self.replica_http,
            replica=True,
            replica_source=f"file://{self.primary_dir}",
            replica_poll_interval="3s",
        )
        self.replica.start(timeout=90)

        # 4. Wait for initial sync to complete by verifying base data
        print("Waiting for initial sync to complete", file=sys.stderr)
        replica_driver = GraphDatabase.driver(f"bolt://localhost:{self.replica_bolt}")
        max_wait = 60
        deadline = time.time() + max_wait
        initial_sync_complete = False

        while time.time() < deadline:
            try:
                with replica_driver.session() as session:
                    result = session.run("MATCH (p:Person) RETURN count(p) as cnt")
                    count = result.single()["cnt"]
                    if count == 5000:
                        print(f"  Initial sync complete ({count} nodes)", file=sys.stderr)
                        initial_sync_complete = True
                        break
                    else:
                        print(f"  Replica has {count} nodes, waiting for 5000", file=sys.stderr)
            except Exception as e:
                print(f"  Query failed (replica still initializing): {e}", file=sys.stderr)
            time.sleep(1)

        self.assertTrue(initial_sync_complete, "Initial sync did not complete in time")

        # 5. NOW write 5k more nodes to primary (for incremental replay)
        print("Writing 5k incremental nodes to primary", file=sys.stderr)
        primary_driver = GraphDatabase.driver(f"bolt://localhost:{self.primary_bolt}")
        batch_size = 500
        with primary_driver.session() as session:
            for batch_start in range(5000, 10000, batch_size):
                session.run("BEGIN TRANSACTION")
                for i in range(batch_start, min(batch_start + batch_size, 10000)):
                    data = f"Payload {i} " * 20  # ~400 bytes
                    session.run(
                        "CREATE (p:Person {id: $id, name: $name, data: $data})",
                        id=i,
                        name=f"P{i:05}",
                        data=data,
                    )
                session.run("COMMIT")
                if (batch_start + batch_size) % 1000 == 0:
                    print(f"  Written {batch_start + batch_size} nodes", file=sys.stderr)

        primary_driver.close()
        time.sleep(1)

        # 6. Run concurrent reads while replica is doing incremental replay
        print("Running concurrent reads during incremental replay (10 queries)", file=sys.stderr)
        results = []
        for i in range(10):
            try:
                with replica_driver.session() as session:
                    # Different query patterns to stress different code paths
                    if i % 2 == 0:
                        result = session.run("MATCH (p:Person) RETURN count(p) as cnt")
                        count = result.single()["cnt"]
                        results.append(("count", count))
                    else:
                        result = session.run("MATCH (p:Person {id: $id}) RETURN p.name as name", id=i * 100)
                        record = result.single()
                        results.append(("point", record["name"] if record else None))
                time.sleep(0.3)
            except Exception as e:
                self.fail(f"Query {i} failed during concurrent access: {e}")

        print(f"  All {len(results)} concurrent queries succeeded", file=sys.stderr)

        # 7. Wait for full convergence
        print("Waiting for replica to catch up to 10k nodes", file=sys.stderr)
        deadline = time.time() + 30  # Give more time for 5k nodes to replicate
        while time.time() < deadline:
            with replica_driver.session() as session:
                result = session.run("MATCH (p:Person) RETURN count(p) as cnt")
                count = result.single()["cnt"]
                print(f"  Replica count: {count} (waiting for 10000)", file=sys.stderr)
                if count == 10000:
                    print(f"  Replica fully caught up ({count} nodes)", file=sys.stderr)
                    break
            time.sleep(2)

        # 8. Final verification
        with replica_driver.session() as session:
            result = session.run("MATCH (p:Person) RETURN count(p) as cnt")
            count = result.single()["cnt"]
            self.assertEqual(count, 10000, f"Expected 10k nodes, got {count}")

        replica_driver.close()
        print("=== Concurrent slow replay test passed! ===", file=sys.stderr)


if __name__ == "__main__":
    unittest.main(verbosity=2)
