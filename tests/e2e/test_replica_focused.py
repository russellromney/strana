#!/usr/bin/env python3
"""
Focused replica tests: restore speed, convergence, data integrity.

Uses pre-built test fixtures to avoid rebuilding large databases each run.
First run creates the fixture (~93s for 100M nodes); subsequent runs reuse it.

Usage:
    make e2e-replica-focused
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

sys.path.insert(0, os.path.dirname(__file__))
from test_e2e import find_free_ports, GRAPHD_BINARY, STRANA_ROOT

FIXTURE_DIR = os.path.join(STRANA_ROOT, ".test-fixtures")


class GraphdProcess:
    """Manages a graphd subprocess."""

    def __init__(self, data_dir, bolt_port, http_port, journal=False,
                 replica=False, replica_source=None, replica_poll_interval=None,
                 s3_bucket=None, s3_prefix=None, extra_env=None):
        self.data_dir = data_dir
        self.bolt_port = bolt_port
        self.http_port = http_port
        self.proc = None
        self._stderr_data = ""
        self._stderr_lines = []
        self._stderr_thread = None

        self.cmd = [
            GRAPHD_BINARY,
            "--data-dir", str(data_dir),
            "--bolt-port", str(bolt_port),
            "--http-port", str(http_port),
            "--bolt-host", "127.0.0.1",
            "--http-host", "127.0.0.1",
        ]
        if journal:
            self.cmd.append("--journal")
        if replica:
            self.cmd.append("--replica")
            if replica_source:
                self.cmd.extend(["--replica-source", replica_source])
            if replica_poll_interval:
                self.cmd.extend(["--replica-poll-interval", replica_poll_interval])
        if s3_bucket:
            self.cmd.extend(["--s3-bucket", s3_bucket])
        if s3_prefix:
            self.cmd.extend(["--s3-prefix", s3_prefix])

        self.env = dict(os.environ)
        self.env["DYLD_LIBRARY_PATH"] = os.path.join(STRANA_ROOT, "lbug-lib")
        self.env["RUST_LOG"] = "info"
        self.env["NO_COLOR"] = "1"  # disable ANSI codes for log capture
        if extra_env:
            self.env.update(extra_env)

    def start(self, timeout=30, **kwargs):
        import requests
        import threading
        self._stderr_lines = []
        self.proc = subprocess.Popen(
            self.cmd, env=self.env,
            stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        )
        # Read stderr in background thread to prevent pipe buffer deadlock.
        def _drain_stderr():
            for line in self.proc.stderr:
                self._stderr_lines.append(line)
        self._stderr_thread = threading.Thread(target=_drain_stderr, daemon=True)
        self._stderr_thread.start()

        deadline = time.time() + timeout
        while time.time() < deadline:
            if self.proc.poll() is not None:
                self._stderr_thread.join(timeout=2)
                err = b"".join(self._stderr_lines).decode("utf-8", errors="replace")
                out = self.proc.stdout.read().decode()
                raise RuntimeError(
                    f"graphd died: exit={self.proc.returncode}\n{err}\n{out}")
            try:
                r = requests.get(f"http://127.0.0.1:{self.http_port}/health", timeout=1)
                if r.status_code == 200:
                    return
            except Exception:
                pass
            time.sleep(0.1)
        raise TimeoutError(f"graphd not ready in {timeout}s")

    def stop(self):
        if self.proc and self.proc.poll() is None:
            self.proc.send_signal(signal.SIGINT)
            try:
                self.proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self.proc.kill()
                self.proc.wait()
        if self._stderr_thread:
            self._stderr_thread.join(timeout=3)
        self._stderr_data = b"".join(self._stderr_lines).decode("utf-8", errors="replace")

    def get_logs(self):
        """Get stderr logs. Must call stop() first for complete output."""
        return self._stderr_data

    def snapshot(self):
        import requests
        r = requests.post(f"http://127.0.0.1:{self.http_port}/v1/snapshot", timeout=300)
        r.raise_for_status()


def query_all_ids(driver):
    """Return sorted list of all Person ids from the database."""
    with driver.session() as s:
        result = s.run("MATCH (p:Person) RETURN p.id AS id ORDER BY p.id")
        return [r["id"] for r in result]


def query_count(driver):
    """Return count of Person nodes."""
    with driver.session() as s:
        result = s.run("MATCH (p:Person) RETURN count(p) AS cnt")
        return result.single()["cnt"]


def batch_insert(driver, start, end):
    """Insert Person nodes [start, end) in a single transaction."""
    with driver.session() as s:
        s.run("BEGIN TRANSACTION")
        for i in range(start, end):
            s.run("CREATE (p:Person {id: $id, name: $name})", id=i, name=f"P{i}")
        s.run("COMMIT")


def csv_insert(driver, data_dir, start, end):
    """Bulk-insert Person nodes via COPY FROM CSV. Much faster than individual CREATEs."""
    csv_path = os.path.join(data_dir, "bulk_load.csv")
    with open(csv_path, "w") as f:
        for i in range(start, end):
            f.write(f"{i},P{i}\n")
    with driver.session() as s:
        s.run(f'COPY Person FROM "{csv_path}" (header=false)')
    os.unlink(csv_path)


def dir_size_mb(path):
    """Return size of a directory tree in MB."""
    total = 0
    for dirpath, _, filenames in os.walk(path):
        for f in filenames:
            total += os.path.getsize(os.path.join(dirpath, f))
    return total / (1024 * 1024)


def fast_copy_tree(src, dst):
    """Copy directory tree. Uses APFS clones on macOS for near-instant copy."""
    if sys.platform == "darwin":
        subprocess.run(["cp", "-ac", src + "/.", dst], check=True)
    else:
        shutil.copytree(src, dst, dirs_exist_ok=True)


def get_or_create_fixture(name, node_count):
    """
    Get or create a pre-built primary database fixture.

    On first call: starts graphd, creates schema, bulk-loads node_count nodes
    via COPY FROM CSV, takes a snapshot, stops graphd, saves the data dir.

    On subsequent calls: returns the cached fixture path immediately.

    Returns the fixture directory path (contains db/, journal/, snapshots/).
    """
    fixture_path = os.path.join(FIXTURE_DIR, name)
    marker = os.path.join(fixture_path, ".fixture_complete")

    if os.path.exists(marker):
        print(f"  Using cached fixture: {fixture_path}", file=sys.stderr)
        return fixture_path

    print(f"  Creating fixture '{name}' ({node_count:,} nodes)...", file=sys.stderr)
    os.makedirs(fixture_path, exist_ok=True)

    bolt_port, http_port = find_free_ports(2)
    proc = GraphdProcess(fixture_path, bolt_port, http_port, journal=True)
    proc.start(timeout=30)

    try:
        drv = GraphDatabase.driver(f"bolt://localhost:{bolt_port}")
        with drv.session() as s:
            s.run("CREATE NODE TABLE Person(id INT64, name STRING, PRIMARY KEY(id))")

        t0 = time.time()
        csv_insert(drv, fixture_path, 0, node_count)
        elapsed = time.time() - t0
        print(f"  COPY FROM {node_count:,} nodes: {elapsed:.1f}s", file=sys.stderr)

        proc.snapshot()
        time.sleep(0.3)

        count = query_count(drv)
        drv.close()
        assert count == node_count, f"Expected {node_count}, got {count}"
    finally:
        proc.stop()

    # Write completion marker with metadata.
    with open(marker, "w") as f:
        f.write(f"nodes={node_count}\n")

    size = dir_size_mb(fixture_path)
    print(f"  Fixture ready: {size:.1f} MB", file=sys.stderr)
    return fixture_path


class TestReplicaFocused(unittest.TestCase):

    def setUp(self):
        self.primary_dir = tempfile.mkdtemp(prefix="graphd_pri_")
        self.replica_dir = tempfile.mkdtemp(prefix="graphd_rep_")
        self.primary_bolt, self.primary_http, self.replica_bolt, self.replica_http = find_free_ports(4)
        self.primary = None
        self.replica = None

    def tearDown(self):
        if self.replica:
            self.replica.stop()
        if self.primary:
            self.primary.stop()
        shutil.rmtree(self.primary_dir, ignore_errors=True)
        shutil.rmtree(self.replica_dir, ignore_errors=True)

    def test_restore_speed_and_integrity(self):
        """
        Restore is fast: snapshot + journal replay, measure it.
        Data integrity: compare primary and replica DBs.

        Uses a pre-built fixture for the snapshot data (cached across runs).
        Only the journal entries are added fresh each run.
        """
        SNAP_COUNT = int(os.environ.get("SNAP_COUNT", "100000000"))
        JOURNAL_COUNT = 10_000
        TOTAL = SNAP_COUNT + JOURNAL_COUNT

        # --- Baseline: empty graphd startup ---
        baseline_dir = tempfile.mkdtemp(prefix="graphd_baseline_")
        bp, bh = find_free_ports(2)
        baseline = GraphdProcess(baseline_dir, bp, bh)
        t0 = time.time()
        baseline.start()
        baseline_time = time.time() - t0
        baseline.stop()
        shutil.rmtree(baseline_dir, ignore_errors=True)

        # --- Primary setup from fixture ---
        fixture = get_or_create_fixture(f"person_{SNAP_COUNT}", SNAP_COUNT)

        t0 = time.time()
        fast_copy_tree(fixture, self.primary_dir)
        copy_time = time.time() - t0
        print(f"\n  Fixture copy: {copy_time:.2f}s", file=sys.stderr)

        self.primary = GraphdProcess(
            self.primary_dir, self.primary_bolt, self.primary_http, journal=True)
        self.primary.start(timeout=60)

        drv = GraphDatabase.driver(f"bolt://localhost:{self.primary_bolt}")

        # Verify fixture loaded correctly.
        snap_count_actual = query_count(drv)
        self.assertEqual(snap_count_actual, SNAP_COUNT,
                         f"Fixture has {snap_count_actual}, expected {SNAP_COUNT}")

        # Journal-only: individual CREATEs (not in snapshot).
        t0 = time.time()
        batch_insert(drv, SNAP_COUNT, TOTAL)
        insert_journal_time = time.time() - t0
        time.sleep(0.3)

        primary_count = query_count(drv)
        self.assertEqual(primary_count, TOTAL)
        drv.close()

        # --- Replica initial sync ---
        t0 = time.time()
        self.replica = GraphdProcess(
            self.replica_dir, self.replica_bolt, self.replica_http,
            replica=True, replica_source=f"file://{self.primary_dir}",
            replica_poll_interval="60s",
        )
        self.replica.start(timeout=120)
        restore_time = time.time() - t0

        rdrv = GraphDatabase.driver(f"bolt://localhost:{self.replica_bolt}")
        replica_count = query_count(rdrv)
        rdrv.close()

        # Stop replica to flush and read all logs.
        self.replica.stop()
        replica_logs = self.replica.get_logs()
        self.replica = None

        restore_only = restore_time - baseline_time

        pri_db_mb = dir_size_mb(os.path.join(self.primary_dir, "db"))
        pri_journal_mb = dir_size_mb(os.path.join(self.primary_dir, "journal"))
        pri_snap_mb = dir_size_mb(os.path.join(self.primary_dir, "snapshots"))

        print(f"  Fixture copy:      {copy_time:.2f}s", file=sys.stderr)
        print(f"  Insert {JOURNAL_COUNT:,} (CREATE): {insert_journal_time:.2f}s", file=sys.stderr)
        print(f"  DB size:           {pri_db_mb:.1f} MB", file=sys.stderr)
        print(f"  Journal size:      {pri_journal_mb:.1f} MB", file=sys.stderr)
        print(f"  Snapshot size:     {pri_snap_mb:.1f} MB", file=sys.stderr)
        print(f"  Baseline startup:  {baseline_time:.2f}s", file=sys.stderr)
        print(f"  Replica startup:   {restore_time:.2f}s  ({primary_count:,} nodes)", file=sys.stderr)
        print(f"  Restore overhead:  {restore_only:.2f}s  (startup subtracted)", file=sys.stderr)
        print(f"  --- replica logs ---", file=sys.stderr)
        for line in replica_logs.strip().splitlines():
            # Show timing lines and key info lines
            if "[timing]" in line or "restore" in line.lower() or "sync" in line.lower():
                print(f"    {line.rstrip()}", file=sys.stderr)
        if not replica_logs.strip():
            print(f"    (no logs captured — {len(replica_logs)} bytes raw)", file=sys.stderr)
        print(f"  --- end logs ---", file=sys.stderr)

        self.assertEqual(replica_count, primary_count,
                         f"Replica has {replica_count} nodes, expected {primary_count}")

    def test_incremental_convergence(self):
        """
        Incremental replication converges: primary writes continuously,
        replica polls at 0.5s, eventually they match.

        1. Primary: create schema + snapshot (empty)
        2. Replica: start with 0.5s poll interval
        3. Primary: write 200 nodes in 4 batches of 50
        4. Wait for replica to converge
        5. Compare all ids
        """
        # --- Primary setup ---
        self.primary = GraphdProcess(
            self.primary_dir, self.primary_bolt, self.primary_http, journal=True)
        self.primary.start()

        drv = GraphDatabase.driver(f"bolt://localhost:{self.primary_bolt}")
        with drv.session() as s:
            s.run("CREATE NODE TABLE Person(id INT64, name STRING, PRIMARY KEY(id))")

        # Snapshot with schema only
        self.primary.snapshot()
        time.sleep(0.3)

        # --- Start replica with fast polling ---
        self.replica = GraphdProcess(
            self.replica_dir, self.replica_bolt, self.replica_http,
            replica=True, replica_source=f"file://{self.primary_dir}",
            replica_poll_interval="500ms",
        )
        self.replica.start(timeout=30)

        # --- Primary writes continuously ---
        for batch in range(4):
            start = batch * 50
            batch_insert(drv, start, start + 50)
            time.sleep(0.2)  # small gap between batches

        time.sleep(0.3)  # let journal flush
        primary_ids = query_all_ids(drv)
        drv.close()

        # --- Wait for replica convergence ---
        rdrv = GraphDatabase.driver(f"bolt://localhost:{self.replica_bolt}")
        deadline = time.time() + 10  # 10s should be plenty at 0.5s poll
        converged = False
        last_count = 0

        while time.time() < deadline:
            count = query_count(rdrv)
            if count == 200:
                converged = True
                break
            last_count = count
            time.sleep(0.3)

        if not converged:
            rdrv.close()
            self.fail(f"Replica did not converge. Last count: {last_count}, expected 200")

        # --- Data integrity check ---
        replica_ids = query_all_ids(rdrv)
        rdrv.close()

        self.assertEqual(replica_ids, primary_ids,
                         "Replica ids don't match primary after convergence")
        print(f"\n  Converged: 200 nodes, all ids match", file=sys.stderr)


class TestReplicaS3Scale(unittest.TestCase):
    """
    S3-based replica at scale: snapshot + journal via S3/Tigris.

    Set NODE_COUNT env var to control scale (default: 10000).
    Example:
        NODE_COUNT=100000 python3 -m pytest tests/e2e/test_replica_focused.py::TestReplicaS3Scale -v
        NODE_COUNT=1000000 python3 -m pytest tests/e2e/test_replica_focused.py::TestReplicaS3Scale -v
    """

    def setUp(self):
        sys.path.insert(0, os.path.dirname(__file__))
        from test_e2e import load_s3_env, cleanup_s3_prefix
        self._load_s3_env = load_s3_env
        self._cleanup_s3_prefix = cleanup_s3_prefix

        self.s3_env, self.s3_bucket = load_s3_env()
        if not self.s3_env or not self.s3_bucket:
            self.skipTest("S3/Tigris credentials not available in .env.test")

        self.primary_dir = tempfile.mkdtemp(prefix="graphd_s3_pri_")
        self.replica_dir = tempfile.mkdtemp(prefix="graphd_s3_rep_")
        self.test_prefix = f"test-s3-scale-{int(time.time())}/"
        self.primary_bolt, self.primary_http, self.replica_bolt, self.replica_http = find_free_ports(4)
        self.primary = None
        self.replica = None

    def tearDown(self):
        if self.replica:
            self.replica.stop()
        if self.primary:
            self.primary.stop()
        shutil.rmtree(self.primary_dir, ignore_errors=True)
        shutil.rmtree(self.replica_dir, ignore_errors=True)
        if self.s3_env and self.s3_bucket:
            self._cleanup_s3_prefix(self.s3_bucket, self.test_prefix, self.s3_env)

    def test_s3_replica_at_scale(self):
        """
        S3 replica with fixture-based snapshot + journal entries.

        1. Create/reuse fixture (COPY FROM CSV)
        2. Start primary with journal + S3, take snapshot
        3. Write JOURNAL_COUNT additional nodes (uploaded to S3 via journal uploader)
        4. Start replica from S3
        5. Verify all nodes present
        """
        SNAP_COUNT = int(os.environ.get("NODE_COUNT", "10000"))
        JOURNAL_COUNT = min(1000, SNAP_COUNT // 10)
        TOTAL = SNAP_COUNT + JOURNAL_COUNT

        # --- Primary setup from fixture ---
        fixture = get_or_create_fixture(f"person_{SNAP_COUNT}", SNAP_COUNT)

        t0 = time.time()
        fast_copy_tree(fixture, self.primary_dir)
        copy_time = time.time() - t0
        print(f"\n  Fixture copy: {copy_time:.2f}s", file=sys.stderr)

        self.primary = GraphdProcess(
            self.primary_dir, self.primary_bolt, self.primary_http,
            journal=True, s3_bucket=self.s3_bucket, s3_prefix=self.test_prefix,
            extra_env=self.s3_env,
        )
        self.primary.start(timeout=60)

        drv = GraphDatabase.driver(f"bolt://localhost:{self.primary_bolt}")

        # Verify fixture loaded.
        snap_count_actual = query_count(drv)
        self.assertEqual(snap_count_actual, SNAP_COUNT)

        # Take snapshot (uploads to S3).
        print(f"  Taking snapshot ({SNAP_COUNT:,} nodes)...", file=sys.stderr)
        t0 = time.time()
        self.primary.snapshot()
        snap_time = time.time() - t0
        print(f"  Snapshot + S3 upload: {snap_time:.2f}s", file=sys.stderr)

        # Write journal entries (uploaded to S3 by background uploader).
        print(f"  Writing {JOURNAL_COUNT:,} journal entries...", file=sys.stderr)
        t0 = time.time()
        batch_insert(drv, SNAP_COUNT, TOTAL)
        insert_time = time.time() - t0
        print(f"  Journal insert: {insert_time:.2f}s", file=sys.stderr)

        primary_count = query_count(drv)
        self.assertEqual(primary_count, TOTAL)
        drv.close()

        # Wait for journal uploader to seal + upload.
        time.sleep(3)

        # --- Replica from S3 ---
        print(f"  Starting S3 replica...", file=sys.stderr)
        s3_source = f"s3://{self.s3_bucket}/{self.test_prefix}"
        t0 = time.time()
        self.replica = GraphdProcess(
            self.replica_dir, self.replica_bolt, self.replica_http,
            replica=True, replica_source=s3_source, replica_poll_interval="2s",
            extra_env=self.s3_env,
        )
        self.replica.start(timeout=120)
        restore_time = time.time() - t0
        print(f"  Replica restore from S3: {restore_time:.2f}s", file=sys.stderr)

        rdrv = GraphDatabase.driver(f"bolt://localhost:{self.replica_bolt}")
        replica_count = query_count(rdrv)
        rdrv.close()

        # Print sizes.
        pri_db_mb = dir_size_mb(os.path.join(self.primary_dir, "db"))
        pri_snap_mb = dir_size_mb(os.path.join(self.primary_dir, "snapshots"))
        pri_journal_mb = dir_size_mb(os.path.join(self.primary_dir, "journal"))
        print(f"  DB size:      {pri_db_mb:.1f} MB", file=sys.stderr)
        print(f"  Snapshot:     {pri_snap_mb:.1f} MB (compressed)", file=sys.stderr)
        print(f"  Journal:      {pri_journal_mb:.1f} MB", file=sys.stderr)
        print(f"  Nodes: {SNAP_COUNT:,} snapshot + {JOURNAL_COUNT:,} journal = {TOTAL:,}", file=sys.stderr)

        self.assertEqual(
            replica_count, TOTAL,
            f"Replica has {replica_count:,} nodes, expected {TOTAL:,}",
        )
        print(f"  PASS: S3 replica has all {TOTAL:,} nodes", file=sys.stderr)


if __name__ == "__main__":
    unittest.main(verbosity=2)
