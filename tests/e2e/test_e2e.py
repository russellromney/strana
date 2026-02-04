"""
End-to-end test for graphd using the neo4j Python driver (Bolt 4.4).

Tests:
  1. Comprehensive Bolt operations (DDL, CRUD, transactions, params)
  2. Local snapshot + restore + journal replay
  3. S3/Tigris snapshot + restore

Usage:
    # Via Makefile (recommended):
    make e2e

    # Direct:
    DYLD_LIBRARY_PATH=./lbug-lib GRAPHD_BINARY=./target/debug/graphd \
        STRANA_ROOT=. python tests/e2e/test_e2e.py -v
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
import uuid

import requests
from neo4j import GraphDatabase

GRAPHD_BINARY = os.environ.get(
    "GRAPHD_BINARY",
    os.path.join(os.path.dirname(__file__), "..", "..", "target", "debug", "graphd"),
)
STRANA_ROOT = os.environ.get(
    "STRANA_ROOT",
    os.path.join(os.path.dirname(__file__), "..", ".."),
)


# ─── Helpers ───


def find_free_ports(n=2):
    """Find n free TCP ports."""
    socks = []
    ports = []
    for _ in range(n):
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.bind(("127.0.0.1", 0))
        ports.append(s.getsockname()[1])
        socks.append(s)
    for s in socks:
        s.close()
    return ports


def load_s3_env():
    """Load .env.test and return (env_dict, bucket) for S3/Tigris."""
    try:
        from dotenv import dotenv_values
    except ImportError:
        return None, None

    env_path = os.path.join(STRANA_ROOT, ".env.test")
    if not os.path.exists(env_path):
        return None, None

    vals = dotenv_values(env_path)
    if not vals.get("TIGRIS_ACCESS_KEY_ID"):
        return None, None

    env = {
        "AWS_ACCESS_KEY_ID": vals["TIGRIS_ACCESS_KEY_ID"],
        "AWS_SECRET_ACCESS_KEY": vals["TIGRIS_SECRET_ACCESS_KEY"],
        "AWS_ENDPOINT_URL": vals["TIGRIS_ENDPOINT"],
        "AWS_REGION": "auto",
    }
    return env, vals["TIGRIS_BUCKET"]


def make_s3_client(s3_env):
    """Create a boto3 S3 client from env dict."""
    import boto3

    session = boto3.Session(
        aws_access_key_id=s3_env["AWS_ACCESS_KEY_ID"],
        aws_secret_access_key=s3_env["AWS_SECRET_ACCESS_KEY"],
        region_name=s3_env.get("AWS_REGION", "auto"),
    )
    return session.client("s3", endpoint_url=s3_env["AWS_ENDPOINT_URL"])


def list_s3_objects(bucket, prefix, s3_env):
    """List all S3 objects under a prefix. Returns list of dicts with Key, Size."""
    s3 = make_s3_client(s3_env)
    resp = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
    return resp.get("Contents", [])


def cleanup_s3_prefix(bucket, prefix, s3_env):
    """Delete all objects under a test prefix in S3."""
    try:
        s3 = make_s3_client(s3_env)
        resp = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
        for obj in resp.get("Contents", []):
            s3.delete_object(Bucket=bucket, Key=obj["Key"])
    except Exception as e:
        print(f"Warning: S3 cleanup failed: {e}", file=sys.stderr)


def list_local_snapshots(data_dir):
    """List local snapshot directories, sorted by name (sequence)."""
    snap_dir = os.path.join(data_dir, "snapshots")
    if not os.path.isdir(snap_dir):
        return []
    entries = sorted(
        e for e in os.listdir(snap_dir)
        if os.path.isdir(os.path.join(snap_dir, e))
    )
    return entries


# ─── GraphdProcess ───


class GraphdProcess:
    """Manages a graphd subprocess."""

    def __init__(
        self,
        data_dir,
        bolt_port,
        http_port,
        journal=False,
        s3_bucket=None,
        s3_prefix="",
        extra_env=None,
        retain_daily=None,
        retain_weekly=None,
        retain_monthly=None,
    ):
        self.data_dir = data_dir
        self.bolt_port = bolt_port
        self.http_port = http_port
        self.proc = None
        self._stdout = ""
        self._stderr = ""

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
        if s3_bucket:
            self.cmd.extend(["--s3-bucket", s3_bucket])
        if s3_prefix:
            self.cmd.extend(["--s3-prefix", s3_prefix])
        if retain_daily is not None:
            self.cmd.extend(["--retain-daily", str(retain_daily)])
        if retain_weekly is not None:
            self.cmd.extend(["--retain-weekly", str(retain_weekly)])
        if retain_monthly is not None:
            self.cmd.extend(["--retain-monthly", str(retain_monthly)])

        self.env = dict(os.environ)
        # macOS SIP strips DYLD_LIBRARY_PATH from subprocess environments,
        # so we must set it explicitly for graphd to find liblbug.dylib.
        lbug_lib = os.path.join(STRANA_ROOT, "lbug-lib")
        self.env["DYLD_LIBRARY_PATH"] = lbug_lib
        if extra_env:
            self.env.update(extra_env)

    def start(self, timeout=15):
        """Start graphd and wait for it to become ready."""
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
            try:
                r = requests.get(
                    f"http://127.0.0.1:{self.http_port}/health", timeout=1
                )
                if r.status_code == 200:
                    return
            except requests.ConnectionError:
                pass
            time.sleep(0.3)
        self.stop()
        raise RuntimeError(
            f"graphd did not become ready within {timeout}s\n"
            f"STDOUT:\n{self._stdout}\nSTDERR:\n{self._stderr}"
        )

    def stop(self):
        if self.proc and self.proc.poll() is None:
            self.proc.send_signal(signal.SIGTERM)
            try:
                self.proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self.proc.kill()
                self.proc.wait()
        self._capture_output()

    def _capture_output(self):
        if self.proc:
            try:
                out, err = self.proc.communicate(timeout=2)
                self._stdout = out.decode(errors="replace")
                self._stderr = err.decode(errors="replace")
            except Exception:
                pass

    def snapshot(self):
        """POST /v1/snapshot and return the response JSON."""
        r = requests.post(f"http://127.0.0.1:{self.http_port}/v1/snapshot")
        r.raise_for_status()
        return r.json()

    def restore(self, snapshot_path=None, s3_bucket=None, s3_prefix="", extra_env=None):
        """Run graphd --restore and wait for exit."""
        cmd = [
            GRAPHD_BINARY,
            "--data-dir", str(self.data_dir),
            "--restore",
        ]
        if snapshot_path:
            cmd.extend(["--snapshot", str(snapshot_path)])
        if s3_bucket:
            cmd.extend(["--s3-bucket", s3_bucket])
            if s3_prefix:
                cmd.extend(["--s3-prefix", s3_prefix])

        env = dict(self.env)
        if extra_env:
            env.update(extra_env)

        result = subprocess.run(
            cmd, env=env, capture_output=True, text=True, timeout=60
        )
        if result.returncode != 0:
            raise RuntimeError(
                f"Restore failed (rc={result.returncode}):\n"
                f"stdout: {result.stdout}\nstderr: {result.stderr}"
            )


# ─── Database operations ───


def populate_database(driver):
    """Run comprehensive operations via Bolt. Returns expected results dict."""
    with driver.session() as session:
        # DDL: Node tables
        session.run(
            "CREATE NODE TABLE Person("
            "name STRING, age INT64, score DOUBLE, active BOOLEAN, "
            "PRIMARY KEY(name))"
        ).consume()
        session.run(
            "CREATE NODE TABLE City("
            "name STRING, population INT64, PRIMARY KEY(name))"
        ).consume()

        # DDL: Relationship tables
        session.run("CREATE REL TABLE LIVES_IN(FROM Person TO City)").consume()
        session.run(
            "CREATE REL TABLE KNOWS(FROM Person TO Person, since INT64)"
        ).consume()

        # Create nodes: people
        people = [
            ("Alice", 30, 0.95, True),
            ("Bob", 42, 0.82, True),
            ("Carol", 28, 0.91, True),
            ("Dave", 35, 0.78, False),
            ("Eve", 22, 0.99, True),
        ]
        for name, age, score, active in people:
            session.run(
                "CREATE (:Person {name: $name, age: $age, score: $score, "
                "active: $active})",
                name=name,
                age=age,
                score=score,
                active=active,
            ).consume()

        # Create nodes: cities
        cities = [("New York", 8_300_000), ("London", 8_800_000), ("Tokyo", 13_900_000)]
        for name, pop in cities:
            session.run(
                "CREATE (:City {name: $name, population: $pop})",
                name=name,
                pop=pop,
            ).consume()

        # Create relationships: LIVES_IN
        edges = [
            ("Alice", "New York"),
            ("Bob", "London"),
            ("Carol", "Tokyo"),
            ("Dave", "New York"),
            ("Eve", "London"),
        ]
        for person, city in edges:
            session.run(
                "MATCH (p:Person {name: $p}), (c:City {name: $c}) "
                "CREATE (p)-[:LIVES_IN]->(c)",
                p=person,
                c=city,
            ).consume()

        # Create relationships: KNOWS
        knows = [
            ("Alice", "Bob", 2020),
            ("Alice", "Carol", 2021),
            ("Bob", "Dave", 2019),
            ("Carol", "Eve", 2022),
        ]
        for a, b, since in knows:
            session.run(
                "MATCH (a:Person {name: $a}), (b:Person {name: $b}) "
                "CREATE (a)-[:KNOWS {since: $since}]->(b)",
                a=a,
                b=b,
                since=since,
            ).consume()

        # Parameterized update
        session.run(
            "MATCH (p:Person {name: $name}) SET p.score = $score",
            name="Alice",
            score=0.97,
        ).consume()

    # Committed transaction: add Frank
    with driver.session() as session:
        tx = session.begin_transaction()
        tx.run(
            "CREATE (:Person {name: 'Frank', age: 50, score: 0.65, active: true})"
        ).consume()
        tx.run(
            "MATCH (f:Person {name: 'Frank'}), (c:City {name: 'Tokyo'}) "
            "CREATE (f)-[:LIVES_IN]->(c)"
        ).consume()
        tx.commit()

    # Rolled-back transaction: Ghost should NOT appear
    with driver.session() as session:
        tx = session.begin_transaction()
        tx.run(
            "CREATE (:Person {name: 'Ghost', age: 0, score: 0.0, active: false})"
        ).consume()
        tx.rollback()


def record_expected_results(driver):
    """Run verification queries and return results dict."""
    results = {}
    with driver.session() as session:
        # Q1: Person count
        r = session.run("MATCH (p:Person) RETURN count(p) AS cnt")
        results["person_count"] = r.single()["cnt"]

        # Q2: All people sorted
        r = session.run(
            "MATCH (p:Person) RETURN p.name AS name, p.age AS age, "
            "p.score AS score, p.active AS active ORDER BY p.name"
        )
        results["all_people"] = [dict(rec) for rec in r]

        # Q3: City count
        r = session.run("MATCH (c:City) RETURN count(c) AS cnt")
        results["city_count"] = r.single()["cnt"]

        # Q4: LIVES_IN count
        r = session.run("MATCH ()-[r:LIVES_IN]->() RETURN count(r) AS cnt")
        results["lives_in_count"] = r.single()["cnt"]

        # Q5: KNOWS count
        r = session.run("MATCH ()-[r:KNOWS]->() RETURN count(r) AS cnt")
        results["knows_count"] = r.single()["cnt"]

        # Q6: Alice's updated score
        r = session.run("MATCH (p:Person {name: 'Alice'}) RETURN p.score AS score")
        results["alice_score"] = r.single()["score"]

        # Q7: Ghost should not exist (rolled back)
        r = session.run(
            "MATCH (p:Person {name: 'Ghost'}) RETURN count(p) AS cnt"
        )
        results["ghost_count"] = r.single()["cnt"]

        # Q8: Frank should exist (committed tx)
        r = session.run("MATCH (p:Person {name: 'Frank'}) RETURN p.age AS age")
        results["frank_age"] = r.single()["age"]

        # Q9: LIVES_IN pairs
        r = session.run(
            "MATCH (p:Person)-[:LIVES_IN]->(c:City) "
            "RETURN p.name AS person, c.name AS city ORDER BY p.name"
        )
        results["lives_in_pairs"] = [(rec["person"], rec["city"]) for rec in r]

        # Q10: City populations
        r = session.run(
            "MATCH (c:City) RETURN c.name AS name, c.population AS pop "
            "ORDER BY c.name"
        )
        results["city_data"] = [(rec["name"], rec["pop"]) for rec in r]

    return results


def assert_results_match(test_case, expected, actual, label=""):
    """Compare two result dicts with informative assertion messages."""
    prefix = f"[{label}] " if label else ""
    for key in expected:
        test_case.assertIn(key, actual, f"{prefix}Missing key '{key}'")
        exp = expected[key]
        act = actual[key]

        if isinstance(exp, float):
            test_case.assertAlmostEqual(
                exp, act, places=4, msg=f"{prefix}Key '{key}'"
            )
        elif isinstance(exp, list) and exp and isinstance(exp[0], dict):
            test_case.assertEqual(
                len(exp), len(act),
                f"{prefix}Key '{key}': row count mismatch",
            )
            for i, (e_row, a_row) in enumerate(zip(exp, act)):
                for k in e_row:
                    if isinstance(e_row[k], float):
                        test_case.assertAlmostEqual(
                            e_row[k], a_row[k], places=4,
                            msg=f"{prefix}Key '{key}' row {i} field '{k}'",
                        )
                    else:
                        test_case.assertEqual(
                            e_row[k], a_row[k],
                            f"{prefix}Key '{key}' row {i} field '{k}'",
                        )
        else:
            test_case.assertEqual(exp, act, f"{prefix}Key '{key}'")


# ─── Test cases ───


class TestGraphdE2E(unittest.TestCase):
    def setUp(self):
        self.tmp_dir = tempfile.mkdtemp(prefix="graphd-e2e-")
        self.processes = []
        self.drivers = []

    def tearDown(self):
        for d in self.drivers:
            try:
                d.close()
            except Exception:
                pass
        for p in self.processes:
            try:
                p.stop()
            except Exception:
                pass
        shutil.rmtree(self.tmp_dir, ignore_errors=True)

    def _make_driver(self, bolt_port):
        driver = GraphDatabase.driver(
            f"bolt://127.0.0.1:{bolt_port}",
            auth=("neo4j", ""),
            encrypted=False,
            max_connection_lifetime=30,
            connection_timeout=10,
        )
        self.drivers.append(driver)
        return driver

    def _start_graphd(self, data_dir=None, journal=True, **kwargs):
        if data_dir is None:
            data_dir = os.path.join(self.tmp_dir, f"data-{uuid.uuid4().hex[:8]}")
            os.makedirs(data_dir, exist_ok=True)
        bolt_port, http_port = find_free_ports(2)
        graphd = GraphdProcess(
            data_dir=data_dir,
            bolt_port=bolt_port,
            http_port=http_port,
            journal=journal,
            **kwargs,
        )
        self.processes.append(graphd)
        graphd.start()
        return graphd, bolt_port, http_port

    # ── Test 1: Bolt operations ──

    def test_bolt_operations(self):
        """Comprehensive Bolt operations work correctly via neo4j driver."""
        graphd, bolt_port, _ = self._start_graphd()
        driver = self._make_driver(bolt_port)

        # Populate
        populate_database(driver)

        # Verify
        results = record_expected_results(driver)

        self.assertEqual(results["person_count"], 6)
        self.assertEqual(results["city_count"], 3)
        self.assertEqual(results["lives_in_count"], 6)
        self.assertEqual(results["knows_count"], 4)
        self.assertAlmostEqual(results["alice_score"], 0.97, places=4)
        self.assertEqual(results["ghost_count"], 0)
        self.assertEqual(results["frank_age"], 50)
        self.assertEqual(len(results["lives_in_pairs"]), 6)
        self.assertEqual(len(results["all_people"]), 6)

        print("  Bolt operations: all checks passed")

    # ── Test 2: Local snapshot + restore ──

    def test_local_snapshot_restore(self):
        """Snapshot to local disk, restore, verify data identical."""
        data_dir = os.path.join(self.tmp_dir, "local-restore")
        os.makedirs(data_dir)
        bolt_port, http_port = find_free_ports(2)

        graphd = GraphdProcess(
            data_dir=data_dir,
            bolt_port=bolt_port,
            http_port=http_port,
            journal=True,
        )
        self.processes.append(graphd)
        graphd.start()

        driver = self._make_driver(bolt_port)
        populate_database(driver)
        expected = record_expected_results(driver)

        # Create snapshot
        snap_resp = graphd.snapshot()
        self.assertIn("data", snap_resp)
        print(f"  Snapshot created: seq={snap_resp['data']['values'][0][0]}")

        # Stop server + close driver
        driver.close()
        self.drivers.remove(driver)
        graphd.stop()

        # Delete the live database (simulate data loss)
        db_path = os.path.join(data_dir, "db")
        if os.path.exists(db_path):
            if os.path.isdir(db_path):
                shutil.rmtree(db_path)
            else:
                os.remove(db_path)
        print("  Deleted db")

        # Restore from local snapshot
        graphd.restore()
        print("  Restore completed")

        # Restart server
        bolt_port2, http_port2 = find_free_ports(2)
        graphd2 = GraphdProcess(
            data_dir=data_dir,
            bolt_port=bolt_port2,
            http_port=http_port2,
            journal=True,
        )
        self.processes.append(graphd2)
        graphd2.start()

        # Reconnect and verify
        driver2 = self._make_driver(bolt_port2)
        restored = record_expected_results(driver2)
        assert_results_match(self, expected, restored, label="local-restore")
        print("  Local restore: all checks passed")

    # ── Test 3: S3/Tigris snapshot + restore ──

    def test_s3_snapshot_restore(self):
        """Snapshot to S3/Tigris, restore from S3, verify data identical."""
        s3_env, bucket = load_s3_env()
        if s3_env is None:
            self.skipTest("S3 credentials not available (.env.test missing or empty)")

        s3_prefix = f"e2e-test/{uuid.uuid4()}/"
        self.addCleanup(cleanup_s3_prefix, bucket, s3_prefix, s3_env)

        data_dir = os.path.join(self.tmp_dir, "s3-restore")
        os.makedirs(data_dir)
        bolt_port, http_port = find_free_ports(2)

        graphd = GraphdProcess(
            data_dir=data_dir,
            bolt_port=bolt_port,
            http_port=http_port,
            journal=True,
            s3_bucket=bucket,
            s3_prefix=s3_prefix,
            extra_env=s3_env,
        )
        self.processes.append(graphd)
        graphd.start()

        driver = self._make_driver(bolt_port)
        populate_database(driver)
        expected = record_expected_results(driver)

        # Create snapshot (also uploads to S3)
        snap_resp = graphd.snapshot()
        self.assertIn("data", snap_resp)
        print(f"  S3 snapshot created: seq={snap_resp['data']['values'][0][0]}")

        # Stop server + close driver
        driver.close()
        self.drivers.remove(driver)
        graphd.stop()

        # Delete ENTIRE data directory (simulate complete data loss)
        shutil.rmtree(data_dir)
        os.makedirs(data_dir)
        print("  Deleted entire data directory")

        # Restore from S3
        graphd.restore(s3_bucket=bucket, s3_prefix=s3_prefix, extra_env=s3_env)
        print("  S3 restore completed")

        # Restart server
        bolt_port2, http_port2 = find_free_ports(2)
        graphd2 = GraphdProcess(
            data_dir=data_dir,
            bolt_port=bolt_port2,
            http_port=http_port2,
            journal=True,
            extra_env=s3_env,
        )
        self.processes.append(graphd2)
        graphd2.start()

        # Reconnect and verify
        driver2 = self._make_driver(bolt_port2)
        restored = record_expected_results(driver2)
        assert_results_match(self, expected, restored, label="s3-restore")
        print("  S3 restore: all checks passed")

    # ── Test 4: Incremental snapshots ──

    def test_incremental_snapshots(self):
        """Two snapshots, restore from latest, verify all data present."""
        s3_env, bucket = load_s3_env()
        if s3_env is None:
            self.skipTest("S3 credentials not available")

        s3_prefix = f"e2e-test/{uuid.uuid4()}/"
        self.addCleanup(cleanup_s3_prefix, bucket, s3_prefix, s3_env)

        data_dir = os.path.join(self.tmp_dir, "incremental")
        os.makedirs(data_dir)

        graphd = GraphdProcess(
            data_dir=data_dir,
            bolt_port=(ports := find_free_ports(2))[0],
            http_port=ports[1],
            journal=True,
            s3_bucket=bucket,
            s3_prefix=s3_prefix,
            extra_env=s3_env,
        )
        self.processes.append(graphd)
        graphd.start()

        driver = self._make_driver(ports[0])

        # Round 1: schema + initial data
        populate_database(driver)
        snap1 = graphd.snapshot()
        seq1 = snap1["data"]["values"][0][0]
        print(f"  Snapshot 1: seq={seq1}")

        # Round 2: add more data
        with driver.session() as session:
            session.run(
                "CREATE (:Person {name: 'Grace', age: 40, score: 0.88, active: true})"
            ).consume()
            session.run(
                "MATCH (g:Person {name: 'Grace'}), (c:City {name: 'London'}) "
                "CREATE (g)-[:LIVES_IN]->(c)"
            ).consume()

        snap2 = graphd.snapshot()
        seq2 = snap2["data"]["values"][0][0]
        print(f"  Snapshot 2: seq={seq2}")
        self.assertGreater(seq2, seq1)

        # Record expected state (should include both rounds)
        expected = record_expected_results(driver)
        self.assertEqual(expected["person_count"], 7)  # 6 + Grace

        # Verify Grace in results
        with driver.session() as session:
            r = session.run(
                "MATCH (p:Person {name: 'Grace'}) RETURN p.age AS age"
            )
            self.assertEqual(r.single()["age"], 40)

        # Stop, delete everything, restore from S3 (gets latest = snap2)
        driver.close()
        self.drivers.remove(driver)
        graphd.stop()

        shutil.rmtree(data_dir)
        os.makedirs(data_dir)

        graphd.restore(s3_bucket=bucket, s3_prefix=s3_prefix, extra_env=s3_env)
        print("  Restored from S3 (latest snapshot)")

        bolt2, http2 = find_free_ports(2)
        graphd2 = GraphdProcess(
            data_dir=data_dir,
            bolt_port=bolt2,
            http_port=http2,
            journal=True,
            extra_env=s3_env,
        )
        self.processes.append(graphd2)
        graphd2.start()

        driver2 = self._make_driver(bolt2)
        restored = record_expected_results(driver2)
        assert_results_match(self, expected, restored, label="incremental")

        # Confirm Grace survived
        with driver2.session() as session:
            r = session.run(
                "MATCH (p:Person {name: 'Grace'}) RETURN p.age AS age"
            )
            self.assertEqual(r.single()["age"], 40)

        print("  Incremental snapshots: all checks passed")

    # ── Test 5: Journal replay after local restore ──

    def test_journal_replay_after_restore(self):
        """Data written after snapshot is recovered via journal replay."""
        data_dir = os.path.join(self.tmp_dir, "journal-replay")
        os.makedirs(data_dir)

        graphd = GraphdProcess(
            data_dir=data_dir,
            bolt_port=(ports := find_free_ports(2))[0],
            http_port=ports[1],
            journal=True,
        )
        self.processes.append(graphd)
        graphd.start()

        driver = self._make_driver(ports[0])

        # Populate and snapshot
        populate_database(driver)
        graphd.snapshot()
        print("  Snapshot taken")

        # Write MORE data AFTER the snapshot (only in journal, not in snapshot)
        with driver.session() as session:
            session.run(
                "CREATE (:Person {name: 'Hank', age: 55, score: 0.72, active: true})"
            ).consume()
            session.run(
                "MATCH (h:Person {name: 'Hank'}), (c:City {name: 'New York'}) "
                "CREATE (h)-[:LIVES_IN]->(c)"
            ).consume()
            session.run(
                "MATCH (p:Person {name: 'Bob'}) SET p.score = 0.50"
            ).consume()

        # Record expected state (includes post-snapshot data)
        expected = record_expected_results(driver)
        self.assertEqual(expected["person_count"], 7)  # 6 + Hank

        # Verify post-snapshot data
        with driver.session() as session:
            r = session.run(
                "MATCH (p:Person {name: 'Hank'}) RETURN p.age AS age"
            )
            self.assertEqual(r.single()["age"], 55)
            r = session.run(
                "MATCH (p:Person {name: 'Bob'}) RETURN p.score AS score"
            )
            self.assertAlmostEqual(r.single()["score"], 0.50, places=4)

        # Stop, delete ONLY the db (keep snapshots + journal for replay)
        driver.close()
        self.drivers.remove(driver)
        graphd.stop()

        db_path = os.path.join(data_dir, "db")
        if os.path.isdir(db_path):
            shutil.rmtree(db_path)
        else:
            os.remove(db_path)
        print("  Deleted db (kept snapshots + journal)")

        # Restore: should restore from snapshot + replay journal
        graphd.restore()

        bolt2, http2 = find_free_ports(2)
        graphd2 = GraphdProcess(
            data_dir=data_dir,
            bolt_port=bolt2,
            http_port=http2,
            journal=True,
        )
        self.processes.append(graphd2)
        graphd2.start()

        driver2 = self._make_driver(bolt2)
        restored = record_expected_results(driver2)
        assert_results_match(self, expected, restored, label="journal-replay")

        # Confirm post-snapshot data survived via journal replay
        with driver2.session() as session:
            r = session.run(
                "MATCH (p:Person {name: 'Hank'}) RETURN p.age AS age"
            )
            self.assertEqual(r.single()["age"], 55)
            r = session.run(
                "MATCH (p:Person {name: 'Bob'}) RETURN p.score AS score"
            )
            self.assertAlmostEqual(r.single()["score"], 0.50, places=4)

        print("  Journal replay: all checks passed")

    # ── Test 6: Retention pruning ──

    def test_retention_pruning(self):
        """Retention policy prunes old local snapshots."""
        data_dir = os.path.join(self.tmp_dir, "retention")
        os.makedirs(data_dir)

        graphd = GraphdProcess(
            data_dir=data_dir,
            bolt_port=(ports := find_free_ports(2))[0],
            http_port=ports[1],
            journal=True,
            retain_daily=2,
            retain_weekly=0,
            retain_monthly=0,
        )
        self.processes.append(graphd)
        graphd.start()

        driver = self._make_driver(ports[0])
        populate_database(driver)

        # Take 4 snapshots, each with a small data change to advance sequence
        seqs = []
        for i in range(4):
            with driver.session() as session:
                session.run(
                    f"MATCH (p:Person {{name: 'Alice'}}) SET p.score = {0.5 + i * 0.1}"
                ).consume()
            snap = graphd.snapshot()
            seq = snap["data"]["values"][0][0]
            seqs.append(seq)
            local = list_local_snapshots(data_dir)
            print(f"  Snapshot {i+1}: seq={seq}, local_count={len(local)}")

        # With retain_daily=2, only the 2 most recent should remain
        remaining = list_local_snapshots(data_dir)
        self.assertEqual(
            len(remaining), 2,
            f"Expected 2 local snapshots after retention, got {len(remaining)}: {remaining}",
        )
        # The remaining should be the two highest sequences
        remaining_seqs = sorted(int(d) for d in remaining)
        expected_seqs = sorted(seqs[-2:])
        self.assertEqual(
            remaining_seqs, expected_seqs,
            f"Expected sequences {expected_seqs}, got {remaining_seqs}",
        )
        print(f"  Retention: {len(remaining)} snapshots kept (correct)")

    # ── Test 7: Clean machine S3 restore ──

    def test_clean_machine_s3_restore(self):
        """Restore to a completely fresh data directory from S3 only."""
        s3_env, bucket = load_s3_env()
        if s3_env is None:
            self.skipTest("S3 credentials not available")

        s3_prefix = f"e2e-test/{uuid.uuid4()}/"
        self.addCleanup(cleanup_s3_prefix, bucket, s3_prefix, s3_env)

        # Phase 1: populate and snapshot to S3 on one "machine"
        data_dir_source = os.path.join(self.tmp_dir, "source")
        os.makedirs(data_dir_source)

        graphd = GraphdProcess(
            data_dir=data_dir_source,
            bolt_port=(ports := find_free_ports(2))[0],
            http_port=ports[1],
            journal=True,
            s3_bucket=bucket,
            s3_prefix=s3_prefix,
            extra_env=s3_env,
        )
        self.processes.append(graphd)
        graphd.start()

        driver = self._make_driver(ports[0])
        populate_database(driver)
        expected = record_expected_results(driver)

        graphd.snapshot()
        driver.close()
        self.drivers.remove(driver)
        graphd.stop()
        print("  Source machine: populated + snapshotted to S3")

        # Phase 2: restore to a COMPLETELY new, empty directory (different "machine")
        data_dir_target = os.path.join(self.tmp_dir, "target")
        os.makedirs(data_dir_target)

        # Verify target is truly empty
        self.assertEqual(os.listdir(data_dir_target), [])

        # Create a new GraphdProcess just for restore (different data dir)
        target_graphd = GraphdProcess(
            data_dir=data_dir_target,
            bolt_port=0,   # not used for restore
            http_port=0,
            extra_env=s3_env,
        )
        target_graphd.restore(
            s3_bucket=bucket, s3_prefix=s3_prefix, extra_env=s3_env
        )
        print("  Target machine: restored from S3 to empty dir")

        # Start server on the restored data
        bolt2, http2 = find_free_ports(2)
        graphd2 = GraphdProcess(
            data_dir=data_dir_target,
            bolt_port=bolt2,
            http_port=http2,
            journal=True,
            extra_env=s3_env,
        )
        self.processes.append(graphd2)
        graphd2.start()

        driver2 = self._make_driver(bolt2)
        restored = record_expected_results(driver2)
        assert_results_match(self, expected, restored, label="clean-machine")
        print("  Clean machine restore: all checks passed")

    # ── Test 8: Cross-verify S3 objects ──

    def test_s3_object_verification(self):
        """Verify S3 objects exist with correct per-file chunked format."""
        s3_env, bucket = load_s3_env()
        if s3_env is None:
            self.skipTest("S3 credentials not available")

        s3_prefix = f"e2e-test/{uuid.uuid4()}/"
        self.addCleanup(cleanup_s3_prefix, bucket, s3_prefix, s3_env)

        data_dir = os.path.join(self.tmp_dir, "s3-verify")
        os.makedirs(data_dir)

        graphd = GraphdProcess(
            data_dir=data_dir,
            bolt_port=(ports := find_free_ports(2))[0],
            http_port=ports[1],
            journal=True,
            s3_bucket=bucket,
            s3_prefix=s3_prefix,
            extra_env=s3_env,
        )
        self.processes.append(graphd)
        graphd.start()

        driver = self._make_driver(ports[0])
        populate_database(driver)

        # Before snapshot: no objects under this prefix
        objects_before = list_s3_objects(bucket, s3_prefix, s3_env)
        self.assertEqual(len(objects_before), 0, "S3 prefix should be empty before snapshot")

        # Take snapshot
        snap_resp = graphd.snapshot()
        seq = snap_resp["data"]["values"][0][0]
        print(f"  Snapshot taken: seq={seq}")

        # Verify per-file chunked format in S3
        objects_after = list_s3_objects(bucket, s3_prefix, s3_env)
        keys_after = [o["Key"] for o in objects_after]

        snap_prefix = f"{s3_prefix}snapshots/{seq:016}"
        manifests = [k for k in keys_after if k == f"{snap_prefix}/manifest.json"]
        metas = [k for k in keys_after if k == f"{snap_prefix}/snapshot.meta"]
        data_files = [k for k in keys_after if "/files/" in k and k.endswith(".zst")]

        self.assertEqual(len(manifests), 1, f"Expected manifest.json, got: {manifests}")
        self.assertEqual(len(metas), 1, f"Expected snapshot.meta, got: {metas}")
        self.assertGreater(len(data_files), 0, f"Expected at least 1 .zst data file, got: {keys_after}")

        for obj in objects_after:
            self.assertGreater(obj["Size"], 0, f"Object should have non-zero size: {obj['Key']}")
        print(f"  Snapshot 1: {len(objects_after)} objects ({len(data_files)} data files)")

        # Take second snapshot → should have two snapshot directories
        with driver.session() as session:
            session.run(
                "MATCH (p:Person {name: 'Alice'}) SET p.score = 0.55"
            ).consume()
        snap2 = graphd.snapshot()
        seq2 = snap2["data"]["values"][0][0]

        objects_final = list_s3_objects(bucket, s3_prefix, s3_env)
        keys_final = [o["Key"] for o in objects_final]

        snap_prefix2 = f"{s3_prefix}snapshots/{seq2:016}"
        manifests2 = [k for k in keys_final if k == f"{snap_prefix2}/manifest.json"]
        self.assertEqual(len(manifests2), 1, f"Expected manifest.json for snap2, got: {manifests2}")

        # Both snapshot prefixes should be distinct
        self.assertNotEqual(snap_prefix, snap_prefix2, "Snapshot prefixes should differ")

        # All objects should be under the test prefix
        for obj in objects_final:
            self.assertTrue(
                obj["Key"].startswith(s3_prefix),
                f"Key should start with prefix: {obj['Key']}",
            )
            self.assertGreater(obj["Size"], 0)

        print(f"  Snapshot 2: {len(objects_final)} total objects")


if __name__ == "__main__":
    unittest.main(verbosity=2)
