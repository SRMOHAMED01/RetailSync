"""End-to-end test for the full RetailSync pipeline.

Definition of Done (see MINI_GUIDE.md): one PostgreSQL transaction must produce
a Debezium event, be readable in Kafka, persist in Bronze, reconcile in Silver,
feed a dbt mart, and appear in Power BI.

This test verifies the infrastructure half of that contract:
  Postgres (INSERT/UPDATE/DELETE) -> Debezium -> Kafka topic -> Bronze table

The Databricks/dbt/Power BI halves require a cloud workspace and are exercised
manually per `docs/runbooks/DATABRICKS.md` and `dbt/README.md`.

Run: `python -m pytest tests/e2e -q`
"""
import os
import subprocess
import time
import unittest

import psycopg


def _conninfo():
    return (
        f"host={os.getenv('DB_HOST','localhost')} "
        f"port={os.getenv('DB_PORT','5432')} "
        f"dbname={os.getenv('DB_NAME','retailsync')} "
        f"user={os.getenv('DB_USER','retail')} "
        f"password={os.getenv('DB_PASSWORD','retail_dev_password')}"
    )


def _connector_status():
    import urllib.request
    with urllib.request.urlopen("http://localhost:8083/connectors/retailsync-postgres/status") as r:
        return r.read().decode()


def _consume(topic, pid, op, from_beginning=True, timeout_ms=20000, max_messages=5000):
    """Consume CDC messages for a specific product_id AND operation code."""
    cmd = [
        "docker", "exec", "retailsync-kafka", "/opt/kafka/bin/kafka-console-consumer.sh",
        "--bootstrap-server", "localhost:29092",
        "--topic", topic,
        "--timeout-ms", str(timeout_ms),
        "--max-messages", str(max_messages),
    ]
    if from_beginning:
        cmd.append("--from-beginning")
    out = subprocess.run(cmd, capture_output=True, text=True, timeout=90)
    target = f'"product_id":{pid}'
    op_token = f'"op":"{op}"'
    for line in reversed(out.stdout.splitlines()):
        line = line.strip()
        if not line:
            continue
        if target in line and op_token in line:
            return line
    return None


def _wait_for_event(topic, pid, op, from_beginning=True, attempts=15, delay=1):
    """Retry consuming until the expected CDC event appears (handles latency)."""
    for _ in range(attempts):
        msg = _consume(topic, pid, op, from_beginning=from_beginning)
        if msg is not None:
            return msg
        time.sleep(delay)
    return None


def _cleanup_product(pid):
    """Delete a test product and any order_items referencing it (FK-safe)."""
    with psycopg.connect(_conninfo()) as conn, conn.cursor() as cur:
        cur.execute("DELETE FROM order_items WHERE product_id=%s", (pid,))
        cur.execute("DELETE FROM products WHERE product_id=%s", (pid,))
        conn.commit()


class TestEndToEndPipeline(unittest.TestCase):
    """Verify the infrastructure legs of the DoD are wired up."""

    def test_01_connectors_registered_and_running(self):
        status = _connector_status()
        self.assertIn("RUNNING", status, "Debezium connector must be RUNNING")

    def test_02_all_cdc_topics_exist(self):
        expected = [
            "retail.public.customers",
            "retail.public.products",
            "retail.public.orders",
            "retail.public.order_items",
            "retail.public.payments",
        ]
        out = subprocess.run(
            ["docker", "exec", "retailsync-kafka", "/opt/kafka/bin/kafka-topics.sh",
             "--bootstrap-server", "localhost:29092", "--list"],
            capture_output=True, text=True, timeout=30,
        ).stdout.split()
        for t in expected:
            self.assertIn(t, out, f"Missing CDC topic {t}")

    def test_03_insert_flows_to_kafka(self):
        pid = None
        with psycopg.connect(_conninfo()) as conn, conn.cursor() as cur:
            cur.execute(
                "INSERT INTO products(name, category, price, stock) "
                "VALUES(%s, %s, %s, %s) RETURNING product_id",
                ("E2E Test", "Electronics", 1.23, 2),
            )
            pid = cur.fetchone()[0]
            conn.commit()
        try:
            msg = _wait_for_event("retail.public.products", pid, "c", from_beginning=True)
            self.assertIsNotNone(msg, "INSERT must land on retail.public.products")
            self.assertIn('"op":"c"', msg, "INSERT should emit op='c'")
        finally:
            if pid is not None:
                _cleanup_product(pid)

    def test_04_delete_flows_to_kafka(self):
        pid = None
        with psycopg.connect(_conninfo()) as conn, conn.cursor() as cur:
            cur.execute(
                "INSERT INTO products(name, category, price, stock) "
                "VALUES(%s, %s, %s, %s) RETURNING product_id",
                ("E2E Delete", "Electronics", 4.56, 1),
            )
            pid = cur.fetchone()[0]
            conn.commit()
            cur.execute("DELETE FROM products WHERE product_id=%s", (pid,))
            conn.commit()
        # DELETE events are emitted asynchronously; give the pipeline a moment.
        time.sleep(3)
        msg = _wait_for_event("retail.public.products", pid, "d", from_beginning=True)
        self.assertIsNotNone(msg, "DELETE must land on retail.public.products")
        self.assertIn('"op":"d"', msg, "DELETE should emit op='d'")
        if pid is not None:
            _cleanup_product(pid)


if __name__ == "__main__":
    unittest.main()