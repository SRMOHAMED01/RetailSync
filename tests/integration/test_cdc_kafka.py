"""Integration test: PostgreSQL -> Debezium -> Kafka CDC topics.

Verifies that an INSERT/UPDATE/DELETE in PostgreSQL produces a CDC event on the
matching `retail.public.<table>` Kafka topic with the expected `op` code.

Run: `python -m pytest tests/integration -q`
Requires the local stack (docker compose up) and a `psycopg` install.
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


class TestPostgresToKafka(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.conn = psycopg.connect(_conninfo())

    @classmethod
    def tearDownClass(cls):
        cls.conn.close()

    def _cleanup_product(self, pid):
        """Delete a test product and any order_items referencing it."""
        with self.conn.cursor() as cur:
            cur.execute("DELETE FROM order_items WHERE product_id=%s", (pid,))
            cur.execute("DELETE FROM products WHERE product_id=%s", (pid,))
            self.conn.commit()

    def _insert_test_product(self):
        with self.conn.cursor() as cur:
            cur.execute(
                "INSERT INTO products(name, category, price, stock) "
                "VALUES(%s, %s, %s, %s) RETURNING product_id",
                ("Integration Test", "Electronics", 9.99, 5),
            )
            pid = cur.fetchone()[0]
            self.conn.commit()
        return pid

    def test_insert_produces_cdc_event(self):
        pid = self._insert_test_product()
        try:
            msg = _wait_for_event("retail.public.products", pid, "c", from_beginning=True)
            self.assertIsNotNone(msg, "No CDC message for our product on retail.public.products")
            self.assertIn('"op":"c"', msg, "INSERT should emit op='c'")
        finally:
            self._cleanup_product(pid)

    def test_update_produces_cdc_event(self):
        pid = self._insert_test_product()
        try:
            with self.conn.cursor() as cur:
                cur.execute(
                    "UPDATE products SET stock=%s WHERE product_id=%s", (3, pid)
                )
                self.conn.commit()
            msg = _wait_for_event("retail.public.products", pid, "u", from_beginning=True)
            self.assertIsNotNone(msg, "No CDC message for our product on retail.public.products")
            self.assertIn('"op":"u"', msg, "UPDATE should emit op='u'")
        finally:
            self._cleanup_product(pid)


if __name__ == "__main__":
    unittest.main()