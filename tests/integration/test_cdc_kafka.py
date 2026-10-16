"""Integration test: PostgreSQL -> Debezium -> Kafka CDC topics.

Verifies that INSERT / UPDATE / DELETE on each of the 5 source tables produces
a CDC event on the matching `retail.public.<table>` Kafka topic with the
expected `op` code.

Definition of Done (MINI_GUIDE.md): every transaction must produce a Debezium
event readable in Kafka, for INSERT, UPDATE and DELETE.

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


def _consume(topic, pk_col, pk_val, op, from_beginning=True, timeout_ms=20000):
    """Consume CDC messages for a specific row AND operation code.

    Uses `--from-beginning --max-messages` so stale replay events from the
    simulator do not mask the event under test. Returns the matching JSON line
    or None.
    """
    cmd = [
        "docker", "exec", "retailsync-kafka", "/opt/kafka/bin/kafka-console-consumer.sh",
        "--bootstrap-server", "localhost:29092",
        "--topic", topic,
        "--timeout-ms", str(timeout_ms),
        "--max-messages", "5000",
    ]
    if from_beginning:
        cmd.append("--from-beginning")
    out = subprocess.run(cmd, capture_output=True, text=True, timeout=90)
    target = f'"{pk_col}":{pk_val}'
    op_token = f'"op":"{op}"'
    for line in reversed(out.stdout.splitlines()):
        line = line.strip()
        if not line:
            continue
        if target in line and op_token in line:
            return line
    return None


def _wait_for_event(topic, pk_col, pk_val, op, from_beginning=True, attempts=15, delay=1):
    """Retry consuming until the expected CDC event appears (handles latency)."""
    for _ in range(attempts):
        msg = _consume(topic, pk_col, pk_val, op, from_beginning=from_beginning)
        if msg is not None:
            return msg
        time.sleep(delay)
    return None


def _cleanup(conn, table, pk_col, pk_val, extra_deletes=None):
    """Delete a test row and any FK-referencing children."""
    with conn.cursor() as cur:
        if extra_deletes:
            for sql, params in extra_deletes:
                cur.execute(sql, params)
        cur.execute(f"DELETE FROM {table} WHERE {pk_col}=%s", (pk_val,))
        conn.commit()


class TestCDCPerEntity(unittest.TestCase):
    """INSERT / UPDATE / DELETE for all 5 CDC tables."""

    @classmethod
    def setUpClass(cls):
        cls.conn = psycopg.connect(_conninfo())

    @classmethod
    def tearDownClass(cls):
        cls.conn.close()

    # ---- customers (no FK) ----
    def test_customers_cud(self):
        with self.conn.cursor() as cur:
            cur.execute(
                "INSERT INTO customers(first_name,last_name,email,city) "
                "VALUES(%s,%s,%s,%s) RETURNING customer_id",
                ("CDC", "Test", "cdc.test@example.com", "TestCity"),
            )
            pk = cur.fetchone()[0]
            self.conn.commit()
        try:
            self.assertIsNotNone(
                _wait_for_event("retail.public.customers", "customer_id", pk, "c"),
                "customers INSERT missing",
            )
            with self.conn.cursor() as cur:
                cur.execute(
                    "UPDATE customers SET city=%s WHERE customer_id=%s",
                    ("UpdatedCity", pk),
                )
                self.conn.commit()
            self.assertIsNotNone(
                _wait_for_event("retail.public.customers", "customer_id", pk, "u"),
                "customers UPDATE missing",
            )
            _cleanup(self.conn, "customers", "customer_id", pk)
            self.assertIsNotNone(
                _consume("retail.public.customers", "customer_id", pk, "d",
                         from_beginning=True),
                "customers DELETE missing",
            )
        except AssertionError:
            _cleanup(self.conn, "customers", "customer_id", pk)
            raise

    # ---- products (no FK) ----
    def test_products_cud(self):
        with self.conn.cursor() as cur:
            cur.execute(
                "INSERT INTO products(name,category,price,stock) "
                "VALUES(%s,%s,%s,%s) RETURNING product_id",
                ("CDC Product", "Electronics", 9.99, 5),
            )
            pk = cur.fetchone()[0]
            self.conn.commit()
        try:
            self.assertIsNotNone(
                _wait_for_event("retail.public.products", "product_id", pk, "c"),
                "products INSERT missing",
            )
            with self.conn.cursor() as cur:
                cur.execute(
                    "UPDATE products SET stock=%s WHERE product_id=%s", (3, pk)
                )
                self.conn.commit()
            self.assertIsNotNone(
                _wait_for_event("retail.public.products", "product_id", pk, "u"),
                "products UPDATE missing",
            )
            _cleanup(self.conn, "products", "product_id", pk,
                     extra_deletes=[
                         ("DELETE FROM order_items WHERE product_id=%s", (pk,)),
                     ])
            self.assertIsNotNone(
                _consume("retail.public.products", "product_id", pk, "d",
                         from_beginning=True),
                "products DELETE missing",
            )
        except AssertionError:
            _cleanup(self.conn, "products", "product_id", pk,
                     extra_deletes=[
                         ("DELETE FROM order_items WHERE product_id=%s", (pk,)),
                     ])
            raise

    # ---- orders (FK to customers) ----
    def test_orders_cud(self):
        # Use an existing seed customer (id=1) to satisfy the FK.
        with self.conn.cursor() as cur:
            cur.execute(
                "INSERT INTO orders(customer_id,status,total_amount) "
                "VALUES(%s,'CREATED',%s) RETURNING order_id",
                (1, 10.00),
            )
            pk = cur.fetchone()[0]
            self.conn.commit()
        try:
            self.assertIsNotNone(
                _wait_for_event("retail.public.orders", "order_id", pk, "c"),
                "orders INSERT missing",
            )
            with self.conn.cursor() as cur:
                cur.execute(
                    "UPDATE orders SET status='PAID' WHERE order_id=%s", (pk,)
                )
                self.conn.commit()
            self.assertIsNotNone(
                _wait_for_event("retail.public.orders", "order_id", pk, "u"),
                "orders UPDATE missing",
            )
            _cleanup(self.conn, "orders", "order_id", pk,
                     extra_deletes=[
                         ("DELETE FROM order_items WHERE order_id=%s", (pk,)),
                         ("DELETE FROM payments WHERE order_id=%s", (pk,)),
                     ])
            self.assertIsNotNone(
                _consume("retail.public.orders", "order_id", pk, "d",
                         from_beginning=True),
                "orders DELETE missing",
            )
        except AssertionError:
            _cleanup(self.conn, "orders", "order_id", pk,
                     extra_deletes=[
                         ("DELETE FROM order_items WHERE order_id=%s", (pk,)),
                         ("DELETE FROM payments WHERE order_id=%s", (pk,)),
                     ])
            raise

    # ---- order_items (FK to orders + products) ----
    def test_order_items_cud(self):
        # Seed an order and product to satisfy FKs.
        with self.conn.cursor() as cur:
            cur.execute(
                "INSERT INTO orders(customer_id,status,total_amount) "
                "VALUES(%s,'CREATED',%s) RETURNING order_id", (1, 10.00))
            order_id = cur.fetchone()[0]
            cur.execute(
                "INSERT INTO products(name,category,price,stock) "
                "VALUES(%s,%s,%s,%s) RETURNING product_id",
                ("OI Product", "Electronics", 5.00, 10),
            )
            product_id = cur.fetchone()[0]
            self.conn.commit()
        try:
            with self.conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO order_items(order_id,product_id,quantity,unit_price) "
                    "VALUES(%s,%s,%s,%s) RETURNING order_item_id",
                    (order_id, product_id, 2, 5.00),
                )
                pk = cur.fetchone()[0]
                self.conn.commit()
            self.assertIsNotNone(
                _wait_for_event("retail.public.order_items", "order_item_id", pk, "c"),
                "order_items INSERT missing",
            )
            with self.conn.cursor() as cur:
                cur.execute(
                    "UPDATE order_items SET quantity=%s WHERE order_item_id=%s",
                    (3, pk),
                )
                self.conn.commit()
            self.assertIsNotNone(
                _wait_for_event("retail.public.order_items", "order_item_id", pk, "u"),
                "order_items UPDATE missing",
            )
            _cleanup(self.conn, "order_items", "order_item_id", pk)
            self.assertIsNotNone(
                _consume("retail.public.order_items", "order_item_id", pk, "d",
                         from_beginning=True),
                "order_items DELETE missing",
            )
        except AssertionError:
            _cleanup(self.conn, "order_items", "order_item_id", pk)
            raise
        finally:
            _cleanup(self.conn, "orders", "order_id", order_id,
                     extra_deletes=[
                         ("DELETE FROM payments WHERE order_id=%s", (order_id,)),
                         ("DELETE FROM order_items WHERE order_id=%s", (order_id,)),
                     ])
            _cleanup(self.conn, "products", "product_id", product_id,
                     extra_deletes=[
                         ("DELETE FROM order_items WHERE product_id=%s", (product_id,)),
                     ])

    # ---- payments (FK to orders) ----
    def test_payments_cud(self):
        with self.conn.cursor() as cur:
            cur.execute(
                "INSERT INTO orders(customer_id,status,total_amount) "
                "VALUES(%s,'CREATED',%s) RETURNING order_id", (1, 10.00))
            order_id = cur.fetchone()[0]
            self.conn.commit()
        try:
            with self.conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO payments(order_id,amount,payment_method,status) "
                    "VALUES(%s,%s,%s,%s) RETURNING payment_id",
                    (order_id, 10.00, "CARD", "SUCCESS"),
                )
                pk = cur.fetchone()[0]
                self.conn.commit()
            self.assertIsNotNone(
                _wait_for_event("retail.public.payments", "payment_id", pk, "c"),
                "payments INSERT missing",
            )
            with self.conn.cursor() as cur:
                cur.execute(
                    "UPDATE payments SET status='REFUNDED' WHERE payment_id=%s",
                    (pk,),
                )
                self.conn.commit()
            self.assertIsNotNone(
                _wait_for_event("retail.public.payments", "payment_id", pk, "u"),
                "payments UPDATE missing",
            )
            _cleanup(self.conn, "payments", "payment_id", pk)
            self.assertIsNotNone(
                _consume("retail.public.payments", "payment_id", pk, "d",
                         from_beginning=True),
                "payments DELETE missing",
            )
        except AssertionError:
            _cleanup(self.conn, "payments", "payment_id", pk)
            raise
        finally:
            _cleanup(self.conn, "orders", "order_id", order_id,
                     extra_deletes=[
                         ("DELETE FROM payments WHERE order_id=%s", (order_id,)),
                         ("DELETE FROM order_items WHERE order_id=%s", (order_id,)),
                     ])


if __name__ == "__main__":
    unittest.main()