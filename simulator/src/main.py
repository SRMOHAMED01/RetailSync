#!/usr/bin/env python3
"""RetailSync data simulator.

Continuously inserts/updates retail data into PostgreSQL so the CDC pipeline
(Postgres -> Debezium -> Kafka -> Databricks) has a live stream to process.

Every loop iteration:
  1. Picks (or creates) a customer.
  2. Picks an in-stock product.
  3. Creates an order + order_item line, decrements stock.
  4. ~20% of the time, marks the order PAID and writes a payment.

Run: `docker compose --profile simulator up -d simulator`
Env:  DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD, EVENT_INTERVAL_SECONDS
"""
import os
import random
import time
from decimal import Decimal

import psycopg
from faker import Faker

fake = Faker()

DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "retailsync")
DB_USER = os.getenv("DB_USER", "retail")
DB_PASSWORD = os.getenv("DB_PASSWORD", "retail_dev_password")
INTERVAL = float(os.getenv("EVENT_INTERVAL_SECONDS", "2"))

PAYMENT_METHODS = ("CARD", "CASH", "WALLET")


def connect():
    return psycopg.connect(
        host=DB_HOST, port=DB_PORT, dbname=DB_NAME,
        user=DB_USER, password=DB_PASSWORD,
    )


def pick_or_create_customer(cur):
    cur.execute("SELECT customer_id FROM customers ORDER BY random() LIMIT 1")
    row = cur.fetchone()
    if row:
        return row[0]
    cur.execute(
        "INSERT INTO customers(first_name, last_name, email, city) "
        "VALUES(%s, %s, %s, %s) RETURNING customer_id",
        (fake.first_name(), fake.last_name(), fake.unique.email(), fake.city()),
    )
    return cur.fetchone()[0]


def pick_in_stock_product(cur):
    cur.execute(
        "SELECT product_id, price FROM products "
        "WHERE stock > 0 ORDER BY random() LIMIT 1"
    )
    return cur.fetchone()


def generate_event(conn):
    with conn.cursor() as cur:
        customer_id = pick_or_create_customer(cur)
        product = pick_in_stock_product(cur)
        if not product:
            return  # nothing to sell right now

        product_id, unit_price = product
        quantity = random.randint(1, 3)
        line_total = Decimal(unit_price) * quantity

        cur.execute(
            "INSERT INTO orders(customer_id, status, total_amount) "
            "VALUES(%s, 'CREATED', %s) RETURNING order_id",
            (customer_id, line_total),
        )
        order_id = cur.fetchone()[0]

        cur.execute(
            "INSERT INTO order_items(order_id, product_id, quantity, unit_price) "
            "VALUES(%s, %s, %s, %s)",
            (order_id, product_id, quantity, unit_price),
        )

        cur.execute(
            "UPDATE products SET stock = stock - %s, updated_at = now() "
            "WHERE product_id = %s",
            (quantity, product_id),
        )

        if random.random() > 0.2:
            cur.execute(
                "UPDATE orders SET status = 'PAID', updated_at = now() "
                "WHERE order_id = %s",
                (order_id,),
            )
            cur.execute(
                "INSERT INTO payments(order_id, amount, payment_method, status) "
                "VALUES(%s, %s, %s, 'SUCCESS')",
                (order_id, line_total, random.choice(PAYMENT_METHODS)),
            )

        print(f"order={order_id} customer={customer_id} "
              f"product={product_id} qty={quantity} total={line_total}", flush=True)


def main():
    print(f"RetailSync simulator starting (interval={INTERVAL}s)", flush=True)
    while True:
        try:
            with connect() as conn:
                generate_event(conn)
            time.sleep(INTERVAL)
        except Exception as exc:  # pragma: no cover - resilience
            print("simulator error:", exc, flush=True)
            time.sleep(5)


if __name__ == "__main__":
    main()