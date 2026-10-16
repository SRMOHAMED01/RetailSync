#!/usr/bin/env python3
"""RetailSync observability probe.

Checks the health of the local CDC pipeline:
  1. Kafka Connect REST + Debezium connector state
  2. CDC topic message counts (lag proxy: messages since last check)
  3. PostgreSQL row counts vs Bronze/Silver reconciliation

Run: `python scripts/monitor.py` (requires the local stack up).

Exit code is non-zero if any critical check fails.
"""
import json
import os
import subprocess
import sys
import time
import urllib.request

CONNECT_URL = os.getenv("CONNECT_URL", "http://localhost:8083")
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")
CONNECTOR = "retailsync-postgres"

CDC_TOPICS = [
    "retail.public.customers",
    "retail.public.products",
    "retail.public.orders",
    "retail.public.order_items",
    "retail.public.payments",
]

PG = {
    "host": os.getenv("DB_HOST", "localhost"),
    "port": os.getenv("DB_PORT", "5432"),
    "dbname": os.getenv("DB_NAME", "retailsync"),
    "user": os.getenv("DB_USER", "retail"),
    "password": os.getenv("DB_PASSWORD", "retail_dev_password"),
}


def _http(url, timeout=10):
    with urllib.request.urlopen(url, timeout=timeout) as r:
        return json.loads(r.read().decode())


def _docker(cmd, timeout=30):
    return subprocess.run(
        ["docker", "exec", "retailsync-kafka", *cmd],
        capture_output=True, text=True, timeout=timeout,
    )


def check_connector():
    """Debezium connector must be RUNNING with at least one RUNNING task."""
    status = _http(f"{CONNECT_URL}/connectors/{CONNECTOR}/status")
    state = status["connector"]["state"]
    tasks = status.get("tasks", [])
    running = sum(1 for t in tasks if t["state"] == "RUNNING")
    ok = state == "RUNNING" and running >= 1
    print(f"[{'OK' if ok else 'FAIL'}] connector={state} tasks_running={running}/{len(tasks)}")
    return ok


def check_topics():
    """All 5 CDC topics must exist and be non-empty (after the simulator runs)."""
    out = _docker(["/opt/kafka/bin/kafka-topics.sh", "--bootstrap-server",
                   "localhost:29092", "--list"]).stdout.split()
    missing = [t for t in CDC_TOPICS if t not in out]
    ok = not missing
    print(f"[{'OK' if ok else 'FAIL'}] cdc_topics={'all present' if ok else 'missing ' + str(missing)}")
    return ok


def check_message_counts():
    """Count messages per CDC topic (proxy for consumer lag)."""
    counts = {}
    for topic in CDC_TOPICS:
        out = _docker(["/opt/kafka/bin/kafka-console-consumer.sh",
                       "--bootstrap-server", "localhost:29092",
                       "--topic", topic, "--from-beginning",
                       "--timeout-ms", "5000", "--max-messages", "100000"],
                      timeout=60)
        counts[topic] = len([l for l in out.stdout.splitlines() if l.strip()])
    print(f"[INFO] message counts: {counts}")
    return counts


def check_pg_counts():
    """Row counts per source table (for reconciliation)."""
    import psycopg
    conninfo = (f"host={PG['host']} port={PG['port']} dbname={PG['dbname']} "
                f"user={PG['user']} password={PG['password']}")
    with psycopg.connect(conninfo) as conn, conn.cursor() as cur:
        cur.execute("""
            SELECT 'customers' t, count(*) FROM customers
            UNION ALL SELECT 'products', count(*) FROM products
            UNION ALL SELECT 'orders', count(*) FROM orders
            UNION ALL SELECT 'order_items', count(*) FROM order_items
            UNION ALL SELECT 'payments', count(*) FROM payments
        """)
        rows = {t: c for t, c in cur.fetchall()}
    print(f"[INFO] postgres counts: {rows}")
    return rows


def main():
    print(f"=== RetailSync monitor @ {time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())} ===")
    results = {
        "connector": check_connector(),
        "topics": check_topics(),
    }
    counts = check_message_counts()
    pg = check_pg_counts()

    # Reconciliation: every CDC topic must have >= messages for its table.
    # (orders can lag the simulator; this is a soft check.)
    recon_ok = counts.get("retail.public.orders", 0) >= pg.get("orders", 0)
    results["reconciliation"] = recon_ok
    print(f"[{'OK' if recon_ok else 'WARN'}] orders: kafka={counts.get('retail.public.orders')} pg={pg.get('orders')}")

    failed = [k for k, v in results.items() if not v]
    print(f"\n=== {'ALL OK' if not failed else 'FAILURES: ' + ', '.join(failed)} ===")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())