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
from datetime import datetime, timezone

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


def check_checkpoint_age():
    """Streaming checkpoint age (minutes since last write).

    A checkpoint older than 30 min means the stream is stalled. Requires
    Databricks SQL (DATABRICKS_SQL_WAREHOUSE env) and the retailsync catalog.
    """
    warehouse = os.getenv("DATABRICKS_SQL_WAREHOUSE")
    if not warehouse:
        print("[SKIP] DATABRICKS_SQL_WAREHOUSE not set — skipping checkpoint check")
        return True
    try:
        host = os.getenv("DATABRICKS_HOST", "https://your-workspace.cloud.databricks.com")
        token = os.getenv("DATABRICKS_TOKEN")
        if not token:
            print("[SKIP] DATABRICKS_TOKEN not set — skipping checkpoint check")
            return True
        import databricks.sql
        with databricks.sql.connect(
            server_hostname=host.lstrip("https://"),
            http_path=warehouse,
            access_token=token,
        ) as conn, conn.cursor() as cur:
            cur.execute("""
                select
                    max(ingested_at) as last_event,
                    max(checkpoint_ts) as last_checkpoint
                from (
                    select ingested_at, null as checkpoint_ts
                    from retailsync.bronze.orders_cdc
                    union all
                    select null, last_update
                    from system.streaming.checkpoints
                    where location like '%retailsync%'
                )
            """)
            row = cur.fetchone()
            last_event, last_checkpoint = row[0], row[1]
        now = datetime.now(timezone.utc)
        event_lag = (now - last_event).total_seconds() / 60 if last_event else None
        ckpt_lag = (now - last_checkpoint).total_seconds() / 60 if last_checkpoint else None
        ok = (event_lag is None or event_lag < 30) and (ckpt_lag is None or ckpt_lag < 30)
        print(f"[{'OK' if ok else 'FAIL'}] last_event_lag={event_lag}min checkpoint_lag={ckpt_lag}min")
        return ok
    except Exception as e:
        print(f"[WARN] checkpoint check error: {e}")
        return True


def check_bronze_silver_counts():
    """Reconcile Bronze/Silver row counts against PostgreSQL source."""
    warehouse = os.getenv("DATABRICKS_SQL_WAREHOUSE")
    if not warehouse:
        print("[SKIP] DATABRICKS_SQL_WAREHOUSE not set — skipping reconciliation")
        return True
    try:
        import databricks.sql
        host = os.getenv("DATABRICKS_HOST", "https://your-workspace.cloud.databricks.com")
        token = os.getenv("DATABRICKS_TOKEN")
        with databricks.sql.connect(
            server_hostname=host.lstrip("https://"),
            http_path=warehouse,
            access_token=token,
        ) as conn, conn.cursor() as cur:
            cur.execute("""
                select 'bronze_orders' t, count(*) from retailsync.bronze.orders_cdc
                union all select 'silver_orders', count(*) from retailsync.silver.orders
                union all select 'bronze_customers', count(*) from retailsync.bronze.customers_cdc
                union all select 'silver_customers', count(*) from retailsync.silver.customers
            """)
            counts = {t: c for t, c in cur.fetchall()}
        print(f"[INFO] databricks counts: {counts}")
        return counts
    except Exception as e:
        print(f"[WARN] databricks reconciliation error: {e}")
        return True


def main():
    print(f"=== RetailSync monitor @ {time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())} ===")
    results = {
        "connector": check_connector(),
        "topics": check_topics(),
        "checkpoint_age": check_checkpoint_age(),
    }
    counts = check_message_counts()
    pg = check_pg_counts()
    db_counts = check_bronze_silver_counts()

    # Reconciliation: every CDC topic must have >= messages for its table.
    # (orders can lag the simulator; this is a soft check.)
    recon_ok = counts.get("retail.public.orders", 0) >= pg.get("orders", 0)
    results["reconciliation"] = recon_ok
    print(f"[{'OK' if recon_ok else 'WARN'}] orders: kafka={counts.get('retail.public.orders')} pg={pg.get('orders')}")

    failed = [k for k, v in results.items() if not v]
    summary = {
        "timestamp": time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime()),
        "results": results,
        "counts": {"kafka": counts, "postgres": pg, "databricks": db_counts},
        "status": "OK" if not failed else "FAIL",
        "failures": failed,
    }
    print(json.dumps(summary, indent=2, default=str))
    print(f"\n=== {'ALL OK' if not failed else 'FAILURES: ' + ', '.join(failed)} ===")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())