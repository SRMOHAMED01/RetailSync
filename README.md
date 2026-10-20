# RetailSync — Cloud CDC & Real-Time Analytics Platform

A portfolio-grade data engineering project: **PostgreSQL → Debezium/Kafka Connect → Kafka → Databricks/PySpark → Delta Lake (Bronze/Silver/Gold) → dbt → Power BI**.

It streams retail events (customers, products, orders, order_items, payments) from an OLTP database through a Change-Data-Capture pipeline into an analytics warehouse, then exposes dashboards through Power BI.

## Architecture

```
Retail App / Simulator
        |
        v
PostgreSQL (OLTP) — wal_level=logical, replication slots
        | WAL / logical replication
        v
Debezium PostgreSQL Connector (Kafka Connect)
        |
        v
Apache Kafka topics (retail.public.*)
        |
        v
Databricks / Spark Structured Streaming
        |
        +--> Bronze: raw CDC events + Kafka metadata (append-only Delta)
        |
        +--> Silver: parsed, validated, MERGE'd current state (idempotent)
        |
        +--> Gold: analytics marts (star schema via dbt)
        |
        v
dbt -> Databricks SQL Warehouse -> Power BI
```

Transverse: data quality (DLT expectations), observability (monitoring), infrastructure as code (Terraform), CI (GitHub Actions).

## Data domains

| Entity | Natural key | Description |
| --- | --- | --- |
| customers | customer_id | Customer profile (SCD Type 1) |
| products | product_id | Catalog + stock level |
| orders | order_id | Order lifecycle (CREATED → PAID → SHIPPED → DELIVERED) |
| order_items | order_item_id | Line items linking orders to products |
| payments | payment_id | Payments against orders |

## Layers

- **Bronze** (`databricks/bronze/`, `databricks/common/cdc_bronze.py`) — immutable, replayable. Keeps the raw Debezium payload plus topic, partition, offset, timestamp.
- **Silver** (`databricks/silver/`, `databricks/common/cdc_silver.py`) — reliable current state. Idempotent MERGE on the natural key handling INSERT / UPDATE / DELETE.
- **Gold** (`dbt/models/`, `databricks/gold/gold_views.sql`) — star schema marts consumed by Power BI.
- **Quality** (`databricks/quality/dlt_retailsync.py`) — DLT expectations on Bronze, Silver, and Gold.

## Quick start

Requires Docker. The local stack is fully executable; the Databricks/dbt/Power BI files are starters you adapt to your cloud credentials.

```bash
# 1. Copy env and start the core stack
cp .env.example .env
docker compose up -d postgres kafka connect

# 2. Register the Debezium connector
bash scripts/register_connector.sh

# 3. Start the data simulator
docker compose --profile simulator up -d simulator

# 4. Observe CDC events in Kafka
docker exec -it retailsync-kafka kafka-console-consumer \
  --bootstrap-server kafka:9092 --topic retail.public.orders --from-beginning
```

See `docs/runbooks/PRODUCTION.md` for the full deploy order and failure modes.

## Tests

| Suite | Command | What it checks |
| --- | --- | --- |
| Unit | `python -m pytest tests/unit -q` | CDC envelope parsing, Silver MERGE contract, dbt model graph |
| Integration | `python -m pytest tests/integration -q` | Postgres → Debezium → Kafka |
| E2E | `python -m pytest tests/e2e -q` | Definition of Done: topics, connector, C/U/D |
| CI | `.github/workflows/tests.yml` | ruff lint + unit + integration + e2e |

## Project status

- **Unit tests**: 27 passing
- **Integration / E2E**: require Docker — run locally or in CI
- **dbt**: models materialize against Databricks; run `dbt build --select retailsync+`

## Tech stack

PostgreSQL 16 · Debezium 3.0 · Apache Kafka 3.7 (KRaft) · Databricks / PySpark · Delta Lake · dbt · Terraform · Power BI · pytest + ruff