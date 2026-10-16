# Architecture RetailSync

```text
Retail App / Simulator
        |
        v
PostgreSQL (OLTP)
        | WAL / logical replication
        v
Debezium PostgreSQL Connector (Kafka Connect)
        |
        v
Apache Kafka topics
        |
        v
Databricks / Spark Structured Streaming
        |
        +--> Bronze: événements CDC bruts + metadata Kafka
        |
        +--> Silver: parsing, validation, déduplication, MERGE CDC
        |
        +--> Gold: modèles analytiques / marts
        |
        v
dbt -> Databricks SQL -> Power BI

Transverse: quality, lineage, security, checkpoints, observability.
```

## Contrats des couches
- Bronze: immutable/replayable; conserver topic, partition, offset, timestamp et payload.
  - `databricks/common/cdc_bronze.py` — shared helper (topics, tables, checkpoints).
  - One stream per entity: `databricks/bronze/<entity>_stream.py`.
- Silver: état métier fiable; appliquer INSERT/UPDATE/DELETE via clé métier et MERGE idempotent.
  - `databricks/common/cdc_silver.py` + `databricks/cdc_entities.json` — shared helper.
  - One MERGE per entity: `databricks/silver/<entity>_merge.py`.
- Gold: tables orientées usages BI, idéalement star schema.
  - dbt: `dbt/models/marts/{dim_customer,dim_product,dim_date,fact_sales,fact_orders}.sql`
  - Databricks SQL views: `databricks/gold/gold_views.sql` (for Power BI).

## Topics attendus
`retail.public.customers`, `retail.public.products`, `retail.public.orders`, `retail.public.order_items`, `retail.public.payments`.

## Tests
- `tests/unit` — contract tests (no Spark), `python -m pytest tests/unit -q`
- `tests/integration` — Postgres -> Debezium -> Kafka, `python -m pytest tests/integration -q`
- `tests/e2e` — Definition of Done infra checks (topics, connector, c/u/d), `python -m pytest tests/e2e -q`
- CI: `.github/workflows/tests.yml` (ruff lint + unit + integration + e2e)
