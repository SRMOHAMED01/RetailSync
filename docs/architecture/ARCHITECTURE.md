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
- Silver: état métier fiable; appliquer INSERT/UPDATE/DELETE via clé métier et MERGE idempotent.
- Gold: tables orientées usages BI, idéalement star schema.

## Topics attendus
`retail.public.customers`, `retail.public.products`, `retail.public.orders`, `retail.public.order_items`, `retail.public.payments`.
