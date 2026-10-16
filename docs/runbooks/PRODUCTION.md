# RetailSync — Production Runbook

Operational procedures for running RetailSync in staging/production.

## 1. Startup checklist

- [ ] Kafka brokers healthy, all `retail.public.*` topics exist (see `kafka/topics.yml`)
- [ ] PostgreSQL logical replication enabled (`wal_level=logical`, publication `retail_publication`)
- [ ] Kafka Connect + Debezium connector `retailsync-postgres` registered and RUNNING
- [ ] Databricks SQL Warehouse running; Unity Catalog schemas `retailsync.bronze/silver/gold`
- [ ] Databricks notebooks deployed: 5 Bronze streams + 5 Silver MERGE jobs
- [ ] dbt `profiles.yml` configured; `dbt build` passes
- [ ] Power BI connected to Gold views (`retailsync.gold.*`)
- [ ] Secrets injected via vault/secret manager (not committed)
- [ ] TLS enabled on Kafka + PostgreSQL endpoints
- [ ] Monitoring probe scheduled (`scripts/monitor.py`)

## 2. Daily checks

Run the observability probe and confirm `ALL OK`:

```bash
pip install -r requirements-test.txt
python scripts/monitor.py
```

Alert if:
- Debezium connector state != RUNNING
- Any `retail.public.*` topic missing or stagnant (no new messages while simulator/source writes)
- Kafka consumer lag growing on Silver consumer groups
- dbt `dbt build` failure on the latest run

## 3. Incident: connector stopped

1. Check Connect logs: `docker logs retailsync-connect`
2. Verify PostgreSQL logical slot `retailsync_slot` is active and not exceeding `max_slot_wal_keep_size`
3. Restart connector:

```bash
bash scripts/register_connector.sh
```

or via REST:

```bash
curl -X POST http://connect:8083/connectors/retailsync-postgres/restart
```

4. Re-run `scripts/monitor.py` to confirm recovery.

## 4. Incident: consumer lag / replay needed

Silver MERGE is idempotent; replaying Bronze into Silver is safe.

- Reset a Silver checkpoint:

```bash
# from Databricks, remove checkpoint then restart the Silver notebook stream
```

- Replay Bronze from Kafka beginning: re-run the Bronze notebook(s) with
  `startingOffsets = "earliest"` on a fresh checkpoint.

## 5. dbt run

```bash
cd dbt
dbt debug
dbt build
dbt docs generate
```

Failures are usually stale Silver tables — confirm Silver MERGE jobs are current.

## 6. Scaling

- Increase `kafka_partitions_orders` / `kafka_partitions_default` in Terraform,
  then re-create topics (Kafka partitions cannot be reduced; increase only).
- Increase Spark structured-streaming shuffle partitions to match Kafka partition count.
- Size the Databricks SQL Warehouse for Power BI concurrency.

## 7. Rollback / disaster recovery

- Bronze is append-only Delta — restore from backup if corrupted.
- Kafka topics are the source of truth; rebuild Silver/Bronze by replaying from Kafka.
- dbt models are views/tables over Silver — re-run `dbt build` to regenerate.
- Power BI reports reference Gold views; rebuild views from `databricks/gold/gold_views.sql`.

## 8. Security operations
- Rotate Databricks token and Kafka credentials every 90 days.
- Review Databricks Unity Catalog ACLs quarterly.
- Keep `.gitignore` excluding `.env` and `profiles.yml`; add new secret files.