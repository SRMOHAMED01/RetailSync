# Production Runbook — RetailSync

## Deploy order

1. **Infrastructure** (Terraform)
   ```bash
   cd terraform
   terraform init
   terraform apply -auto-approve
   ```
   This provisions Kafka topics and Databricks warehouse/cluster.

2. **Local stack** (docker-compose)
   ```bash
   docker compose up -d postgres kafka connect
   bash scripts/register_connector.sh
   docker compose --profile simulator up -d simulator
   ```

3. **Bronze streams** (Databricks notebooks)
   Import each `databricks/bronze/<entity>_stream.py` notebook and run.
   Set `KAFKA_BOOTSTRAP_SERVERS` to a broker reachable from Databricks compute.

4. **Silver streams** (Databricks notebooks)
   Import each `databricks/silver/<entity>_merge.py` notebook and run.

5. **dbt Gold** (dbt)
   ```bash
   cp dbt/profiles.yml.template ~/.dbt/profiles.yml
   # fill in Databricks credentials
   dbt deps
   dbt build --select retailsync+
   ```

6. **Gold SQL views** (Databricks SQL)
   Run `databricks/gold/gold_views.sql` in the Databricks SQL editor.

7. **Power BI**
   Connect to the Databricks SQL Warehouse and import `powerbi/report.json`.

## Monitoring

```bash
python scripts/monitor.py
```
Checks: connector state, CDC topic presence, message counts, checkpoint age,
PostgreSQL vs Bronze/Silver reconciliation. JSON output for alerting.

## Failure modes

| Symptom | Likely cause | Fix |
| --- | --- | --- |
| Connector `PAUSED` | Kafka Connect down | `docker compose restart connect` |
| No CDC events | Debezium WAL not streaming | Re-register connector; check `pg_replication_slots` |
| Bronze stream stalled | Broker unreachable from Databricks | Update `KAFKA_BOOTSTRAP_SERVERS` |
| Silver lagging | foreachBatch backpressure | Scale cluster; increase trigger interval |
| dbt build fails | Schema drift in Silver | Inspect `cdc_entities.json` types; re-merge |

## Rollback

- Bronze/Silver are append-only Delta — no rollback needed, just stop streams.
- dbt models are recreated on each `dbt build` — rerun after fix.
- Power BI refreshes from Gold views; no data loss.

## On-call checklist

- [ ] `python scripts/monitor.py` exits 0
- [ ] All 5 CDC topics non-empty
- [ ] Connector state = RUNNING, tasks >= 1
- [ ] Checkpoint age < 30 min
- [ ] `dbt test --select retailsync+` passes