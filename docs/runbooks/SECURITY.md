# RetailSync — Production Security Checklist

This checklist covers secrets management, transport security, and access
control for deploying RetailSync outside the local dev stack.

## 1. Secrets management

Never commit secrets to Git. The `.gitignore` already excludes `.env`, but the
following must also stay out of version control:

| Secret | Where it lives | Consumer |
|---|---|---|
| `POSTGRES_PASSWORD` | Docker secret / env file | postgres, simulator, Debezium connector |
| `DB_PASSWORD` | same | same |
| Kafka SASL/SSL creds | same | Kafka clients, Connect |
| Databricks token | `~/.dbt/profiles.yml` (templated) | dbt, Power BI |
| Power BI service principal | Key Vault / secret store | Power BI gateway |

### Recommended approach per environment
- **Dev**: `.env` file excluded by `.gitignore` (current setup).
- **Staging/Prod**: Docker secrets, Kubernetes secrets, or a vault (HashiCorp
  Vault / Azure Key Vault / AWS Secrets Manager). Inject at runtime, never bake
  into images.
- **CI**: store as GitHub Actions encrypted secrets; pass to the job via env
  vars. The `.github/workflows/tests.yml` job already uses
  `actions/setup-python` and installs deps without secrets.

### dbt profiles
`dbt/profiles.yml.template` is committed; the real `profiles.yml` must be
generated at deploy time and contain the Databricks host, http_path, and a
token sourced from an env var (`DBT_DATABRICKS_TOKEN`). Never commit the real
file.

## 2. Transport security (TLS)

The local compose uses `PLAINTEXT` listeners for simplicity. For any
environment reachable over a network:

- **Kafka**: enable `SSL` listeners + SASL authentication. The current
  `docker-compose.yml` exposes `EXTERNAL://:9092` on PLAINTEXT — switch to TLS
  for staging/prod.
- **PostgreSQL**: require TLS (`ssl = on`, `ssl_cert_file`/`ssl_key_file`).
- **Kafka Connect**: TLS to Kafka + HTTPS on the REST port (8083).
- **Databricks**: always HTTPS (enforced by the workspace).

## 3. IAM / RBAC

- **PostgreSQL**: least-privilege DB user. The `retail` user currently has
  DML + replication privileges needed by Debezium; do not grant `SUPERUSER`
  in production. The replication slot (`retailsync_slot`) and publication are
  created by the connector.
- **Kafka**: ACLs per topic (`retail.public.*`, `retailsync_connect_*`) and per
  consumer group (`retailsync-connect`, Spark consumer groups). Restrict
  `Cluster Create` / `Topic Create` so auto-creation cannot sprawl.
- **Databricks**: Unity Catalog with row/column-level security on
  `retailsync.gold.*` and `retailsync.silver.*`. The SQL Warehouse used by dbt
  and Power BI should run under a service principal, not a personal token.
- **Power BI**: row-level security on `dim_customer` if multi-tenant.

## 4. Audit & data retention

- Bronze is append-only Delta — immutable by design. Retention is governed by
  the Delta log; do not `VACUUM` aggressively on Bronze.
- Kafka retention per topic should be >= the Bronze replay window (e.g. 7
  days). Configure in `kafka/` topic configs.

## 5. Compliance notes
- Debezium snapshots and CDC payloads may contain PII (emails, cities). Apply
  masking at the Silver/Gold layer or restrict column access via RBAC.
- Do not store card data — the schema only keeps `payment_method` and amounts.