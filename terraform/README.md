# Terraform / IaC for RetailSync (starter)

This directory holds Terraform modules for provisioning the Kafka and
Databricks resources that the local compose stack emulates. It is a starter —
review, then apply per environment.

## Layout
- `main.tf` — provider configs + module wiring
- `kafka.tf` — Kafka topics (retention, partitions) via the Confluent or
  Aiven provider, or raw `kafka_topic` resources.
- `databricks.tf` — Databricks workspace, SQL Warehouse, Unity Catalog
  schemas (`retailsync.bronze`, `retailsync.silver`, `retailsync.gold`).
- `variables.tf` / `outputs.tf` — environment inputs and warehouse URL.

## Prerequisites
- Terraform >= 1.6
- Provider credentials (Confluent/Aiven API key, or Databricks token + host)
- `terraform init && terraform plan`

## Notes
- Secrets (tokens, passwords) must come from env vars or a secret manager —
  never hard-coded. See `docs/runbooks/SECURITY.md`.
- The local compose stack is the reference implementation; Terraform provisions
  the production equivalent.
- Topics are idempotent (`create_if_not_exists`), so re-running is safe.

## Example: `terraform.tfvars`
```
environment = "prod"
kafka_broker = "kafka.prod.retailsync.internal:9093"
kafka_security_protocol = "SASL_SSL"
databricks_host = "https://your-workspace.cloud.databricks.com"
databricks_warehouse_id = "<WAREHOUSE_ID>"
```