# RetailSync — Guide de réalisation A à Z

## Sprint 1 — PostgreSQL
- `cp .env.example .env`
- `docker compose up -d postgres`
- Vérifier: `docker exec retailsync-postgres psql -U retail -d retailsync -c "select * from products;"`

## Sprint 2 — Simulateur
- `docker compose --profile simulator up -d simulator`
- `docker logs -f retailsync-simulator`

## Sprint 3 — Kafka
- `docker compose up -d kafka`
- Comprendre topic/key/partition/offset/retention avant de continuer.
- Vérifier: `docker exec retailsync-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:29092 --list`

## Sprint 4 — Debezium
- `docker compose up -d connect`
- `bash scripts/register_connector.sh` (ou `curl -X POST http://localhost:8083/connectors --data @debezium/connectors/postgres-retail.json`)
- Faire INSERT/UPDATE/DELETE dans PostgreSQL et lire `retail.public.orders` avec le console consumer Kafka.
- Les 5 topics CDC sont créés automatiquement: `retail.public.customers`, `retail.public.products`, `retail.public.orders`, `retail.public.order_items`, `retail.public.payments`.

## Sprint 5/6 — Databricks + Bronze
- Lire `docs/runbooks/DATABRICKS.md`.
- Adapter et exécuter `databricks/bronze/{orders,customers,products,order_items,payments}_stream.py`.
- Bronze doit conserver le payload brut et metadata Kafka pour replay/audit.
- Helper partagé: `databricks/common/cdc_bronze.py` (topics, tables, checkpoints).

## Sprint 7 — Silver
- Exécuter/adopter `databricks/silver/{orders,customers,products,order_items,payments}_merge.py`.
- Tester CREATE/UPDATE/DELETE, redémarrage et doublons.
- Helper partagé: `databricks/common/cdc_silver.py` + `databricks/cdc_entities.json`.
- Corrigé: les timestamps Debezium sont des strings ISO-8601 (ZonedTimestamp), pas des Long.

## Sprint 8 — dbt/Gold
- Installer/configurer dbt-databricks dans un environnement Python séparé.
- `dbt debug && dbt build` (config: `dbt/profiles.yml.template` à copier en `~/.dbt/profiles.yml`).
- Modèles: `dim_customer`, `dim_product`, `dim_date`, `fact_sales`, `fact_orders` + `stg_orders`.
- Tests de schéma dans `dbt/models/schema.yml`.
- Vues Gold pour Power BI: `databricks/gold/gold_views.sql`.

## Sprint 9 — Power BI
- Créer SQL Warehouse Databricks.
- Connecter Power BI aux marts Gold.
- Dashboard: CA, commandes, panier moyen, évolution, top produits, clients.
- Spéc: `powerbi/REPORT_SPEC.md`.

## Sprint 10 — Production-like
- Secrets hors Git; TLS/IAM/RBAC selon environnement.
- Tests: `python -m pytest tests/{unit,integration,e2e} -q` (13 tests, tous passants localement).
- Monitoring: connector status, Kafka consumer lag, Spark input/processing rate, failed records, dbt tests/freshness.
- CI: `.github/workflows/tests.yml` (lint ruff + unit + integration + e2e).
- IaC/Terraform en bonus.

## Definition of Done
Une transaction PostgreSQL doit produire un événement Debezium, être lisible dans Kafka, persister en Bronze, être réconciliée en Silver, alimenter un mart dbt et apparaître dans Power BI. Tester aussi UPDATE et DELETE.
Les tests E2E valident localement l'étape infra (Postgres -> Debezium -> Kafka, op c/u/d).

## Important
Le compose est un environnement de développement. Un Databricks cloud ne peut pas joindre automatiquement `localhost:9092`; utilisez un Kafka accessible depuis le cloud ou une connectivité réseau appropriée.
