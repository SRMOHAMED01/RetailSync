# RetailSync — Cloud CDC & Real-Time Analytics Platform

RetailSync est un projet portfolio Data Engineering : PostgreSQL → Debezium/Kafka Connect → Kafka → Databricks/PySpark → Delta Lake Bronze/Silver/Gold → dbt → Power BI.

## Démarrage rapide
1. Copier `.env.example` vers `.env`.
2. Lancer `docker compose up -d`.
3. Attendre que PostgreSQL, Kafka et Kafka Connect soient prêts.
4. Exécuter `bash scripts/register_connector.sh` (Git Bash/WSL sous Windows).
5. Lancer le simulateur : `docker compose --profile simulator up -d simulator`.
6. Observer les événements : `docker exec -it retailsync-kafka kafka-console-consumer --bootstrap-server kafka:9092 --topic retail.public.orders --from-beginning`.
7. Importer/adopter les scripts `databricks/` dans Databricks et configurer les variables indiquées dans `docs/runbooks/DATABRICKS.md`.
8. Configurer dbt selon `dbt/README.md`, puis construire les modèles Gold.
9. Connecter Power BI au Databricks SQL Warehouse et créer le dashboard décrit dans `powerbi/README.md`.

## Tests
- Unitaires: `python -m pytest tests/unit -q` (7 tests, sans Spark)
- Intégration: `python -m pytest tests/integration -q` (Postgres -> Debezium -> Kafka)
- E2E: `python -m pytest tests/e2e -q` (DoD infra: topics, connector, INSERT/UPDATE/DELETE)
- CI: `.github/workflows/tests.yml` (lint ruff + unit + integration + e2e)

## Objectif pédagogique
- CDC PostgreSQL via WAL/logical replication
- Debezium et Kafka Connect
- Topics, partitions, offsets et replay Kafka
- Spark Structured Streaming + checkpointing
- Delta Lake, MERGE idempotent et architecture Medallion
- dbt : staging, marts, tests et documentation
- Star Schema et Power BI
- Tests E2E et observabilité

## Architecture
Voir `docs/architecture/ARCHITECTURE.md`.

## Plan recommandé
Sprint 1 PostgreSQL → 2 simulateur → 3 Kafka → 4 Debezium → 5 Databricks → 6 Bronze → 7 Silver → 8 dbt/Gold → 9 Power BI → 10 tests/monitoring/CI.

> Le socle local est exécutable. Les fichiers Databricks/dbt/Power BI sont des starters à adapter aux credentials et à l'espace cloud de l'utilisateur.
