# RetailSync — Guide de réalisation A à Z

## Sprint 1 — PostgreSQL
- `cp .env.example .env`
- `docker compose up -d postgres`
- Vérifier: `docker exec -it retailsync-postgres psql -U retail -d retailsync -c "select * from products;"`

## Sprint 2 — Simulateur
- `docker compose --profile simulator up -d simulator`
- `docker logs -f retailsync-simulator`

## Sprint 3 — Kafka
- `docker compose up -d kafka`
- Comprendre topic/key/partition/offset/retention avant de continuer.

## Sprint 4 — Debezium
- `docker compose up -d connect`
- `bash scripts/register_connector.sh`
- Faire INSERT/UPDATE/DELETE dans PostgreSQL et lire `retail.public.orders` avec le console consumer Kafka.

## Sprint 5/6 — Databricks + Bronze
- Lire `docs/runbooks/DATABRICKS.md`.
- Adapter et exécuter `databricks/bronze/orders_stream.py`.
- Bronze doit conserver le payload brut et metadata Kafka pour replay/audit.

## Sprint 7 — Silver
- Exécuter/adopter `databricks/silver/orders_merge.py`.
- Tester CREATE/UPDATE/DELETE, redémarrage et doublons.
- Étendre le pattern aux autres entités.

## Sprint 8 — dbt/Gold
- Installer/configurer dbt-databricks dans un environnement Python séparé.
- `dbt debug && dbt build`.
- Ajouter dimensions (`dim_customer`, `dim_product`, `dim_date`) et facts (`fact_sales`, `fact_orders`).

## Sprint 9 — Power BI
- Créer SQL Warehouse Databricks.
- Connecter Power BI aux marts Gold.
- Dashboard: CA, commandes, panier moyen, évolution, top produits, clients.

## Sprint 10 — Production-like
- Secrets hors Git; TLS/IAM/RBAC selon environnement.
- Tests unitaires/intégration/E2E.
- Monitoring: connector status, Kafka consumer lag, Spark input/processing rate, failed records, dbt tests/freshness.
- CI: lint/test/dbt build; IaC/Terraform en bonus.

## Definition of Done
Une transaction PostgreSQL doit produire un événement Debezium, être lisible dans Kafka, persister en Bronze, être réconciliée en Silver, alimenter un mart dbt et apparaître dans Power BI. Tester aussi UPDATE et DELETE.

## Important
Le compose est un environnement de développement. Un Databricks cloud ne peut pas joindre automatiquement `localhost:9092`; utilisez un Kafka accessible depuis le cloud ou une connectivité réseau appropriée.
