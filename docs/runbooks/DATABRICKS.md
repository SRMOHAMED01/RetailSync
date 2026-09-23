# Mini-guide Databricks
1. Créer un workspace/compute Databricks.
2. Rendre Kafka joignable depuis Databricks (un Kafka `localhost` sur le PC n'est pas accessible directement depuis un workspace cloud). Pour une démo cloud, utiliser un broker accessible/managed ou un réseau approprié.
3. Définir `KAFKA_BOOTSTRAP_SERVERS`, le catalogue/schema Delta et les chemins de checkpoint.
4. Exécuter `databricks/bronze/orders_stream.py` puis `databricks/silver/orders_merge.py`.
5. Vérifier les tables Bronze/Silver et les offsets/checkpoints.
6. Répéter le pattern pour customers/products/order_items/payments.
7. Créer un SQL Warehouse pour dbt et Power BI.

Ne jamais committer credentials/tokens dans Git.
