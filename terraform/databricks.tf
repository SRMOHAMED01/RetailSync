# Databricks workspace resources.
# - SQL Warehouse used by dbt + Power BI
# - Unity Catalog schemas: bronze / silver / gold

resource "databricks_sql_endpoint" "gold_warehouse" {
  name        = "retailsync-gold-${var.environment}"
  cluster_size = var.databricks_cluster_size  # e.g. Medium
  min_num_clusters = 1
  max_num_clusters = 2
  auto_stop_mins = 10
  channel = "PREVIEW"
  enable_photon = true
  spot_instance_policy = "COST_OPTIMIZED"
}

resource "databricks_catalog" "retailsync" {
  name = var.catalog_name
}

resource "databricks_schema" "bronze" {
  name         = "bronze"
  catalog_name = databricks_catalog.retailsync.id
  comment      = "Bronze layer: raw CDC events + Kafka metadata"
}

resource "databricks_schema" "silver" {
  name         = "silver"
  catalog_name = databricks_catalog.retailsync.id
  comment      = "Silver layer: idempotent CDC MERGE current state"
}

resource "databricks_schema" "gold" {
  name         = "gold"
  catalog_name = databricks_catalog.retailsync.id
  comment      = "Gold layer: star schema marts for Power BI"
}

output "warehouse_id" {
  value = databricks_sql_endpoint.gold_warehouse.id
}

output "warehouse_http_path" {
  value = databricks_sql_endpoint.gold_warehouse.http_path
}

output "gold_catalog" {
  value = databricks_catalog.retailsync.name
}