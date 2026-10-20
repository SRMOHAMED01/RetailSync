# Databricks notebook — Silver MERGE for `orders`.
#
# Parses the Debezium envelope from `retailsync.bronze.orders_cdc.payload` and
# idempotently MERGEs the current state into `retailsync.silver.orders` on the
# natural key `order_id`. Handles INSERT / UPDATE / DELETE.
#
# Silver is a reliable current-state view (SCD-like). Re-processing the same
# event is a no-op because the MERGE keys on the natural key.
#
# The orders table has 6 columns: order_id, customer_id, status,
# total_amount, created_at, updated_at. The status field follows the lifecycle
# CREATED -> PAID -> SHIPPED -> DELIVERED (or CANCELLED).
#
# See databricks/silver/cdc_silver.py for the shared implementation.
import sys
sys.path.append("/Volumes/retailsync/checkpoints")
sys.path.append("dbfs:/databricks/common")  # adjust to your import path

from cdc_silver import start_silver_stream

q = start_silver_stream(spark, "orders")
print(f"Started silver MERGE for orders -> retailsync.silver.orders")
print(f"Checkpoint: /Volumes/retailsync/checkpoints/silver_orders")
print(q.status)