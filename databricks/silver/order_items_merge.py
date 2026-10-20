# Databricks notebook — Silver MERGE for `order_items`.
#
# Parses the Debezium envelope from `retailsync.bronze.order_items_cdc.payload`
# and idempotently MERGEs the current state into `retailsync.silver.order_items`
# on the natural key `order_item_id`. Handles INSERT / UPDATE / DELETE.
#
# Silver is a reliable current-state view (SCD-like). Re-processing the same
# event is a no-op because the MERGE keys on the natural key.
#
# See databricks/silver/cdc_silver.py for the shared implementation.
import sys
sys.path.append("/Volumes/retailsync/checkpoints")
sys.path.append("dbfs:/databricks/common")  # adjust to your import path

from cdc_silver import start_silver_stream

# order_items has 5 columns: order_item_id, order_id, product_id, quantity,
# unit_price. The MERGE uses order_item_id as the natural key.
q = start_silver_stream(spark, "order_items")
print(f"Started silver MERGE for order_items -> retailsync.silver.order_items")
print(f"Checkpoint: /Volumes/retailsync/checkpoints/silver_order_items")
print(q.status)