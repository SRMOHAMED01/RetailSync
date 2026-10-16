# Databricks notebook — Silver MERGE for `order_items`.
# Idempotent MERGE on `order_item_id` (INSERT / UPDATE / DELETE).
import sys
sys.path.append("/Volumes/retailsync/checkpoints")
sys.path.append("dbfs:/databricks/common")  # adjust to your import path

from cdc_silver import start_silver_stream

q = start_silver_stream(spark, "order_items")
print(f"Started silver MERGE for order_items -> {q.status}")