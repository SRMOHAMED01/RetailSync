# Databricks notebook — Silver MERGE for `orders`.
#
# Parses the Debezium envelope from `retailsync.bronze.orders_cdc.payload` and
# idempotently MERGEs the current state into `retailsync.silver.orders` on the
# natural key `order_id`. Handles INSERT / UPDATE / DELETE.
#
# See databricks/silver/cdc_silver.py for the shared implementation.
import sys
sys.path.append("/Volumes/retailsync/checkpoints")
sys.path.append("dbfs:/databricks/common")  # adjust to your import path

from cdc_silver import start_silver_stream

q = start_silver_stream(spark, "orders")
print(f"Started silver MERGE for orders -> {q.status}")