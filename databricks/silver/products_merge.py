# Databricks notebook — Silver MERGE for `products`.
#
# Parses the Debezium envelope from `retailsync.bronze.products_cdc.payload`
# and idempotently MERGEs the current state into `retailsync.silver.products`
# on the natural key `product_id`. Handles INSERT / UPDATE / DELETE.
#
# Silver is a reliable current-state view (SCD-like). Re-processing the same
# event is a no-op because the MERGE keys on the natural key.
#
# See databricks/silver/cdc_silver.py for the shared implementation.
import sys
sys.path.append("/Volumes/retailsync/checkpoints")
sys.path.append("dbfs:/databricks/common")  # adjust to your import path

from cdc_silver import start_silver_stream

# products has 6 columns: product_id, name, category, price, stock, updated_at.
# The MERGE uses product_id as the natural key.
q = start_silver_stream(spark, "products")
print(f"Started silver MERGE for products -> retailsync.silver.products")
print(f"Checkpoint: /Volumes/retailsync/checkpoints/silver_products")
print(q.status)