# Databricks notebook — Silver MERGE for `customers`.
#
# Parses the Debezium envelope from `retailsync.bronze.customers_cdc.payload`
# and idempotently MERGEs the current state into `retailsync.silver.customers`
# on the natural key `customer_id`. Handles INSERT / UPDATE / DELETE.
#
# Silver is a reliable current-state view (SCD-like). Re-processing the same
# event is a no-op because the MERGE keys on the natural key.
#
# See databricks/silver/cdc_silver.py for the shared implementation.
import sys
sys.path.append("/Volumes/retailsync/checkpoints")
sys.path.append("dbfs:/databricks/common")  # adjust to your import path

from cdc_silver import start_silver_stream

# customers has 7 columns: customer_id, first_name, last_name, email, city,
# created_at, updated_at. The MERGE uses customer_id as the natural key.
q = start_silver_stream(spark, "customers")
print(f"Started silver MERGE for customers -> retailsync.silver.customers")
print(f"Checkpoint: /Volumes/retailsync/checkpoints/silver_customers")
print(q.status)