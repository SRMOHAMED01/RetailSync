# Databricks notebook — Silver MERGE for `payments`.
#
# Parses the Debezium envelope from `retailsync.bronze.payments_cdc.payload`
# and idempotently MERGEs the current state into `retailsync.silver.payments`
# on the natural key `payment_id`. Handles INSERT / UPDATE / DELETE.
#
# Silver is a reliable current-state view (SCD-like). Re-processing the same
# event is a no-op because the MERGE keys on the natural key.
#
# See databricks/silver/cdc_silver.py for the shared implementation.
import sys
sys.path.append("/Volumes/retailsync/checkpoints")
sys.path.append("dbfs:/databricks/common")  # adjust to your import path

from cdc_silver import start_silver_stream

# payments has 6 columns: payment_id, order_id, amount, payment_method, status,
# created_at. The MERGE uses payment_id as the natural key.
q = start_silver_stream(spark, "payments")
print(f"Started silver MERGE for payments -> retailsync.silver.payments")
print(f"Checkpoint: /Volumes/retailsync/checkpoints/silver_payments")
print(q.status)