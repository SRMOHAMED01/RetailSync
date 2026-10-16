# Databricks notebook — Silver MERGE for `payments`.
# Idempotent MERGE on `payment_id` (INSERT / UPDATE / DELETE).
import sys
sys.path.append("/Volumes/retailsync/checkpoints")
sys.path.append("dbfs:/databricks/common")  # adjust to your import path

from cdc_silver import start_silver_stream

q = start_silver_stream(spark, "payments")
print(f"Started silver MERGE for payments -> {q.status}")