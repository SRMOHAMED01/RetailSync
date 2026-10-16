# Databricks notebook — Bronze streaming for `products`.
# See databricks/bronze/orders_stream.py for full context.
import sys
sys.path.append("/Volumes/retailsync/checkpoints")
sys.path.append("dbfs:/databricks/common")  # adjust to your import path

from cdc_bronze import start_bronze_stream, BRONZE_SOURCES

source = next(s for s in BRONZE_SOURCES if s["topic"] == "retail.public.products")
q = start_bronze_stream(spark, source)
print(f"Started bronze stream for {source['topic']} -> {source['table']}")
print(q.status)