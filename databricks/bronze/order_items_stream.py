# Databricks notebook — Bronze streaming for `order_items`.
#
# Appends raw Debezium CDC events for the `order_items` table to
# `retailsync.bronze.order_items_cdc`. Append-only Delta — full history
# (every INSERT/UPDATE/DELETE) is replayable.
#
# Natural key: `order_item_id` (BIGSERIAL primary key). Each line belongs to
# an order and references a product.
#
# Prereqs:
#   1. A Databricks workspace + cluster with Delta + Kafka support.
#   2. `common/` on the notebook search path (or paste cdc_bronze.py above).
#   3. KAFKA_BOOTSTRAP_SERVERS set to a broker reachable from your compute
#      (see docs/runbooks/DATABRICKS.md — a local `localhost:9092` is not
#      reachable from a cloud workspace).
#   4. Catalog/schema `retailsync.bronze` and checkpoint volume exist.
#
# Run this cell to start the stream.
import sys
sys.path.append("/Volumes/retailsync/checkpoints")
sys.path.append("dbfs:/databricks/common")  # adjust to your import path

from cdc_bronze import start_bronze_stream, BRONZE_SOURCES

# The order_items topic carries the Debezium envelope for every order_items row.
source = next(s for s in BRONZE_SOURCES if s["topic"] == "retail.public.order_items")
q = start_bronze_stream(spark, source)
print(f"Started bronze stream for {source['topic']} -> {source['table']}")
print(f"Checkpoint: {source['checkpoint']}")
print(q.status)