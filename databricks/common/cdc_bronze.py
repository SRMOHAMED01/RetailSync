# Shared helper for the Bronze layer (raw CDC + Kafka metadata).
#
# Bronze contract (see docs/architecture/ARCHITECTURE.md):
#   - immutable / replayable (append-only Delta)
#   - keeps the raw Debezium payload + topic, partition, offset, timestamp
#   - one table per source entity: <catalog>.bronze.<entity>_cdc
#
# Set KAFKA_BOOTSTRAP_SERVERS to a broker reachable from your Databricks
# compute (a local `localhost:9092` is NOT reachable from a cloud workspace;
# use a managed/accessible broker or equivalent network connectivity).
from pyspark.sql import functions as F

KAFKA_BOOTSTRAP_SERVERS = "CHANGE_ME:9092"

# (topic, bronze_table, checkpoint) per source entity.
BRONZE_SOURCES = [
    {
        "topic": "retail.public.customers",
        "table": "retailsync.bronze.customers_cdc",
        "checkpoint": "/Volumes/retailsync/checkpoints/bronze_customers",
    },
    {
        "topic": "retail.public.products",
        "table": "retailsync.bronze.products_cdc",
        "checkpoint": "/Volumes/retailsync/checkpoints/bronze_products",
    },
    {
        "topic": "retail.public.orders",
        "table": "retailsync.bronze.orders_cdc",
        "checkpoint": "/Volumes/retailsync/checkpoints/bronze_orders",
    },
    {
        "topic": "retail.public.order_items",
        "table": "retailsync.bronze.order_items_cdc",
        "checkpoint": "/Volumes/retailsync/checkpoints/bronze_order_items",
    },
    {
        "topic": "retail.public.payments",
        "table": "retailsync.bronze.payments_cdc",
        "checkpoint": "/Volumes/retailsync/checkpoints/bronze_payments",
    },
]


def start_bronze_stream(spark, source):
    """Start one append-only Delta streaming query for a single CDC topic."""
    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", source["topic"])
        .option("startingOffsets", "earliest")
        .load()
    )
    bronze = raw.select(
        F.col("key").cast("string").alias("event_key"),
        F.col("value").cast("string").alias("payload"),
        F.col("topic").alias("topic"),
        F.col("partition").alias("partition"),
        F.col("offset").alias("offset"),
        F.col("timestamp").alias("kafka_timestamp"),
        F.current_timestamp().alias("ingested_at"),
    )
    return (
        bronze.writeStream.format("delta")
        .option("checkpointLocation", source["checkpoint"])
        .outputMode("append")
        .toTable(source["table"])
    )


def start_all_bronze_streams(spark):
    """Start one streaming query per source entity. Returns the list of queries."""
    return [start_bronze_stream(spark, s) for s in BRONZE_SOURCES]