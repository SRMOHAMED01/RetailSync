# Databricks starter — adapter bootstrap servers/catalog/checkpoint à votre environnement.
from pyspark.sql import functions as F
KAFKA_BOOTSTRAP_SERVERS = "CHANGE_ME:9092"
TOPIC = "retail.public.orders"
BRONZE_TABLE = "retailsync.bronze.orders_cdc"
CHECKPOINT = "/Volumes/retailsync/checkpoints/bronze_orders"
raw = (spark.readStream.format("kafka")
       .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
       .option("subscribe", TOPIC).option("startingOffsets", "earliest").load())
bronze = raw.select(F.col("key").cast("string").alias("event_key"),F.col("value").cast("string").alias("payload"),"topic","partition","offset",F.col("timestamp").alias("kafka_timestamp"),F.current_timestamp().alias("ingested_at"))
(bronze.writeStream.format("delta").option("checkpointLocation", CHECKPOINT).outputMode("append").toTable(BRONZE_TABLE))
