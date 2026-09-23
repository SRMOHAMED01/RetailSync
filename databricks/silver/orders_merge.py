# Starter pédagogique: parse l'enveloppe Debezium et MERGE le current state.
from pyspark.sql import functions as F, types as T
from delta.tables import DeltaTable
BRONZE="retailsync.bronze.orders_cdc"; SILVER="retailsync.silver.orders"
after_schema=T.StructType([T.StructField("order_id",T.LongType()),T.StructField("customer_id",T.LongType()),T.StructField("status",T.StringType()),T.StructField("total_amount",T.StringType()),T.StructField("created_at",T.LongType()),T.StructField("updated_at",T.LongType())])
envelope=T.StructType([T.StructField("before",after_schema),T.StructField("after",after_schema),T.StructField("op",T.StringType()),T.StructField("ts_ms",T.LongType())])

def upsert(batch, batch_id):
    parsed=(batch.withColumn("e",F.from_json("payload",envelope)).select("e.*","topic","partition","offset","kafka_timestamp","ingested_at"))
    src=(parsed.select(F.coalesce("after.order_id","before.order_id").alias("order_id"),"after.customer_id","after.status",F.col("after.total_amount").cast("decimal(12,2)").alias("total_amount"),"op","offset","ingested_at").filter("order_id is not null"))
    if not spark.catalog.tableExists(SILVER):
        src.filter("op <> 'd'").limit(0).write.format("delta").saveAsTable(SILVER)
    t=DeltaTable.forName(spark,SILVER)
    (t.alias("t").merge(src.alias("s"),"t.order_id=s.order_id")
      .whenMatchedDelete(condition="s.op='d'")
      .whenMatchedUpdateAll(condition="s.op<>'d'")
      .whenNotMatchedInsertAll(condition="s.op<>'d'").execute())

(spark.readStream.table(BRONZE).writeStream.foreachBatch(upsert).option("checkpointLocation","/Volumes/retailsync/checkpoints/silver_orders").start())
