# Shared helper for the Silver layer (parsed, MERGE'd current state).
#
# Silver contract (see docs/architecture/ARCHITECTURE.md):
#   - reliable business state per entity (SCD-like current view)
#   - idempotent MERGE on the entity's natural key
#   - handles INSERT / UPDATE / DELETE (`op` = c / u / d)
#
# Debezium envelope parsed from the Bronze `payload` JSON column:
#   {"before": {...}?, "after": {...}?, "op": "c"|"u"|"d", "ts_ms": <int>}
#
# Timestamps come from Debezium as ISO-8601 strings
# (io.debezium.time.ZonedTimestamp), so they are parsed with a timestamp
# cast, not a LongType cast.
import json
from pathlib import Path

from pyspark.sql import functions as F, types as T
from delta.tables import DeltaTable

BRONZE_CATALOG = "retailsync.bronze"
SILVER_CATALOG = "retailsync.silver"
CHECKPOINT_ROOT = "/Volumes/retailsync/checkpoints"

_ENTITIES_PATH = Path(__file__).resolve().parent / "cdc_entities.json"
ENTITIES = json.loads(_ENTITIES_PATH.read_text())

# Map our JSON type strings to Spark types (used to build the envelope schema).
_TYPE_MAP = {
    "long": T.LongType(),
    "int": T.IntegerType(),
    "string": T.StringType(),
    "timestamp": T.TimestampType(),
    "decimal(12,2)": T.DecimalType(12, 2),
}


def _after_schema(entity_cfg):
    return T.StructType([
        T.StructField(f["name"], _TYPE_MAP[f["type"]]) for f in entity_cfg["after"]
    ])


def _envelope_schema(after):
    """Debezium envelope: before/after share the entity schema, plus op + ts_ms."""
    return T.StructType([
        T.StructField("before", after),
        T.StructField("after", after),
        T.StructField("op", T.StringType()),
        T.StructField("ts_ms", T.LongType()),
    ])


def upsert_entity(batch, batch_id, entity):
    """ForeachBatch MERGE for one entity. Idempotent on the natural key."""
    cfg = ENTITIES[entity]
    envelope = _envelope_schema(_after_schema(cfg))
    key = cfg["key"]

    parsed = (
        batch.withColumn("e", F.from_json("payload", envelope))
        .select("e.*", "topic", "partition", "offset", "kafka_timestamp", "ingested_at")
    )
    src = (
        parsed.select(
            F.coalesce(f"after.{key}", f"before.{key}").alias(key),
            "after.*",
            "op",
            "offset",
            "ingested_at",
        )
        .filter(f"{key} is not null")
    )

    if not spark.catalog.tableExists(cfg["silver"]):
        # Bootstrap an empty table preserving the schema.
        src.filter("op <> 'd'").limit(0).write.format("delta").saveAsTable(cfg["silver"])

    t = DeltaTable.forName(spark, cfg["silver"])
    (
        t.alias("t")
        .merge(src.alias("s"), f"t.{key} = s.{key}")
        .whenMatchedDelete(condition="s.op = 'd'")
        .whenMatchedUpdateAll(condition="s.op <> 'd'")
        .whenNotMatchedInsertAll(condition="s.op <> 'd'")
        .execute()
    )


def start_silver_stream(spark, entity):
    """Start the Silver MERGE streaming query for one entity."""
    cfg = ENTITIES[entity]
    checkpoint = f"{CHECKPOINT_ROOT}/silver_{entity}"
    return (
        spark.readStream.table(cfg["bronze"])
        .writeStream
        .foreachBatch(lambda b, i: upsert_entity(b, i, entity))
        .option("checkpointLocation", checkpoint)
        .start()
    )


def start_all_silver_streams(spark):
    """Start one Silver MERGE query per entity. Returns the list of queries."""
    return [start_silver_stream(spark, e) for e in ENTITIES]