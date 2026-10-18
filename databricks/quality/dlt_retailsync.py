# Delta Live Tables pipeline for RetailSync Bronze/Silver.
#
# Deploy this as a DLT pipeline in Databricks:
#   1. Create a pipeline in the Databricks UI (or via the API).
#   2. Point it at this notebook (databricks/quality/dlt_retailsync.py).
#   3. Set the target catalog/schema to `retailsync`.
#
# Expectations (data quality constraints) are declared inline and surface
# failures in the DLT UI + Delta Lake audit logs.
#
# Prereqs:
#   - Bronze tables populated by the Bronze streaming queries
#     (see databricks/bronze/<entity>_stream.py).
#   - `KAFKA_BOOTSTRAP_SERVERS` reachable from the pipeline cluster.

import json
from pathlib import Path
from pyspark.sql import functions as F

from delta.tables import DeltaTable

_ENTITIES_PATH = Path(__file__).resolve().parent.parent / "cdc_entities.json"
ENTITIES = json.loads(_ENTITIES_PATH.read_text())


def _envelope_schema(after):
    """Build the Debezium envelope schema from the entity config."""
    from pyspark.sql import types as T
    _TYPE_MAP = {
        "long": T.LongType(),
        "int": T.IntegerType(),
        "string": T.StringType(),
        "timestamp": T.TimestampType(),
        "decimal(12,2)": T.DecimalType(12, 2),
    }
    after_schema = T.StructType([
        T.StructField(f["name"], _TYPE_MAP[f["type"]]) for f in after
    ])
    return T.StructType([
        T.StructField("before", after_schema),
        T.StructField("after", after_schema),
        T.StructField("op", T.StringType()),
        T.StructField("ts_ms", T.LongType()),
    ])


def _parse_bronze(entity):
    """Parse the raw Bronze payload column into a structured envelope."""
    cfg = ENTITIES[entity]
    envelope = _envelope_schema(cfg["after"])
    bronze = spark.read.table(cfg["bronze"])
    return (
        bronze.withColumn("e", F.from_json("payload", envelope))
        .select("e.*", "topic", "partition", "offset", "kafka_timestamp", "ingested_at")
    )


# ---------------------------------------------------------------------------
# Bronze expectations: raw CDC must carry a valid envelope.
# ---------------------------------------------------------------------------

@dlt.expect("valid_envelope", "after is not null OR op = 'd'")
def bronze_orders():
    return _parse_bronze("orders")


@dlt.expect("valid_envelope", "after is not null OR op = 'd'")
def bronze_customers():
    return _parse_bronze("customers")


@dlt.expect("valid_envelope", "after is not null OR op = 'd'")
def bronze_products():
    return _parse_bronze("products")


@dlt.expect("valid_envelope", "after is not null OR op = 'd'")
def bronze_order_items():
    return _parse_bronze("order_items")


@dlt.expect("valid_envelope", "after is not null OR op = 'd'")
def bronze_payments():
    return _parse_bronze("payments")


# ---------------------------------------------------------------------------
# Silver expectations: business state must be consistent.
# ---------------------------------------------------------------------------

@dlt.expect("valid_status", "status in ('CREATED','PAID','SHIPPED','DELIVERED','CANCELLED')")
@dlt.expect("positive_amount", "total_amount > 0")
def silver_orders():
    cfg = ENTITIES["orders"]
    return spark.read.table(cfg["silver"])


@dlt.expect("valid_email", "email rlike '^[^@]+@[^@]+$'")
def silver_customers():
    cfg = ENTITIES["customers"]
    return spark.read.table(cfg["silver"])


@dlt.expect("positive_price", "price > 0")
def silver_products():
    cfg = ENTITIES["products"]
    return spark.read.table(cfg["silver"])


@dlt.expect("positive_quantity", "quantity > 0")
def silver_order_items():
    cfg = ENTITIES["order_items"]
    return spark.read.table(cfg["silver"])


@dlt.expect("positive_amount", "amount > 0")
def silver_payments():
    cfg = ENTITIES["payments"]
    return spark.read.table(cfg["silver"])


# ---------------------------------------------------------------------------
# Gold expectations: analytics marts must be non-empty and consistent.
# ---------------------------------------------------------------------------

@dlt.expect("non_empty", "count(*) > 0")
def gold_daily_sales():
    return spark.sql("SELECT * FROM retailsync.gold.daily_sales_bi")


@dlt.expect("non_empty", "count(*) > 0")
def gold_product_sales():
    return spark.sql("SELECT * FROM retailsync.gold.product_sales_bi")


@dlt.expect("non_empty", "count(*) > 0")
def gold_customer_360():
    return spark.sql("SELECT * FROM retailsync.gold.customer_360_bi")