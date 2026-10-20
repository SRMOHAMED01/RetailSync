# Delta Live Tables pipeline for RetailSync data quality.
#
# Deploy this as a DLT pipeline in Databricks:
#   1. Create a pipeline in the Databricks UI (or via the API).
#   2. Point it at this notebook (databricks/quality/dlt_retailsync.py).
#   3. Set the target catalog/schema to `retailsync`.
#
# Data quality is enforced with DLT expectations. When a row violates an
# expectation the pipeline fails (or warns, depending on config) and the
# violation is recorded in the Delta Lake audit log.
#
# Layers covered:
#   Bronze — raw CDC envelope must carry a valid `after` payload (or be a delete).
#   Silver — business rules: email format, positive amounts, valid statuses.
#   Gold   — analytics marts must be non-empty.
#
# Prereqs:
#   - Bronze tables populated by the Bronze streaming queries
#     (see databricks/bronze/<entity>_stream.py).
#   - Silver tables populated by the Silver MERGE queries
#     (see databricks/silver/<entity>_merge.py).
#   - Gold views materialized (see databricks/gold/gold_views.sql).

import json
from pathlib import Path

from pyspark.sql import functions as F

_ENTITIES_PATH = (
    Path(__file__).resolve().parent.parent / "cdc_entities.json"
)
ENTITIES = json.loads(_ENTITIES_PATH.read_text())


def _type_for(type_str):
    """Map a JSON type string to a Spark type."""
    from pyspark.sql import types as T
    return {
        "long": T.LongType(),
        "int": T.IntegerType(),
        "string": T.StringType(),
        "timestamp": T.TimestampType(),
        "decimal(12,2)": T.DecimalType(12, 2),
    }[type_str]


def _after_schema(entity_cfg):
    """Build the Spark schema for the `after` struct of an entity."""
    from pyspark.sql import types as T
    return T.StructType([
        T.StructField(f["name"], _type_for(f["type"])) for f in entity_cfg["after"]
    ])


def _envelope_schema(entity_cfg):
    """Debezium envelope: before/after share the entity schema, plus op + ts_ms."""
    from pyspark.sql import types as T
    after = _after_schema(entity_cfg)
    return T.StructType([
        T.StructField("before", after),
        T.StructField("after", after),
        T.StructField("op", T.StringType()),
        T.StructField("ts_ms", T.LongType()),
    ])


def _parse_bronze(entity):
    """Read the Bronze Delta table and parse the raw `payload` JSON column.

    Returns the structured envelope plus the Kafka metadata columns that
    Bronze retains (topic, partition, offset, timestamps).
    """
    cfg = ENTITIES[entity]
    envelope = _envelope_schema(cfg)
    bronze = spark.read.table(cfg["bronze"])
    return (
        bronze.withColumn("e", F.from_json("payload", envelope))
        .select(
            "e.*",
            "topic",
            "partition",
            "offset",
            "kafka_timestamp",
            "ingested_at",
        )
    )


# ---------------------------------------------------------------------------
# Bronze expectations: every CDC event must carry a valid envelope.
# A delete op legitimately has `after = null`, so we allow that case.
# ---------------------------------------------------------------------------


@dlt.expect("valid_envelope", "after IS NOT NULL OR op = 'd'")
def bronze_customers():
    return _parse_bronze("customers")


@dlt.expect("valid_envelope", "after IS NOT NULL OR op = 'd'")
def bronze_products():
    return _parse_bronze("products")


@dlt.expect("valid_envelope", "after IS NOT NULL OR op = 'd'")
def bronze_orders():
    return _parse_bronze("orders")


@dlt.expect("valid_envelope", "after IS NOT NULL OR op = 'd'")
def bronze_order_items():
    return _parse_bronze("order_items")


@dlt.expect("valid_envelope", "after IS NOT NULL OR op = 'd'")
def bronze_payments():
    return _parse_bronze("payments")


# ---------------------------------------------------------------------------
# Silver expectations: business rules on the current-state tables.
# ---------------------------------------------------------------------------


@dlt.expect("valid_status", "status IN ('CREATED','PAID','SHIPPED','DELIVERED','CANCELLED')")
@dlt.expect("positive_amount", "total_amount > 0")
def silver_orders():
    return spark.read.table(ENTITIES["orders"]["silver"])


@dlt.expect("valid_email", "email RLIKE '^[^@]+@[^@]+$'")
def silver_customers():
    return spark.read.table(ENTITIES["customers"]["silver"])


@dlt.expect("positive_price", "price > 0")
@dlt.expect("non_negative_stock", "stock >= 0")
def silver_products():
    return spark.read.table(ENTITIES["products"]["silver"])


@dlt.expect("positive_quantity", "quantity > 0")
@dlt.expect("positive_unit_price", "unit_price > 0")
def silver_order_items():
    return spark.read.table(ENTITIES["order_items"]["silver"])


@dlt.expect("positive_amount", "amount > 0")
@dlt.expect("valid_payment_status", "status IN ('PENDING','SUCCESS','FAILED','REFUNDED')")
def silver_payments():
    return spark.read.table(ENTITIES["payments"]["silver"])


# ---------------------------------------------------------------------------
# Gold expectations: analytics marts must exist and be non-empty.
# ---------------------------------------------------------------------------


@dlt.expect("non_empty", "COUNT(*) > 0")
def gold_daily_sales():
    return spark.sql("SELECT * FROM retailsync.gold.daily_sales_bi")


@dlt.expect("non_empty", "COUNT(*) > 0")
def gold_product_sales():
    return spark.sql("SELECT * FROM retailsync.gold.product_sales_bi")


@dlt.expect("non_empty", "COUNT(*) > 0")
def gold_customer_360():
    return spark.sql("SELECT * FROM retailsync.gold.customer_360_bi")