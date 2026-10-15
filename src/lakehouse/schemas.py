"""Explicit StructTypes. Streams never infer schemas."""

from __future__ import annotations

from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

TELEMETRY_SCHEMA = StructType(
    [
        StructField("event_id", StringType()),
        StructField("machine_id", StringType()),
        StructField("event_ts", TimestampType()),
        StructField("temperature_c", DoubleType()),
        StructField("vibration_mm_s", DoubleType()),
        StructField("status", StringType()),
    ]
)

WORKORDER_CDC_SCHEMA = StructType(
    [
        StructField("op", StringType()),  # c = create, u = update, d = delete
        StructField("op_ts", TimestampType()),
        StructField("work_order_id", StringType()),
        StructField("machine_id", StringType()),
        StructField("state", StringType()),
        StructField("priority", StringType()),
        StructField("opened_at", TimestampType()),
    ]
)

BRONZE_SCHEMA = StructType(
    [
        StructField("topic", StringType()),
        StructField("kafka_partition", IntegerType()),
        StructField("kafka_offset", LongType()),
        StructField("kafka_ts", TimestampType()),
        StructField("payload", StringType()),
        StructField("ingested_at", TimestampType()),
    ]
)

SILVER_WORKORDER_COLUMNS = [
    "work_order_id",
    "machine_id",
    "state",
    "priority",
    "opened_at",
    "op",
    "op_ts",
    "is_deleted",
]

WORKORDER_STATES = ("OPEN", "IN_PROGRESS", "CLOSED")
TELEMETRY_STATUSES = ("OK", "WARN", "ERROR")
