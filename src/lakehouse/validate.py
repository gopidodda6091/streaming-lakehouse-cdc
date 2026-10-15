"""Parse bronze payloads and route rows that break a rule to quarantine with a reason code.

Every row ends up in exactly one place: silver (``reason`` is null) or quarantine
(``reason`` is the first rule it failed). Nothing is silently dropped.
"""

from __future__ import annotations

from collections.abc import Sequence

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.streaming import StreamingQuery
from pyspark.sql.types import StructType

from lakehouse.bronze import read_bronze_stream
from lakehouse.config import Settings
from lakehouse.schemas import (
    TELEMETRY_SCHEMA,
    TELEMETRY_STATUSES,
    WORKORDER_CDC_SCHEMA,
    WORKORDER_STATES,
)

Rule = tuple[str, Column]  # (reason code, condition that is TRUE when the row is invalid)

KAFKA_META = ["kafka_partition", "kafka_offset", "kafka_ts"]


def _p(name: str) -> Column:
    return F.col(f"parsed.{name}")


def _malformed() -> Rule:
    return ("MALFORMED_JSON", F.col("parsed").isNull() | (F.to_json("parsed") == "{}"))


def telemetry_rules() -> list[Rule]:
    """Rules for machine telemetry. Built lazily: Columns need an active SparkContext."""
    return [
        _malformed(),
        ("MISSING_EVENT_ID", _p("event_id").isNull()),
        ("MISSING_MACHINE_ID", _p("machine_id").isNull()),
        ("BAD_EVENT_TS", _p("event_ts").isNull()),
        ("TEMPERATURE_OUT_OF_RANGE", ~_p("temperature_c").between(-40.0, 150.0)),
        ("NEGATIVE_VIBRATION", _p("vibration_mm_s") < 0),
        ("UNKNOWN_STATUS", ~_p("status").isin(*TELEMETRY_STATUSES)),
    ]


def workorder_rules() -> list[Rule]:
    """Rules for work-order CDC events. Deletes do not need a valid state."""
    return [
        _malformed(),
        ("MISSING_WORK_ORDER_ID", _p("work_order_id").isNull()),
        ("BAD_OP_TS", _p("op_ts").isNull()),
        ("UNKNOWN_OP", ~_p("op").isin("c", "u", "d")),
        ("UNKNOWN_STATE", (_p("op") != "d") & ~_p("state").isin(*WORKORDER_STATES)),
    ]


def parse_payload(bronze_df: DataFrame, schema: StructType) -> DataFrame:
    """Add a ``parsed`` struct decoded from ``payload`` (ISO-8601 timestamps, e.g. ...Z)."""
    return bronze_df.withColumn("parsed", F.from_json("payload", schema))


def with_reason(parsed_df: DataFrame, rules: Sequence[Rule]) -> DataFrame:
    """Add ``reason``: the first failed rule, or null. A null rule result counts as failed."""
    checks = [F.when(F.coalesce(cond, F.lit(True)), F.lit(code)) for code, cond in rules]
    return parsed_df.withColumn("reason", F.coalesce(*checks))


def valid_rows(checked_df: DataFrame) -> DataFrame:
    """Rows that passed every rule, flattened, with Kafka lineage kept for tie-breaks."""
    return checked_df.where(F.col("reason").isNull()).select("parsed.*", *KAFKA_META)


def invalid_rows(checked_df: DataFrame) -> DataFrame:
    """Rows for the quarantine table: raw payload, where it came from, and why it failed."""
    return checked_df.where(F.col("reason").isNotNull()).select(
        "topic",
        "kafka_partition",
        "kafka_offset",
        "payload",
        "reason",
        F.current_timestamp().alias("quarantined_at"),
    )


def check(bronze_df: DataFrame, topic_kind: str) -> DataFrame:
    """Parse + apply the rule set for ``telemetry`` or ``workorder``."""
    if topic_kind == "telemetry":
        return with_reason(parse_payload(bronze_df, TELEMETRY_SCHEMA), telemetry_rules())
    if topic_kind == "workorder":
        return with_reason(parse_payload(bronze_df, WORKORDER_CDC_SCHEMA), workorder_rules())
    raise ValueError(f"unknown topic kind: {topic_kind}")


def start_quarantine(
    spark: SparkSession, settings: Settings, topic: str, topic_kind: str
) -> StreamingQuery:
    """Stream invalid rows of one topic from bronze into the quarantine Delta table."""
    bad = invalid_rows(check(read_bronze_stream(spark, settings, topic), topic_kind))
    return (
        bad.writeStream.format("delta")
        .outputMode("append")
        .option("checkpointLocation", settings.checkpoint(f"quarantine_{topic_kind}"))
        .trigger(availableNow=True)
        .start(settings.quarantine)
    )
