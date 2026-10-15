"""Silver telemetry: valid readings, deduplicated on ``event_id`` within a 15-minute watermark."""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.streaming import StreamingQuery

from lakehouse.bronze import read_bronze_stream
from lakehouse.config import Settings
from lakehouse.validate import check, valid_rows

WATERMARK = "15 minutes"
SILVER_TELEMETRY_COLUMNS = [
    "event_id",
    "machine_id",
    "event_ts",
    "temperature_c",
    "vibration_mm_s",
    "status",
    "kafka_ts",
]


def dedupe_telemetry(valid_df: DataFrame, watermark: str = WATERMARK) -> DataFrame:
    """Drop re-sent readings.

    ``dropDuplicatesWithinWatermark`` keeps state only for ``watermark`` past the max
    event time, so state stays bounded while a duplicate arriving within 15 minutes of
    the original is still caught. Batch DataFrames (backfills, tests) have no watermark,
    so they use a plain ``dropDuplicates(['event_id'])``.
    """
    if valid_df.isStreaming:
        deduped = valid_df.withWatermark("event_ts", watermark).dropDuplicatesWithinWatermark(
            ["event_id"]
        )
    else:
        deduped = valid_df.dropDuplicates(["event_id"])
    return deduped.select(*SILVER_TELEMETRY_COLUMNS)


def start_silver_telemetry(spark: SparkSession, settings: Settings) -> StreamingQuery:
    """Bronze (telemetry topic) -> validate -> dedupe -> append to silver telemetry."""
    bronze = read_bronze_stream(spark, settings, settings.telemetry_topic)
    silver = dedupe_telemetry(valid_rows(check(bronze, "telemetry")))
    return (
        silver.writeStream.format("delta")
        .outputMode("append")
        .option("checkpointLocation", settings.checkpoint("silver_telemetry"))
        .trigger(availableNow=True)
        .start(settings.silver_telemetry)
    )
