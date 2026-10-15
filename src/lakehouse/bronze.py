"""Kafka source -> append-only bronze Delta table with ingestion metadata.

Bronze keeps the raw payload as a string. Nothing is parsed or dropped here, so any
later bug in validation can be fixed and silver rebuilt from bronze.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.streaming import StreamingQuery

from lakehouse.config import Settings


def kafka_to_bronze(kafka_df: DataFrame) -> DataFrame:
    """Project the Kafka source columns into the bronze layout (see ``BRONZE_SCHEMA``)."""
    return kafka_df.select(
        F.col("topic"),
        F.col("partition").alias("kafka_partition"),
        F.col("offset").alias("kafka_offset"),
        F.col("timestamp").alias("kafka_ts"),
        F.col("value").cast("string").alias("payload"),
        F.current_timestamp().alias("ingested_at"),
    )


def read_kafka(spark: SparkSession, settings: Settings) -> DataFrame:
    """Streaming Kafka reader for both topics, bounded per micro-batch."""
    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", settings.kafka_bootstrap)
        .option("subscribe", f"{settings.telemetry_topic},{settings.workorder_topic}")
        .option("startingOffsets", "earliest")
        .option("maxOffsetsPerTrigger", settings.max_offsets_per_trigger)
        .option("failOnDataLoss", "false")
        .load()
    )


def start_bronze(
    spark: SparkSession, settings: Settings, available_now: bool = True
) -> StreamingQuery:
    """Start the Kafka -> bronze query. ``available_now`` drains the backlog and stops."""
    writer = (
        kafka_to_bronze(read_kafka(spark, settings))
        .writeStream.format("delta")
        .outputMode("append")
        .partitionBy("topic")
        .option("checkpointLocation", settings.checkpoint("bronze"))
    )
    writer = (
        writer.trigger(availableNow=True)
        if available_now
        else writer.trigger(processingTime=settings.trigger_interval)
    )
    return writer.start(settings.bronze)


def read_bronze_stream(spark: SparkSession, settings: Settings, topic: str) -> DataFrame:
    """Stream one topic back out of bronze; downstream jobs never re-read Kafka."""
    return spark.readStream.format("delta").load(settings.bronze).where(F.col("topic") == topic)
