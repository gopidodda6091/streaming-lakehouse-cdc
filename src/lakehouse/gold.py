"""Gold: hourly machine-health aggregates.

Rebuilt as a batch job from silver. Recomputing from deduplicated, merged silver makes
gold deterministic: running it twice gives the same rows.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from lakehouse.config import Settings
from lakehouse.silver_workorders import active_work_orders


def hourly_telemetry(telemetry: DataFrame) -> DataFrame:
    """Aggregate silver telemetry to one row per machine per hour."""
    return (
        telemetry.withColumn("hour", F.date_trunc("hour", "event_ts"))
        .groupBy("machine_id", "hour")
        .agg(
            F.count("*").alias("readings"),
            F.round(F.avg("temperature_c"), 2).alias("avg_temperature_c"),
            F.max("temperature_c").alias("max_temperature_c"),
            F.round(F.avg("vibration_mm_s"), 3).alias("avg_vibration_mm_s"),
            F.sum(F.when(F.col("status") == "ERROR", 1).otherwise(0)).alias("error_readings"),
        )
    )


def open_work_orders(work_orders: DataFrame) -> DataFrame:
    """Current count of not-yet-closed, not-deleted work orders per machine."""
    return (
        active_work_orders(work_orders)
        .where(F.col("state") != "CLOSED")
        .groupBy("machine_id")
        .agg(F.count("*").alias("open_work_orders"))
    )


def machine_health_hourly(telemetry: DataFrame, work_orders: DataFrame) -> DataFrame:
    """Hourly telemetry + current open work-order backlog, with a RED/AMBER/GREEN flag."""
    joined = hourly_telemetry(telemetry).join(open_work_orders(work_orders), "machine_id", "left")
    return (
        joined.fillna({"open_work_orders": 0})
        .withColumn(
            "health",
            F.when((F.col("error_readings") > 0) | (F.col("max_temperature_c") > 95), "RED")
            .when((F.col("avg_temperature_c") > 80) | (F.col("avg_vibration_mm_s") > 4.5), "AMBER")
            .otherwise("GREEN"),
        )
        .orderBy("hour", "machine_id")
    )


def build_gold(spark: SparkSession, settings: Settings) -> DataFrame:
    """Recompute gold from silver and overwrite the gold Delta table."""
    telemetry = spark.read.format("delta").load(settings.silver_telemetry)
    work_orders = spark.read.format("delta").load(settings.silver_workorders)
    gold = machine_health_hourly(telemetry, work_orders)
    (
        gold.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(settings.gold_machine_health)
    )
    return spark.read.format("delta").load(settings.gold_machine_health)
