"""SparkSession factory with Delta Lake (and optionally the Kafka connector) configured."""

from __future__ import annotations

import os

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

from lakehouse.config import KAFKA_PACKAGE


def build_spark(app_name: str = "lakehouse", with_kafka: bool = True) -> SparkSession:
    """Local SparkSession with Delta extensions. Small shuffle partitions for a laptop.

    On Databricks the runtime already ships Delta and Kafka, so the existing session is used.
    """
    if os.environ.get("DATABRICKS_RUNTIME_VERSION"):
        return SparkSession.builder.appName(app_name).getOrCreate()
    builder = (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.databricks.delta.retentionDurationCheck.enabled", "true")
    )
    extra = [KAFKA_PACKAGE] if with_kafka else []
    return configure_spark_with_delta_pip(builder, extra_packages=extra).getOrCreate()
