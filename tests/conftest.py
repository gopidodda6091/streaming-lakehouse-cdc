"""Shared local SparkSession.

The session tries to load Delta Lake (jars resolved from Maven Central on first use).
If that is impossible (offline machine, blocked proxy) the tests fall back to plain
Spark: Delta-only tests are skipped with a clear reason and everything else still runs.
Set ``LAKEHOUSE_TEST_DELTA=0`` to skip the Delta attempt.
"""

from __future__ import annotations

import os
import sys
from collections.abc import Iterator

import pytest
from pyspark.sql import SparkSession

# Spark workers must run the same interpreter as the driver (the venv's python).
os.environ.setdefault("PYSPARK_PYTHON", sys.executable)


def _base_builder() -> SparkSession.Builder:
    return (
        SparkSession.builder.master("local[2]")
        .appName("lakehouse-tests")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.enabled", "false")
    )


def _delta_session() -> SparkSession | None:
    if os.environ.get("LAKEHOUSE_TEST_DELTA", "1") == "0":
        return None
    from delta import configure_spark_with_delta_pip

    builder = (
        _base_builder()
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog"
        )
    )
    try:
        spark = configure_spark_with_delta_pip(builder).getOrCreate()
        spark.range(1).write.format("delta").mode("overwrite").save(_probe_path())
        return spark
    except Exception:  # noqa: BLE001 - any failure means "no Delta here"
        active = SparkSession.getActiveSession()
        if active is not None:
            active.stop()
        return None


def _probe_path() -> str:
    import tempfile

    return os.path.join(tempfile.mkdtemp(prefix="delta-probe-"), "t")


@pytest.fixture(scope="session")
def spark_and_delta() -> Iterator[tuple[SparkSession, bool]]:
    spark = _delta_session()
    has_delta = spark is not None
    if spark is None:
        spark = _base_builder().getOrCreate()
    yield spark, has_delta
    spark.stop()


@pytest.fixture(scope="session")
def spark(spark_and_delta: tuple[SparkSession, bool]) -> SparkSession:
    return spark_and_delta[0]


@pytest.fixture()
def delta_spark(spark_and_delta: tuple[SparkSession, bool]) -> SparkSession:
    spark, has_delta = spark_and_delta
    if not has_delta:
        pytest.skip("Delta Lake jars unavailable (no Maven Central access); MERGE test skipped")
    return spark
