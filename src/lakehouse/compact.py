"""File compaction: OPTIMIZE (+ ZORDER BY) and VACUUM with a retention guard.

Streaming writes produce many small files. Run this on a schedule (e.g. hourly for
silver, daily for gold), never inside the streaming query itself.
"""

from __future__ import annotations

import argparse
import logging

from delta.tables import DeltaTable
from pyspark.sql import SparkSession

from lakehouse.config import Settings, load_settings

log = logging.getLogger(__name__)

MIN_RETENTION_HOURS = 168  # 7 days: longer than any reader or checkpoint should lag

ZORDER_COLUMNS: dict[str, list[str]] = {
    "silver_telemetry": ["machine_id", "event_ts"],
    "silver_workorders": ["machine_id"],
    "gold_machine_health": ["machine_id"],
}


def check_retention(hours: int, minimum: int = MIN_RETENTION_HOURS) -> int:
    """Refuse a VACUUM that could delete files a running stream or time-travel still needs."""
    if hours < minimum:
        raise ValueError(
            f"retention {hours}h is below the {minimum}h guard; "
            "streams reading this table could fail with FileNotFound"
        )
    return hours


def table_paths(settings: Settings) -> dict[str, str]:
    """Tables that get compacted, keyed like ``ZORDER_COLUMNS``."""
    return {
        "silver_telemetry": settings.silver_telemetry,
        "silver_workorders": settings.silver_workorders,
        "gold_machine_health": settings.gold_machine_health,
    }


def optimize(spark: SparkSession, path: str, zorder_by: list[str]) -> None:
    """Bin-pack small files and co-locate rows on ``zorder_by`` columns."""
    table = DeltaTable.forPath(spark, path).optimize()
    metrics = table.executeZOrderBy(*zorder_by) if zorder_by else table.executeCompaction()
    log.info("OPTIMIZE %s -> %s", path, metrics.select("metrics.numFilesRemoved").first())


def vacuum(spark: SparkSession, path: str, retention_hours: int) -> None:
    """Delete files no longer referenced by the table and older than the retention."""
    DeltaTable.forPath(spark, path).vacuum(check_retention(retention_hours))
    log.info("VACUUM %s retain=%sh", path, retention_hours)


def run(spark: SparkSession, settings: Settings, retention_hours: int) -> None:
    """Compact and vacuum every silver/gold table that exists."""
    for name, path in table_paths(settings).items():
        if not DeltaTable.isDeltaTable(spark, path):
            log.warning("skip %s: not a Delta table yet", path)
            continue
        optimize(spark, path, ZORDER_COLUMNS[name])
        vacuum(spark, path, retention_hours)


def main() -> None:
    from lakehouse.session import build_spark

    parser = argparse.ArgumentParser(description="OPTIMIZE + VACUUM lakehouse tables")
    parser.add_argument("--retention-hours", type=int, default=MIN_RETENTION_HOURS)
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO)
    run(build_spark("compact", with_kafka=False), load_settings(), args.retention_hours)


if __name__ == "__main__":
    main()
