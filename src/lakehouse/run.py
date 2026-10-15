"""Pipeline entry point: ``python -m lakehouse.run [bronze|silver|gold|all]``.

Each streaming stage runs with ``trigger(availableNow=True)``: it drains everything
available since its checkpoint and stops, which is how the demo and a scheduled job
(e.g. the Databricks bundle) run it. Use ``--continuous`` for a long-running bronze.
"""

from __future__ import annotations

import argparse
import logging

from pyspark.sql import SparkSession

from lakehouse.bronze import start_bronze
from lakehouse.config import Settings, load_settings
from lakehouse.gold import build_gold
from lakehouse.silver_telemetry import start_silver_telemetry
from lakehouse.silver_workorders import start_silver_workorders
from lakehouse.validate import start_quarantine

log = logging.getLogger("lakehouse")
STAGES = ("bronze", "silver", "gold", "all")


def run_bronze(spark: SparkSession, settings: Settings, continuous: bool = False) -> None:
    start_bronze(spark, settings, available_now=not continuous).awaitTermination()


def run_silver(spark: SparkSession, settings: Settings) -> None:
    queries = [
        start_quarantine(spark, settings, settings.telemetry_topic, "telemetry"),
        start_silver_telemetry(spark, settings),
        start_silver_workorders(spark, settings),
    ]
    for query in queries:
        query.awaitTermination()


def run_gold(spark: SparkSession, settings: Settings, show: int = 20) -> None:
    gold = build_gold(spark, settings)
    log.info("gold rows: %s", gold.count())
    gold.orderBy("hour", "machine_id").show(show, truncate=False)


def main(argv: list[str] | None = None) -> None:
    from lakehouse.session import build_spark

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("stage", choices=STAGES, nargs="?", default="all")
    parser.add_argument("--continuous", action="store_true", help="bronze only: keep running")
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    settings = load_settings()
    spark = build_spark("lakehouse-" + args.stage, with_kafka=args.stage in ("bronze", "all"))
    if args.stage in ("bronze", "all"):
        run_bronze(spark, settings, args.continuous)
    if args.stage in ("silver", "all"):
        run_silver(spark, settings)
    if args.stage in ("gold", "all"):
        run_gold(spark, settings)


if __name__ == "__main__":
    main()
