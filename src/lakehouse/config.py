"""Paths, topics and checkpoint locations, resolved from environment variables."""

from __future__ import annotations

import os
from dataclasses import dataclass

DELTA_PACKAGE = "io.delta:delta-spark_2.12:3.2.1"
KAFKA_PACKAGE = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3"


@dataclass(frozen=True)
class Settings:
    """Runtime settings for every job. One lake root, everything else is derived."""

    kafka_bootstrap: str = "localhost:19092"
    telemetry_topic: str = "telemetry"
    workorder_topic: str = "workorder_cdc"
    lake_root: str = "./data/lake"
    trigger_interval: str = "30 seconds"
    max_offsets_per_trigger: int = 20_000

    def table_path(self, layer: str, name: str) -> str:
        """Location of a Delta table, e.g. ``table_path('silver', 'telemetry')``."""
        return f"{self.lake_root}/{layer}/{name}"

    def checkpoint(self, job: str) -> str:
        """One checkpoint directory per streaming query; never share them."""
        return f"{self.lake_root}/_checkpoints/{job}"

    @property
    def bronze(self) -> str:
        return self.table_path("bronze", "events")

    @property
    def quarantine(self) -> str:
        return self.table_path("bronze", "quarantine")

    @property
    def silver_telemetry(self) -> str:
        return self.table_path("silver", "telemetry")

    @property
    def silver_workorders(self) -> str:
        return self.table_path("silver", "work_orders")

    @property
    def gold_machine_health(self) -> str:
        return self.table_path("gold", "machine_health_hourly")


def load_settings() -> Settings:
    """Build settings from ``LAKEHOUSE_*`` / ``KAFKA_BOOTSTRAP`` environment variables."""
    env = os.environ
    return Settings(
        kafka_bootstrap=env.get("KAFKA_BOOTSTRAP", Settings.kafka_bootstrap),
        telemetry_topic=env.get("TELEMETRY_TOPIC", Settings.telemetry_topic),
        workorder_topic=env.get("WORKORDER_TOPIC", Settings.workorder_topic),
        lake_root=env.get("LAKEHOUSE_ROOT", Settings.lake_root),
        trigger_interval=env.get("TRIGGER_INTERVAL", Settings.trigger_interval),
        max_offsets_per_trigger=int(
            env.get("MAX_OFFSETS_PER_TRIGGER", Settings.max_offsets_per_trigger)
        ),
    )
