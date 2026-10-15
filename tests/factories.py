"""Small builders for bronze-shaped test batches."""

from __future__ import annotations

import json
from datetime import datetime, timezone

from pyspark.sql import DataFrame, SparkSession

from lakehouse.schemas import BRONZE_SCHEMA

_TS = datetime(2026, 3, 2, 6, 0, tzinfo=timezone.utc)


def bronze_df(spark: SparkSession, topic: str, payloads: list[dict | str]) -> DataFrame:
    """Build a bronze-shaped batch DataFrame; offsets follow list order."""
    rows = [
        (
            topic,
            0,
            offset,
            _TS,
            p if isinstance(p, str) else json.dumps(p),
            _TS,
        )
        for offset, p in enumerate(payloads)
    ]
    return spark.createDataFrame(rows, BRONZE_SCHEMA)


def cdc(op: str, wo: str, ts: str, state: str | None = "OPEN", machine: str = "M1") -> dict:
    """Work-order change event; ``ts`` is HH:MM on the fixture day."""
    return {
        "op": op,
        "op_ts": f"2026-03-02T{ts}:00.000Z",
        "work_order_id": wo,
        "machine_id": machine,
        "state": state,
        "priority": "HIGH",
        "opened_at": "2026-03-02T06:00:00.000Z",
    }


def reading(event_id: str, ts: str, temp: float = 70.0, machine: str = "M1", **kw) -> dict:
    """Telemetry reading; ``ts`` is HH:MM on the fixture day."""
    base = {
        "event_id": event_id,
        "machine_id": machine,
        "event_ts": f"2026-03-02T{ts}:00.000Z",
        "temperature_c": temp,
        "vibration_mm_s": 2.0,
        "status": "OK",
    }
    return {**base, **kw}
