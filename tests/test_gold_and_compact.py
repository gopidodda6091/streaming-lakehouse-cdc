import pytest
from factories import bronze_df, cdc, reading
from pyspark.sql import functions as F

from lakehouse.compact import MIN_RETENTION_HOURS, check_retention
from lakehouse.gold import machine_health_hourly
from lakehouse.silver_telemetry import dedupe_telemetry
from lakehouse.silver_workorders import apply_changes, to_silver_rows
from lakehouse.validate import check, valid_rows


def _silver_telemetry(spark, payloads):
    return dedupe_telemetry(valid_rows(check(bronze_df(spark, "telemetry", payloads), "telemetry")))


def _silver_workorders(spark, events):
    batch = valid_rows(check(bronze_df(spark, "workorder_cdc", events), "workorder"))
    return apply_changes(to_silver_rows(batch).limit(0), batch)


def test_machine_health_hourly(spark):
    telemetry = _silver_telemetry(
        spark,
        [
            reading("a", "06:05", 70.0),
            reading("b", "06:35", 80.0),
            reading("b", "06:35", 80.0),  # duplicate must not be counted twice
            reading("c", "07:10", 99.0, status="ERROR"),
            reading("d", "06:10", 60.0, machine="M2"),
        ],
    )
    work_orders = _silver_workorders(
        spark,
        [
            cdc("c", "WO-1", "06:00"),
            cdc("u", "WO-2", "06:00", "CLOSED"),
            cdc("d", "WO-3", "06:30", None),
            cdc("c", "WO-4", "06:00", machine="M2"),
            cdc("d", "WO-4", "06:20", None, machine="M2"),
        ],
    )
    gold = machine_health_hourly(telemetry, work_orders).withColumn("h", F.hour("hour"))
    rows = {(r.machine_id, r.h): r for r in gold.collect()}
    m1_six = rows[("M1", 6)]
    assert m1_six.readings == 2
    assert m1_six.avg_temperature_c == 75.0
    assert m1_six.open_work_orders == 1
    assert m1_six.health == "GREEN"
    assert rows[("M1", 7)].health == "RED"
    assert rows[("M2", 6)].open_work_orders == 0


def test_vacuum_retention_guard():
    assert check_retention(MIN_RETENTION_HOURS) == MIN_RETENTION_HOURS
    with pytest.raises(ValueError, match="below"):
        check_retention(1)
