from factories import bronze_df, reading

from lakehouse.silver_telemetry import dedupe_telemetry
from lakehouse.validate import check, valid_rows


def test_duplicates_dropped_late_events_kept(spark):
    payloads = [
        reading("e1", "06:00"),
        reading("e2", "06:05"),
        reading("e1", "06:00"),  # re-sent duplicate
        reading("e3", "05:40"),  # late, but within no-state batch semantics
    ]
    valid = valid_rows(check(bronze_df(spark, "telemetry", payloads), "telemetry"))
    out = dedupe_telemetry(valid)
    assert sorted(r.event_id for r in out.collect()) == ["e1", "e2", "e3"]
    assert "kafka_partition" not in out.columns
