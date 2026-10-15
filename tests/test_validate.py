from factories import bronze_df, cdc, reading

from lakehouse.validate import check, invalid_rows, valid_rows


def _reasons(spark, topic, kind, payloads):
    checked = check(bronze_df(spark, topic, payloads), kind)
    return [r.reason for r in checked.orderBy("kafka_offset").select("reason").collect()]


def test_valid_telemetry_passes(spark):
    assert _reasons(spark, "telemetry", "telemetry", [reading("e1", "06:00")]) == [None]


def test_invalid_telemetry_gets_reason_codes(spark):
    payloads = [
        '{"event_id": "x", "temperature_c": ',
        reading(None, "06:00"),
        reading("e3", "06:00", temp=999.0),
        reading("e4", "06:00", vibration_mm_s=-1.0),
        reading("e5", "06:00", status="MAYBE"),
        reading("e6", "06:00", event_ts="not-a-time"),
    ]
    assert _reasons(spark, "telemetry", "telemetry", payloads) == [
        "MALFORMED_JSON",
        "MISSING_EVENT_ID",
        "TEMPERATURE_OUT_OF_RANGE",
        "NEGATIVE_VIBRATION",
        "UNKNOWN_STATUS",
        "BAD_EVENT_TS",
    ]


def test_workorder_rules_allow_delete_without_state(spark):
    payloads = [
        cdc("d", "WO-1", "07:00", state=None),
        cdc("x", "WO-2", "07:00"),
        cdc("u", "WO-3", "07:00", state="LOST"),
        cdc("u", None, "07:00"),
    ]
    assert _reasons(spark, "workorder_cdc", "workorder", payloads) == [
        None,
        "UNKNOWN_OP",
        "UNKNOWN_STATE",
        "MISSING_WORK_ORDER_ID",
    ]


def test_every_row_lands_in_exactly_one_place(spark):
    payloads = [reading("e1", "06:00"), "garbage", reading("e2", "06:01", temp=-100.0)]
    checked = check(bronze_df(spark, "telemetry", payloads), "telemetry")
    good, bad = valid_rows(checked), invalid_rows(checked)
    assert good.count() == 1
    assert bad.count() == 2
    assert set(bad.columns) >= {"payload", "reason", "kafka_offset", "quarantined_at"}
