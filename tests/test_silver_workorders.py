"""CDC semantics: latest op_ts wins, deletes are tombstones, replays change nothing.

Two layers are tested with the same scenarios:
- ``apply_changes``: the pure-DataFrame statement of the MERGE rules (always runs)
- ``merge_batch``: the real Delta MERGE used by the stream (needs Delta jars)
"""

from __future__ import annotations

from factories import bronze_df, cdc
from pyspark.sql import functions as F

from lakehouse.schemas import SILVER_WORKORDER_COLUMNS
from lakehouse.silver_workorders import (
    active_work_orders,
    apply_changes,
    latest_per_key,
    merge_batch,
    to_silver_rows,
)
from lakehouse.validate import check, valid_rows


def changes(spark, events):
    return valid_rows(check(bronze_df(spark, "workorder_cdc", events), "workorder"))


def empty_silver(spark):
    return to_silver_rows(changes(spark, [cdc("c", "WO-0", "06:00")])).limit(0)


def state_of(df) -> dict:
    """work_order_id -> (state, is_deleted, op_ts) with op_ts rendered in session UTC."""
    ts = F.date_format("op_ts", "yyyy-MM-dd'T'HH:mm:ss").alias("ts")
    rows = df.select(*SILVER_WORKORDER_COLUMNS, ts).collect()
    return {r.work_order_id: (r.state, r.is_deleted, r.ts) for r in rows}


BATCH_1 = [
    cdc("c", "WO-1", "06:00"),
    cdc("u", "WO-1", "07:00", "IN_PROGRESS"),
    cdc("c", "WO-2", "06:00"),
    cdc("c", "WO-3", "06:00"),
]
BATCH_2 = [
    cdc("u", "WO-1", "06:30", "CLOSED"),  # older than stored 07:00 -> ignored
    cdc("d", "WO-2", "08:00", None),  # delete
    cdc("u", "WO-3", "09:00", "CLOSED"),
    cdc("c", "WO-4", "09:00"),
]
BATCH_3_LATE_REPLAY = [cdc("c", "WO-2", "06:00")]  # replayed create after the delete

EXPECTED = {
    "WO-1": ("IN_PROGRESS", False, "2026-03-02T07:00:00"),
    "WO-2": (None, True, "2026-03-02T08:00:00"),
    "WO-3": ("CLOSED", False, "2026-03-02T09:00:00"),
    "WO-4": ("OPEN", False, "2026-03-02T09:00:00"),
}


# ---------- within-batch dedupe ----------


def test_latest_per_key_handles_out_of_order_batch(spark):
    batch = changes(
        spark,
        [
            cdc("u", "WO-1", "09:00", "CLOSED"),
            cdc("c", "WO-1", "06:00"),
            cdc("u", "WO-1", "07:00", "IN_PROGRESS"),
        ],
    )
    rows = latest_per_key(batch).collect()
    assert [(r.work_order_id, r.state) for r in rows] == [("WO-1", "CLOSED")]


def test_latest_per_key_tie_breaks_on_offset(spark):
    batch = changes(
        spark, [cdc("u", "WO-1", "07:00", "IN_PROGRESS"), cdc("u", "WO-1", "07:00", "CLOSED")]
    )
    assert latest_per_key(batch).first().state == "CLOSED"


# ---------- pure-DataFrame semantics ----------


def _apply_all(spark, batches):
    current = empty_silver(spark)
    for events in batches:
        current = apply_changes(current, changes(spark, events)).localCheckpoint()
    return current


def test_apply_changes_out_of_order_and_deletes(spark):
    result = _apply_all(spark, [BATCH_1, BATCH_2, BATCH_3_LATE_REPLAY])
    assert state_of(result) == EXPECTED
    assert sorted(r.work_order_id for r in active_work_orders(result).collect()) == [
        "WO-1",
        "WO-3",
        "WO-4",
    ]


def test_apply_changes_replay_is_idempotent(spark):
    once = _apply_all(spark, [BATCH_1, BATCH_2])
    twice = _apply_all(spark, [BATCH_1, BATCH_2, BATCH_1, BATCH_2])
    assert once.count() == twice.count() == 4
    assert state_of(once) == state_of(twice) == EXPECTED


# ---------- real Delta MERGE ----------


def _merge_all(spark, path, batches):
    for events in batches:
        merge_batch(spark, changes(spark, events), path)
    return spark.read.format("delta").load(path)


def test_merge_applies_latest_op_ts_and_deletes(delta_spark, tmp_path):
    path = str(tmp_path / "silver_wo")
    result = _merge_all(delta_spark, path, [BATCH_1, BATCH_2, BATCH_3_LATE_REPLAY])
    assert state_of(result) == EXPECTED


def test_merge_replay_same_batch_twice_is_idempotent(delta_spark, tmp_path):
    """The replay test: processing the same batch again must not change silver."""
    path = str(tmp_path / "silver_wo")
    _merge_all(delta_spark, path, [BATCH_1, BATCH_2])
    before = delta_spark.read.format("delta").load(path)
    count_before, state_before = before.count(), state_of(before)

    after = _merge_all(delta_spark, path, [BATCH_2, BATCH_1, BATCH_2])
    assert after.count() == count_before == 4
    assert state_of(after) == state_before == EXPECTED
