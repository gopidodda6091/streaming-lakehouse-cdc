"""Silver work orders: CDC applied with ``foreachBatch`` + Delta ``MERGE``.

Rules, in order:
1. Inside one micro-batch keep only the latest event per ``work_order_id``
   (``op_ts`` desc, then Kafka partition/offset desc as a deterministic tie-break).
2. A change is applied only if its ``op_ts`` is strictly newer than the stored row.
3. Deletes are soft: the row becomes a tombstone (``is_deleted = true``) that keeps its
   ``op_ts``. A hard delete would let a replayed, older ``c``/``u`` event resurrect it.

Because of 2 and 3, re-processing any batch (checkpoint loss, backfill, Kafka replay)
leaves the table unchanged: the MERGE is idempotent.
"""

from __future__ import annotations

from collections.abc import Callable

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.streaming import StreamingQuery

from lakehouse.bronze import read_bronze_stream
from lakehouse.config import Settings
from lakehouse.schemas import SILVER_WORKORDER_COLUMNS
from lakehouse.validate import check, invalid_rows, valid_rows

KEY = "work_order_id"
MERGE_CONDITION = f"t.{KEY} = s.{KEY}"
NEWER_CONDITION = "s.op_ts > t.op_ts"


def latest_per_key(changes: DataFrame) -> DataFrame:
    """Collapse a batch to one change per key: the one with the highest ``op_ts``."""
    w = Window.partitionBy(KEY).orderBy(
        F.col("op_ts").desc(), F.col("kafka_partition").desc(), F.col("kafka_offset").desc()
    )
    return changes.withColumn("_rn", F.row_number().over(w)).where("_rn = 1").drop("_rn")


def to_silver_rows(changes: DataFrame) -> DataFrame:
    """Shape CDC events into silver rows; deletes become tombstones."""
    return changes.withColumn("is_deleted", F.col("op") == F.lit("d")).select(
        *SILVER_WORKORDER_COLUMNS
    )


def prepare_changes(batch: DataFrame) -> DataFrame:
    """The exact source DataFrame the MERGE uses."""
    return to_silver_rows(latest_per_key(batch))


def apply_changes(current: DataFrame, batch: DataFrame) -> DataFrame:
    """Pure-DataFrame equivalent of :func:`merge_batch` (same rules, no Delta needed).

    Used to document and unit-test the semantics; the stored row wins on equal ``op_ts``,
    exactly like ``whenMatchedUpdateAll(condition='s.op_ts > t.op_ts')``.
    """
    stored = current.select(*SILVER_WORKORDER_COLUMNS).withColumn("_src", F.lit(0))
    incoming = prepare_changes(batch).withColumn("_src", F.lit(1))
    w = Window.partitionBy(KEY).orderBy(F.col("op_ts").desc(), F.col("_src").asc())
    return (
        stored.unionByName(incoming)
        .withColumn("_rn", F.row_number().over(w))
        .where("_rn = 1")
        .drop("_rn", "_src")
    )


def ensure_table(spark: SparkSession, path: str, like: DataFrame) -> None:
    """Create an empty silver Delta table with the batch's schema if it does not exist."""
    if not DeltaTable.isDeltaTable(spark, path):
        like.limit(0).write.format("delta").save(path)


def merge_batch(spark: SparkSession, batch: DataFrame, path: str) -> None:
    """Apply one micro-batch of CDC events to the silver Delta table."""
    source = prepare_changes(batch)
    ensure_table(spark, path, source)
    (
        DeltaTable.forPath(spark, path)
        .alias("t")
        .merge(source.alias("s"), MERGE_CONDITION)
        .whenMatchedUpdateAll(condition=NEWER_CONDITION)
        .whenNotMatchedInsertAll()
        .execute()
    )


def active_work_orders(silver: DataFrame) -> DataFrame:
    """What analysts query: tombstones removed."""
    return silver.where(~F.col("is_deleted"))


def make_batch_writer(path: str, quarantine_path: str) -> Callable[[DataFrame, int], None]:
    """``foreachBatch`` handler: invalid rows to quarantine, valid rows MERGEd into silver."""

    def _write(checked: DataFrame, batch_id: int) -> None:
        checked.persist()
        try:
            # txnAppId/txnVersion make the append idempotent if Spark retries this batch.
            (
                invalid_rows(checked)
                .write.format("delta")
                .mode("append")
                .option("txnAppId", "silver_workorders_quarantine")
                .option("txnVersion", batch_id)
                .save(quarantine_path)
            )
            merge_batch(checked.sparkSession, valid_rows(checked), path)
        finally:
            checked.unpersist()

    return _write


def start_silver_workorders(spark: SparkSession, settings: Settings) -> StreamingQuery:
    """Bronze (CDC topic) -> validate -> foreachBatch(quarantine + MERGE)."""
    checked = check(read_bronze_stream(spark, settings, settings.workorder_topic), "workorder")
    return (
        checked.writeStream.foreachBatch(
            make_batch_writer(settings.silver_workorders, settings.quarantine)
        )
        .option("checkpointLocation", settings.checkpoint("silver_workorders"))
        .trigger(availableNow=True)
        .start()
    )
