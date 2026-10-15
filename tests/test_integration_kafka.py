"""End-to-end: generator -> Kafka -> bronze -> silver -> gold, then replay silver.

Skipped unless ``KAFKA_BOOTSTRAP`` points at a running broker (``make up`` starts one).
Needs the Delta and Kafka connector jars, which Spark fetches from Maven Central.
"""

from __future__ import annotations

import os
import uuid

import pytest

pytestmark = pytest.mark.integration

BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP")


@pytest.mark.skipif(not BOOTSTRAP, reason="set KAFKA_BOOTSTRAP to run the Kafka integration test")
def test_end_to_end_and_replay(tmp_path):
    from generate_events import generate, produce

    from lakehouse.config import Settings
    from lakehouse.run import run_bronze, run_gold, run_silver
    from lakehouse.session import build_spark

    suffix = uuid.uuid4().hex[:8]
    settings = Settings(
        kafka_bootstrap=BOOTSTRAP,
        telemetry_topic=f"telemetry_{suffix}",
        workorder_topic=f"workorder_cdc_{suffix}",
        lake_root=str(tmp_path / "lake"),
    )
    renamed = [
        (settings.telemetry_topic if t == "telemetry" else settings.workorder_topic, k, v)
        for t, k, v in generate(3_000, seed=1)
    ]
    produce(renamed, BOOTSTRAP)

    spark = build_spark("integration", with_kafka=True)
    run_bronze(spark, settings)
    run_silver(spark, settings)
    run_gold(spark, settings, show=0)

    silver = spark.read.format("delta").load(settings.silver_workorders)
    before = sorted(silver.collect())
    quarantined = spark.read.format("delta").load(settings.quarantine).count()

    # Replay: wipe the silver checkpoint so every bronze row is processed again.
    import shutil

    shutil.rmtree(settings.checkpoint("silver_workorders"))
    run_silver(spark, settings)
    after = sorted(spark.read.format("delta").load(settings.silver_workorders).collect())

    assert quarantined > 0
    assert before == after
