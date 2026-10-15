# Cost and tuning notes

Practical levers for this pipeline, roughly in the order they pay off. Numbers are starting points, not benchmarks.

## 1. Small files are the main cost driver
Every micro-batch writes at least one file per partition it touches. A 30-second trigger on a table partitioned by date writes ~2,900 files per day per partition before compaction. Readers then pay per file (listing, opening, footer reads), and so does `MERGE`, which must rewrite every file containing a matched key.

- Run `compact.py` (`OPTIMIZE` + `ZORDER BY`) on a schedule: hourly for silver, daily for gold. Never inside the streaming query.
- On Databricks, turn on optimized writes and auto-compaction (`delta.autoOptimize.optimizeWrite`, `delta.autoOptimize.autoCompact`) for silver; keep the scheduled `OPTIMIZE` for Z-ordering.
- Target file size of 128–256 MB for tables that are mostly scanned; smaller (32–64 MB) for tables that are mostly merged into, because a `MERGE` rewrites whole files.

## 2. Partitioning vs clustering
- Partition only on a low-cardinality column that almost every query filters on, and only when each partition will hold at least ~1 GB. Bronze is partitioned by `topic`; silver and gold are not partitioned at this volume.
- Use `ZORDER BY` (or liquid clustering on Databricks, `CLUSTER BY`) for high-cardinality filter columns such as `machine_id`, `event_ts`. Clustering can be changed later without rewriting the table layout by hand; a partition column cannot.
- Over-partitioning (for example by `machine_id` and hour) is the most common way to create millions of tiny files.

## 3. Make MERGE cheap
- Deduplicate the batch before merging (`latest_per_key`): one source row per key avoids the "multiple source rows matched" error and shrinks the join.
- Add a pruning predicate when the target is large, for example restrict the target to recent `opened_at` partitions or clustered ranges that the batch can touch.
- Soft deletes (tombstones) keep replays safe. Purge tombstones older than the replay horizon (Kafka retention + checkpoint recovery window) in a periodic job.

## 4. Trigger intervals
| Need | Trigger | Comment |
|---|---|---|
| Dashboards refreshed every few minutes | `processingTime='1 minute'` to `'5 minutes'` | Fewer, larger files; cheaper `MERGE` |
| Hourly or daily freshness | `availableNow=True` from a scheduled job | Cluster runs only while there is work; usually the cheapest option |
| Sub-minute alerting | `processingTime='10 seconds'` on a small dedicated cluster | Keep this path narrow; do not run gold here |

Rule of thumb: halving the trigger interval roughly doubles file count and per-batch overhead. Bound batch size with `maxOffsetsPerTrigger` so a backlog after an outage does not create one huge batch.

## 5. Watermarks and state
- The 15-minute watermark bounds dedup state for telemetry. A longer watermark catches later duplicates but grows state (memory, checkpoint size) linearly with event rate.
- Use the RocksDB state store provider for large state (`spark.sql.streaming.stateStore.providerClass`).
- Events later than the watermark are not deduplicated by the stream. The `MERGE` path (work orders) does not depend on the watermark at all.

## 6. Cluster sizing and autoscaling
- Streaming queries do not scale down well under classic autoscaling: shuffle and state partitions are fixed when the query starts. Size a fixed cluster for steady load, or use `availableNow` jobs on job clusters (or serverless jobs) and let the cluster terminate.
- Set `spark.sql.shuffle.partitions` close to the total executor cores for streaming jobs (default 200 is far too many for small batches; this repo uses 8 locally). Changing it later requires a new checkpoint.
- Run several small streams on one cluster rather than one cluster per stream; streams are mostly idle between triggers.
- Use spot/preemptible workers for `availableNow` and compaction jobs only; checkpoints make them safe to retry.

## 7. Storage housekeeping
- `VACUUM` with at least 7 days retention (the guard in `compact.py`). Shorter retention can delete files a lagging stream or a time-travel query still needs.
- Keep checkpoints on the same storage as the tables, and never share a checkpoint between two queries.
- Set `delta.logRetentionDuration` and `delta.deletedFileRetentionDuration` explicitly on silver so storage cost is predictable.
