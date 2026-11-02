# Design tradeoffs and limits

## PostgreSQL as a local analytical store

The required profile uses a dedicated PostgreSQL database with warehouse-style facts, dimensions, and SQL marts. This keeps local setup measurable and inexpensive while demonstrating contracts, incremental ELT, backfills, dimensional history, lineage, and reconciliation. It does not establish Snowflake, BigQuery, distributed storage, or production scale experience. A cloud warehouse should be a separate role-driven extension.

## Correctness and publication cost

Copying the previous model tables into an isolated candidate schema makes failure recovery and atomic publication straightforward. Incremental transformation touches selected date partitions, but cloning still costs time and storage proportional to retained model size. Report both end-to-end runtime and work processed; do not describe this as a fully zero-copy warehouse implementation. Old publication schemas are deliberately retained for audit and comparisons; a larger deployment needs a tested retention policy.

One advisory lock serializes writers. This favors a clear single-writer correctness story over ingestion concurrency. Source versions, durable manifests, and idempotent merges handle repeated deliveries. The project demonstrates at-least-once delivery with idempotent analytical effects, rather than claiming end-to-end exactly-once transport.

## Late data, history, and as-of answers

Routine lookback windows bound reprocessing. Late refunds must map to the original sale partition, and both the old and new partition must be rebuilt if a correction moves a record. Older changes require a selected-date historical backfill or a full refresh. A changed dimension can invalidate historical joins, which must be rebuilt rather than silently leaving a stale surrogate.

Catalog effective timestamps support business-time history; ingestion timestamps record arrival time; published schema history supports what-was-known-at-publication comparisons. These times are not interchangeable. A reproducible reference as-of date is useful for fixtures, while wall-clock source lag remains an operational diagnostic.

When `--as-of` is omitted, the inferred date is the maximum of manifest partitions and the previous successful publication date. This allows historical correction deliveries while preserving the reporting clock; an explicit backward as-of request is rejected.

## Local Airflow profile

The optional Airflow profile uses `standalone`, SQLite metadata, and `SequentialExecutor`. It provides a real scheduler/UI and finite DAG/backfill execution without a message broker or a separate worker fleet. It runs tasks sequentially and is a development configuration. Production use would need supported metadata storage, secrets/authentication configuration, retention, observability, availability planning, and separate component management.

Airflow and dbt have independent dependency constraints. A separate runtime virtual environment inside the Airflow image keeps installing dbt from mutating Airflow's environment. The same commerce CLI handles scheduled and direct batch runs so scheduling does not change publication semantics.

## Memory and benchmarks

Compose imposes container memory and CPU limits. Docker Desktop/WSL and image-build overhead sit outside those process/container budgets. The base sample is intentionally small; benchmarks record actual generated sizes, repeated runtimes, peak memory, query latency, plans, and hardware/runtime context. Results describe the measured synthetic workload only. Incremental execution may be slower than a full rebuild at small scale because startup, validation, and cloning costs dominate.

## Scope

Streaming brokers/consumers, cloud infrastructure, Spark, CDC, ClickHouse, dashboards, and a separate frontend remain optional extensions. If streaming is added, its long-lived consumer must remain separate from Airflow scheduling and prove durable offsets, duplicate handling, out-of-order reconciliation, and recovery before the extension can be called complete.
