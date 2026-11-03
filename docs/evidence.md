# Reproducing correctness and performance evidence

All inputs are synthetic. Exact reference fixtures live under `fixtures/`; larger data is deterministically generated from a committed seed and workload parameters. The authoritative observed benchmark artifact is a JSON report produced by `commerce benchmark`, with its runtime context and limitations. Never replace an unexecuted scenario with a claimed successful result.

## Observed local batch run

On 2026-11-02, the Docker batch demo completed on the Windows development machine using Docker Desktop's Linux engine and the Compose PostgreSQL service. The deterministic seed-42, seven-day, 20-orders/day workload produced 140 orders, 189 order lines, and 21 refund events. The initial build passed its dbt publication gate. Replaying the same manifest accepted the duplicate delivery with zero canonical source changes and identical published fact/mart counts and sales totals. These are synthetic demo observations, not production throughput claims.

```sh
docker compose run --build --rm runner demo
```

## Original single-interval Airflow observation

On the same machine, the pinned `apache/airflow:2.11.0-python3.12` image with the isolated commerce virtual environment built successfully. `scripts/stage_intervals.py` produced nine self-contained, checksum-verified daily deliveries for 2024-01-01 through 2024-01-09. The actual Airflow backfill below executed the DAG's finite BashOperator against a separate `commerce_schedule` analytical database, using temporary SQLite Airflow metadata:

```sh
airflow dags backfill commerce_batch --start-date 2024-01-01 --end-date 2024-01-01
```

The Docker invocation of `scripts/test_airflow.sh` exited 0. Run `374f050c0c3e4082913d76ab49dc1fb7` published as-of 2024-01-01: 38 source records received/accepted, zero rejected, 27 order-line facts, 20 order lifecycle facts, six inventory snapshots, and two date/location sales rows. All 99 dbt build/test nodes completed without failed, errored, or skipped nodes. Application run time in its audit ledger was 22.815 seconds; this excludes image download/build, Airflow initialization, and CLI startup. The container had a 2 GiB memory cap and 1.5 CPU limit, and PostgreSQL had its Compose 384 MiB cap.

At that earlier revision, the smoke harness parsed the DAG, migrated temporary metadata, ran one backfill interval, and asserted warehouse publication and quality results. This historical observation establishes one daily scheduler execution; it does not describe the revised shared-volume/failure-recovery harness below. Detailed original logs/artifacts remain ignored under `data/airflow-smoke.log` and `data/airflow-smoke/`.

## Follow-up fixes and current validation

The follow-up audit identified seven missing cases: Linux artifact ownership,
old-snapshot reads during publication, end-of-day dimension attribution, additive
partition-field collisions, durable input rejection, skipped initial daily
coverage, and conflicting-version resolution. Their implementation and regression
coverage are recorded in the [follow-up review record](reviews/followup-fixes.md).

The complete revised native suite passed **142 tests in 555.36 seconds** with
disposable PostgreSQL databases. Independent Astra medium review additionally
reran the inventory-history and scheduling regressions: **3 passed in 105.36
seconds**. Current successful dbt builds have 16 models and 84 data tests (100
nodes), including independent inventory closing-day history validation. Older
83-test/99-node observations in this report are historical, not current counts.

The updated `scripts/test_container_workflow.sh` prepares real Linux shared
artifact and metadata mounts, runs the nonroot batch producer, and executes the
actual Airflow DAG. It injects a missing first delivery, verifies that the later
interval cannot publish through the gap, repairs the failed/blocked interval
range, and retries the older interval after later publication. It also verifies
that initialization preserves evidence bytes and supports migration of existing
root-owned artifacts and prior Airflow SQLite/log volumes. CI now runs this
shared-mount workflow rather than the old no-mount single-interval smoke.

The default UID 50000 local workflow passed against persisted metadata, producing
27, 54, and 54 order lines at reporting horizons January 1, January 2, and January
2 respectively. Each publication passed all 100 dbt nodes. The full UID 1000
Linux-bind round also exited 0: it migrated the previous round's actual SQLite
and log volumes, preserved raw evidence, executed the missing-first-delivery
failure, recovered both days to 54 lines, and retried January 1 without moving
the reporting clock back from January 2. The exact dbt documentation command in
the README also completed under UID 1000, generating its 16-model/84-test catalog.

Three independent recovery-helper guard checks rejected scheduled tasks, a live
backfill lease, and an unpaused DAG without changing interval/task/history rows.
Initializer checks refused unsafe symlinks/hardlinks without modifying external
targets. A second initialization performed zero ownership/mode writes, preserving
ctime, bytes, owner, and permissions. These checks used dedicated disposable
Linux mounts/databases, not the regular demo database. Logs remain ignored under
`data/audit_ops/bind-workflow-final.log`, `default-helper-recovery.log`, and
`recovery-guards.log`.

The documented `docker compose run --build --rm runner demo` command also exited
0 on the Windows Docker Desktop workspace with its existing bind-mounted
artifact history. Initial ownership migration completed; the healthy pipeline
published 189 order lines and 21 refunds, and its replay preserved counts/totals.

## Correctness demonstrations

Observed on 2026-11-02: 56 source/manifest unit checks passed, and all seven
PostgreSQL/dbt integration scenarios passed across directed runs after the
affected changes. The integration checks covered exact reference metrics and
dimension history; replay; late-refund publication deltas and full-rebuild
parity; additive/breaking schema handling and clean retry; three interruption
stages including rollback after the first view replacement; freshness and dbt
business-quality failures; date-moving corrections and removal of empty old
partitions; and multi-shard quarantine plus future-lifecycle rejection. These
checks ran against a separately created `commerce_test` database, not the demo
database. The committed suite runs all scenarios in CI.

`dbt docs generate` also completed against the observed demo publication and
produced its manifest/catalog: 16 models and 83 data tests were discovered.
Generated runtime artifacts remain ignored; the versioned model documentation
and the reproduction commands are committed.

After the first adversarial review, three additional accepted-horizon scenarios
and seven record-specific quarantine/database scenarios passed. They reproduce
and prevent future-data leakage after failed ingestion and same-partition
quarantine bypass. The matching identity unit check also passed. The affected
schema-drift and quarantine tests were rerun successfully. Review findings and
their disposition are recorded in [the review record](reviews/astra-review.md).

Run the source unit tests and the PostgreSQL/dbt integration suite against a disposable database:

```sh
python -m pytest -q
```

Set `COMMERCE_TEST_DATABASE_URL` before running integration checks. CI provisions its own disposable PostgreSQL service. Scenario assertions should establish these properties:

| Scenario | Required evidence |
| --- | --- |
| Reference answers | Exact fixture sales/refund/inventory/fulfillment answers match expected values |
| Replay | Second delivery produces the same fact/mart contents and totals |
| Late refund | Original sale-date net sales change once; no duplicate revenue; event-date refund remains visible |
| Schema drift | Compatible added field succeeds; incompatible type or schema is quarantined/rejected with evidence |
| Interrupted run | Failed run leaves last-good analytics intact; subsequent retry reconciles accepted source state |
| Backfill | Selected historical rebuild equals clean full rebuild from the same canonical source inputs |
| Dimension history | Earlier and later sales resolve to their respective effective category/region |
| Reconciliation | Atomic facts agree with marts; no orphan references, duplicate business grains, or refund fanout |

Each run retains manifest identifiers, source hashes, row counts, selected dates, and quality outcomes. Publication deltas need the changed source evidence as well as the numeric difference.

## Benchmark protocol

```sh
docker compose run --rm runner benchmark --small 100 --large 1000 --repeats 3 --output /workspace/data/benchmark.json
```

The two size arguments are orders per day over the generator's seven-day workload. The report must identify actual row counts, run repetitions, full-rebuild and incremental elapsed times, throughput, peak memory method, query latency, query plans/scan behavior, PostgreSQL/Python versions, hardware/runtime context, and measurement limitations. Preserve individual observations, not only averages. Startup and candidate-copy overhead belong in end-to-end comparisons.

Keep a small observed report in `docs/evidence/` only after it has been executed and inspected. Generated bulk source files remain ignored. A committed report should identify whether it ran natively, in Docker, or in another runtime, because container caps and filesystem behavior affect comparisons.

## Review and publication

Independent adversarial review checks the implemented behavior and evidence against the project goal. Review findings must be fixed and the updated implementation reviewed again before claiming a clean verdict. Private GitHub visibility, a matching pushed commit, and passing checks are separate final verification steps; a repository being created alone does not complete them.
