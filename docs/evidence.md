# Reproducing correctness and performance evidence

All inputs are synthetic. Exact reference fixtures live under `fixtures/`; larger data is deterministically generated from a committed seed and workload parameters. The authoritative observed benchmark artifact is a JSON report produced by `commerce benchmark`, with its runtime context and limitations. Never replace an unexecuted scenario with a claimed successful result.

## Observed local batch run

On 2026-11-02, the Docker batch demo completed on the Windows development machine using Docker Desktop's Linux engine and the Compose PostgreSQL service. The deterministic seed-42, seven-day, 20-orders/day workload produced 140 orders, 189 order lines, and 21 refund events. The initial build passed its dbt publication gate. Replaying the same manifest accepted the duplicate delivery with zero canonical source changes and identical published fact/mart counts and sales totals. These are synthetic demo observations, not production throughput claims.

```sh
docker compose run --build --rm runner demo
```

## Observed Airflow execution

On the same machine, the pinned `apache/airflow:2.11.0-python3.12` image with the isolated commerce virtual environment built successfully. `scripts/stage_intervals.py` produced nine self-contained, checksum-verified daily deliveries for 2024-01-01 through 2024-01-09. The actual Airflow backfill below executed the DAG's finite BashOperator against a separate `commerce_schedule` analytical database, using temporary SQLite Airflow metadata:

```sh
airflow dags backfill commerce_batch --start-date 2024-01-01 --end-date 2024-01-01
```

The Docker invocation of `scripts/test_airflow.sh` exited 0. Run `374f050c0c3e4082913d76ab49dc1fb7` published as-of 2024-01-01: 38 source records received/accepted, zero rejected, 27 order-line facts, 20 order lifecycle facts, six inventory snapshots, and two date/location sales rows. All 99 dbt build/test nodes completed without failed, errored, or skipped nodes. Application run time in its audit ledger was 22.815 seconds; this excludes image download/build, Airflow initialization, and CLI startup. The container had a 2 GiB memory cap and 1.5 CPU limit, and PostgreSQL had its Compose 384 MiB cap.

The reproducible smoke harness parses the DAG, migrates its temporary metadata, runs the actual backfill, and asserts the warehouse publication and quality results. CI executes the same harness against a fresh service database. This observation establishes one daily scheduler execution; the complete nine-interval backfill is available through the README demonstration and is not claimed as part of this single-interval result. Detailed local logs/artifacts are ignored under `data/airflow-smoke.log` and `data/airflow-smoke/`.

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
