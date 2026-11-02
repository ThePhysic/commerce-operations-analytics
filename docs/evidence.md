# Reproducing correctness and performance evidence

All inputs are synthetic. Exact reference fixtures live under `fixtures/`; larger data is deterministically generated from a committed seed and workload parameters. The authoritative observed benchmark artifact is a JSON report produced by `commerce benchmark`, with its runtime context and limitations. Never replace an unexecuted scenario with a claimed successful result.

## Correctness demonstrations

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
