# Observed local batch benchmark

The complete machine-readable observations are in [benchmark.json](benchmark.json).
All business records are deterministic synthetic data with seed 42. Measurements
began at 2026-11-02 23:04:54 UTC. The native client ran on Windows 11 with Python
3.12.15, 16 logical CPUs and 31.21 GiB of host memory. PostgreSQL 16.10 ran through
the project's local Docker service, capped at one CPU and 384 MiB. The native
Python/dbt client did not run inside the runner container's memory or CPU cap.

The recorded command was:

```powershell
.venv\Scripts\python.exe -m commerce.cli benchmark --small 100 --large 1000 --repeats 3 --output docs/evidence/benchmark.json
```

The command used `COMMERCE_DATABASE_URL` for the local Compose PostgreSQL service.
It created a uniquely named disposable benchmark database, reset its schemas
between workload sizes, and dropped that database on completion. It preserved the
regular demo database. A separate 10/20-orders-per-day, one-repeat smoke benchmark
completed successfully before the recorded run.

## Method

Each workload contains seven sale days, five source feeds in CSV and JSONL, and
explicit empty daily feed deliveries through the late-refund reporting horizon.
After an unmeasured baseline load, each of three repetitions applies a versioned
unit-price correction to every line in the final sale partition (January 7).
The timed incremental run reprocesses that partition. The following timed full
run rebuilds from the same resulting canonical source inputs. The incremental
ingestion writes new source versions; the full run replays the same manifest and
performs no source updates. Thus these are end-to-end pipeline timings with
different source update work, rather than isolated transformation timings.

Each pair compares the saved daily-location sales snapshots, including gross,
discounts, refunds, and net sales. All six comparisons passed. The report's
`sales_totals_identical` field describes this assertion; broader reference-answer
and backfill parity checks belong to the integration suite. The assertion label
was clarified after collection without changing any observed measurements.

## Observations

Runtime is the median of three measured runs. Client memory is the largest
25 ms sampled sum of Python/dbt process-tree RSS across those runs, in MiB. Query
latency is the median of all 30 client-roundtrip observations per variant.

| Workload | Order lines | Incremental selected lines | Incremental runtime | Full runtime | Incremental peak RSS | Full peak RSS | Incremental query | Full query |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| 100 orders/day | 938 | 134 | 21.00 s | 18.67 s | 175.97 MiB | 175.88 MiB | 1.914 ms | 1.624 ms |
| 1,000 orders/day | 9,338 | 1,334 | 34.98 s | 19.29 s | 187.20 MiB | 186.31 MiB | 2.273 ms | 2.473 ms |

Median selected-order-line throughput over the entire pipeline was 6.38 and
38.14 lines/sec for incremental runs, versus 50.23 and 484.07 lines/sec for full
runs. The incremental numerator is one sale partition; the full numerator is all
seven sale partitions. These values do not measure total tuples scanned or raw
SQL operator throughput.

The measured incremental run took longer for both workloads. Candidate isolation
copies all prior physical tables and preserves their indexes, and the complete
dbt test suite still runs. The different source-update work also remains inside
the timed interval. The measurements establish no runtime speedup and do not
isolate the cost of each contributing stage.

## Query plans and limits

The query sums gross sales for the final sale date. Each incremental/full
publication is analyzed before ten latency observations and an
`EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON)` observation. Both variants used a bitmap
index scan feeding a bitmap heap scan in all repetitions. Scans returned 134 of
938 lines in the small workload and 1,334 of 9,338 lines in the larger workload.
Small-workload heap scans recorded 16 shared-hit blocks incrementally versus 9
after full rebuilds. Larger incremental heap scans recorded 76–146 shared-hit
blocks versus 74 after full rebuilds. All recorded heap scans had zero shared-read
blocks, so these observations characterize warm-cache access.

PostgreSQL/server memory is excluded from the client RSS figures. Summed process
RSS can count shared pages more than once, and sampling can miss short peaks.
Other workstation processes were not isolated. Incremental always precedes full
within a pair, introducing sequence/cache bias. Three repetitions and modest
local synthetic datasets do not support statistical significance, production
capacity, cloud-warehouse performance, or distributed-scaling claims. The JSON
retains individual timings, query plans, row counts, hardware context, and these
limitations for inspection and reruns.
