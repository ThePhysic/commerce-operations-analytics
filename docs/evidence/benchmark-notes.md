# Observed local batch benchmark

The complete machine-readable observations are in [benchmark.json](benchmark.json).
All business records are deterministic synthetic data with seed 42. Measurements
began at 2026-11-03 00:15:41 UTC. The native client ran on Windows 11 with Python
3.12.15, 16 logical CPUs and 31.21 GiB of host memory. PostgreSQL 16.10 ran through
the project's local Docker service, capped at one CPU and 384 MiB. The native
Python/dbt client did not run inside the runner container's memory or CPU cap.

This is a fresh collection after functional revision `1bd5884`, including durable
file blockers, safe Unicode and storage-range rejection, strict conflict
recovery, explicit daily coverage, and the inventory end-of-day dimension rule.
The benchmark, ingestion, pipeline, contract, coverage, and warehouse files
matched that revision throughout collection. The JSON records the full revision
and benchmark program hash. Valid generated inputs exercise the normal pipeline;
fault-recovery behavior is verified separately by the integration suite. The
benchmark queries analytical views directly and does not measure the consumer
read API's publication-pinning path.

Collection overlapped integration and Airflow validation on the same local
PostgreSQL service and workstation. Background load was neither isolated nor
constant. These observations document the current run under that context; they
do not establish isolated performance or the runtime effect of individual fixes.

The recorded command was:

```powershell
.venv\Scripts\commerce.exe benchmark --small 100 --large 1000 --repeats 3 --output docs/evidence/benchmark.json
```

The command used `COMMERCE_DATABASE_URL` for the local Compose PostgreSQL service.
It created a uniquely named disposable benchmark database, reset its schemas
between workload sizes, and dropped that database on completion. It preserved the
regular demo database. Removal of the disposable database was verified after
collection. All measured sample values are retained unchanged.

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
and backfill parity checks belong to the integration suite. The report was checked
for six successful comparisons, twelve measured plans, and 120 latency
observations. Actual fact counts were 938/9,338 order lines, 91/910 refunds,
700/7,000 order-level fulfillment rows, and 42 inventory snapshots per workload.

## Observations

Runtime is the median of three measured runs. Client memory is the largest
25 ms sampled sum of Python/dbt process-tree RSS across those runs, in MiB. Query
latency is the median of all 30 client-roundtrip observations per variant.

| Workload | Order lines | Incremental selected lines | Incremental runtime | Full runtime | Incremental peak RSS | Full peak RSS | Incremental query | Full query |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| 100 orders/day | 938 | 134 | 18.13 s | 16.94 s | 176.43 MiB | 175.61 MiB | 2.148 ms | 2.278 ms |
| 1,000 orders/day | 9,338 | 1,334 | 33.16 s | 22.51 s | 188.16 MiB | 188.10 MiB | 2.478 ms | 2.141 ms |

Median selected-order-line throughput over the entire pipeline was 7.39 and
40.23 lines/sec for incremental runs, versus 55.36 and 414.85 lines/sec for full
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
index scan feeding a bitmap heap scan in all repetitions. Heap scans returned 134 of
938 lines in the small workload and 1,334 of 9,338 lines in the larger workload.
Small-workload heap scans recorded 16–23 shared-hit blocks incrementally versus
9–15 after full rebuilds. Larger incremental heap scans recorded 146 shared-hit
blocks versus 74 after full rebuilds. All recorded heap scans had zero shared-read
blocks, so these observations characterize warm-cache access.

PostgreSQL/server memory is excluded from the client RSS figures. Summed process
RSS can count shared pages more than once, and sampling can miss short peaks.
Other workstation processes and the concurrent validation sharing PostgreSQL were
not isolated. Incremental always precedes full
within a pair, introducing sequence/cache bias. Three repetitions and modest
local synthetic datasets do not support statistical significance, production
capacity, cloud-warehouse performance, or distributed-scaling claims. The JSON
retains individual timings, query plans, row counts, hardware context, and these
limitations for inspection and reruns.
