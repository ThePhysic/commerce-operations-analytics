# Commerce Operations Analytics Platform

A reproducible batch data platform for a **fictional commerce business**. Every business record is deterministically generated synthetic data. Python ingestion preserves original files, validates contracts, and reconciles source versions; dbt turns the accepted records into dimensional facts and SQL analytics. Airflow can schedule the same finite workflow.

The engineering focus is correctness over time: duplicate delivery, late refunds, changing business dimensions, schema drift, selected-date backfills, and recovery without partial publication. This is a local portfolio implementation; PostgreSQL serves as an analytical database, and the project makes no claims about real customer traffic or distributed warehouse experience.

## Ten-minute local demo

Install Docker with Compose v2 and Linux containers. The commands work in PowerShell and a POSIX shell from the repository root. First-time image/dependency downloads can take longer than ten minutes.

```sh
docker compose run --build --rm runner demo
```

This starts the dedicated PostgreSQL service, generates a small synthetic dataset, ingests it, builds/tests the dbt models, and publishes the analytics. The Airflow UI is optional and stays stopped during the batch demo. Generated source files and run artifacts are under the ignored `data/` directory; database state is in the `warehouse-data` named volume.

```sh
docker compose run --rm runner status
docker compose run --rm runner query --name sales
docker compose run --rm runner query --name refunds
docker compose run --rm runner query --name fulfillment
docker compose run --rm runner query --name inventory
docker compose run --rm runner query --name changes
```

Source and mart money is stored in integer cents; the example sales query formats those amounts in currency units by dividing by 100. `net_sales_cents = gross_sales_cents - discount_cents - refund_cents`; tax and tips are outside the synthetic source model. Refunds in sales marts are attributed to the original sale date. The separate event-date refund mart answers when a refund happened. Published schemas retain what was known at each successful publication, so a later correction can be traced to its source manifest.

Connect a SQL client to `localhost:55432`, database `commerce_analytics`, user `commerce`, password `synthetic-local-only`, or query inside the service:

```sh
docker compose exec postgres psql -U commerce -d commerce_analytics -c "select * from analytics.mart_daily_location_sales order by sale_date, location_id;"
```

The committed password is a deliberately public local synthetic-demo default. Ports bind to loopback. Use separate credentials and proper access controls before adapting this design for another environment.

Generate dbt's browsable model documentation and lineage from the published views:

```sh
docker compose run --rm -e DBT_TARGET_SCHEMA=analytics -e DBT_TARGET_PATH=/workspace/data/dbt-docs --entrypoint dbt runner docs generate --project-dir /app/warehouse --profiles-dir /app/warehouse
docker compose run --rm -p 127.0.0.1:8081:8080 -e DBT_TARGET_SCHEMA=analytics -e DBT_TARGET_PATH=/workspace/data/dbt-docs --entrypoint dbt runner docs serve --project-dir /app/warehouse --profiles-dir /app/warehouse --host 0.0.0.0 --port 8080 --no-browser
```

Open <http://localhost:8081> while the second command is running. These commands inspect the existing publication; they do not publish another dataset.

## Run and reprocess files

```sh
docker compose run --rm runner generate --output /workspace/data/incoming --days 7 --orders-per-day 20 --seed 42
docker compose run --rm runner run --manifest /workspace/data/incoming/manifest.json
docker compose run --rm runner run --manifest /workspace/data/incoming/manifest.json
docker compose run --rm runner run --manifest /workspace/data/incoming/manifest.json --start 2024-01-01 --end 2024-01-03
```

The second delivery is a replay: stable keys, source versions, and payload hashes keep the canonical facts from duplicating. A selected-date rebuild uses inclusive UTC dates. `--lookback-days` bounds routine reprocessing: a value of 7 includes the current as-of date plus seven prior dates, for eight inclusive dates. Use an explicit historical range for corrections outside that window. `--full-refresh` rebuilds the complete analytical result from retained canonical sources. A manifest declares each source file, expected rows, partition, schema version, and checksum; never edit a file without regenerating its manifest.

Without `--as-of`, a run uses the latest of its manifest partitions, the last successful reporting date, and the durable accepted source horizon. That horizon includes source-defined event/effective dates and order payment/fulfillment dates, including accepted data from failed runs. An explicit `--as-of` earlier than that durable horizon is rejected. Supply complete required daily feed deliveries through the resulting reporting horizon when recovering; retained publication schemas provide earlier published answers.

Contract failures preserve original payloads in quarantine with actionable reasons. Compatible extra fields are retained; required-field/type failures and unsupported positive schema versions are raw-landed and quarantined. Every unresolved rejected record blocks publication. A valid redelivery resolves only its matching source/business key at a version at least as recent as the rejected correction; unrelated clean rows or empty files cannot clear it. Rows without a recoverable key require an explicit audited operator decision. Inspect them with `commerce quarantine list` and record a disposition with `commerce quarantine resolve --id N --reason "producer-confirmed explanation"`. Resolved quarantine records and original raw bytes remain available. See [quarantine recovery](docs/quarantine.md) for commands, version rules, and audit details. A conflicting payload with the same business key and version is rejected rather than arbitrarily choosing a winner.

Fault injection stops a run after source ingestion, before publication, or inside the publication transaction:

```sh
docker compose run --rm runner run --manifest /workspace/data/incoming/manifest.json --fail-at after-ingest
docker compose run --rm runner run --manifest /workspace/data/incoming/manifest.json
docker compose run --rm runner run --manifest /workspace/data/incoming/manifest.json --fail-at before-publish
docker compose run --rm runner run --manifest /workspace/data/incoming/manifest.json --fail-at during-publish
docker compose run --rm runner status
```

The injected runs are expected to exit unsuccessfully. The `during-publish` case raises after the first view replacement and proves that the transaction rolls it back. Source ingestion remains auditable; analysts keep the last successful publication until a retry passes the quality gate. The test suite exercises late corrections, schema changes, interrupted runs, and backfill parity with deterministic assertions.

## Airflow scheduling and backfills

Airflow runs in its own image and profile. Its SQLite metadata is separate from the PostgreSQL analytical database. The local `SequentialExecutor` intentionally runs one task at a time; the application also serializes writers with a database advisory lock. Airflow's dependencies and dbt's dependencies live in separate Python environments.

```sh
docker compose run --rm runner generate --output /workspace/data/incoming --days 7 --orders-per-day 20 --seed 42
docker compose run --rm --entrypoint python runner /app/scripts/stage_intervals.py --manifest /workspace/data/incoming/manifest.json --output /workspace/data/scheduled
docker compose stop
docker compose -p commerce-schedule --profile airflow up --build -d postgres airflow
docker compose -p commerce-schedule exec airflow cat /opt/airflow/state/standalone_admin_password.txt
```

This uses a separate Compose project and fresh analytical/metadata volumes for the historical arrival-by-day demonstration. The earlier batch project is stopped to release the loopback ports. Reusing a database with a later published as-of date would correctly prevent moving its reporting clock backward.

Open <http://localhost:8080>, sign in as `admin` using that generated password, and inspect `commerce_batch`. DAGs start paused. The producer must place a valid source manifest at `data/scheduled/YYYY-MM-DD/manifest.json` **before** a run. The staging utility creates these deliveries, includes only catalog versions effective by each day, and emits explicit empty daily files when a feed has no events. The daily DAG processes the UTC date represented by its data interval; it does not run a streaming consumer. Request the historical backfill while keeping the daily schedule paused:

```sh
docker compose -p commerce-schedule exec airflow airflow dags backfill commerce_batch --start-date 2024-01-01 --end-date 2024-01-09
```

The task uses a seven-day lookback (the current date plus seven prior dates) so a refund arriving today can update its earlier sale date. The CLI remains available for selected-date historical reprocessing outside that window without starting Airflow. `catchup=False` avoids automatically creating years of historical runs. Airflow retries finite failed tasks once; publication still requires application and dbt quality checks. Unpause the current daily schedule only after supplying source deliveries for those dates; the historical fixture does not fabricate today's feed.

## Resource profiles

| Profile | Services | Container memory caps | Purpose |
| --- | --- | --- | --- |
| Batch | PostgreSQL + one runner | 384 MiB + 1536 MiB | Required pipeline, tests, queries, backfills |
| Airflow UI | PostgreSQL + Airflow | 384 MiB + 2048 MiB | Optional scheduler/UI; includes its task subprocess |

Docker/WSL overhead is additional. Allocate about 4 GiB to Docker for the batch demo; allow more when using the Airflow UI. Avoid running a separate batch runner concurrently with the Airflow profile on a constrained laptop. Start with the small sample and increase synthetic scale only after measuring. No broker, Spark cluster, frontend framework, or Kubernetes is required.

```sh
docker compose --profile airflow stop airflow
docker compose down
docker compose -p commerce-schedule --profile airflow down
```

Stopping containers preserves named volumes. Repeated publications preserve historical schemas, so disk usage grows with publication count; this is deliberate evidence retention rather than a production retention policy. To start an entirely separate test project, use a distinct Compose project name with `docker compose -p commerce-scratch ...`.

## Development and verification

Use Python 3.12 and a disposable PostgreSQL database. Dependencies are pinned in `requirements.lock` and `requirements-dev.lock`.

```sh
python -m venv .venv
python -m pip install -r requirements-dev.lock
python -m pip install --no-deps -e .
python -m pytest -q
```

Activate the virtual environment before the `pip` and `pytest` commands. Set `COMMERCE_TEST_DATABASE_URL` to a disposable test database to run the integration suite; those tests may recreate its project schemas. Quarantine regressions use a separate `commerce_quarantine_test` database and require `CREATE DATABASE`, or a precreated disposable database supplied through `COMMERCE_QUARANTINE_TEST_DATABASE_URL`. `COMMERCE_DATABASE_URL` configures CLI connections; the standard `PGHOST`, `PGPORT`, `PGUSER`, `PGPASSWORD`, and `PGDATABASE` variables are also supported. `COMMERCE_ROOT` identifies this repository's models/contracts/queries when running an installed package outside the root. `COMMERCE_DATA_DIR` chooses the generated-data directory.

CI runs the deterministic source and PostgreSQL/dbt integration tests, checks Compose configuration, builds the batch image, and independently builds/parses the Airflow DAG. Test and benchmark evidence are described in [the evidence guide](docs/evidence.md). A successful code build is not a substitute for the runtime correctness checks.

```sh
docker compose run --rm runner benchmark --small 100 --large 1000 --repeats 3 --output /workspace/data/benchmark.json
```

Benchmark workloads are generated, not committed in full. Compare full and incremental runtimes only within the recorded hardware/runtime context. The report includes the work performed by each path and does not claim end-to-end exactly-once delivery or unmeasured performance gains.

## Design and model reference

- [Architecture and publication protocol](docs/architecture.md)
- [Business definitions and data dictionary](docs/data-dictionary.md)
- [Design tradeoffs and operational limits](docs/tradeoffs.md)
- [Reproducible checks and evidence](docs/evidence.md)
- [Source contracts](contracts/) and [versioned example SQL](queries/)
- [dbt models, tests, and lineage](warehouse/)

The complete core is batch ingestion, validated modeling, incremental correction/backfill, quality gates, SQL consumption, and reproducible evidence. Streaming, cloud warehouses, distributed processing, CDC, and dashboards remain explicit future extensions.
