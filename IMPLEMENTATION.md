# Technical implementation notes

The platform is a finite batch application: Python package `commerce`, the
PostgreSQL dbt project in `warehouse/`, JSON source contracts, deterministic
fixtures, Docker Compose, and optional local Airflow scheduling. All commerce
records are synthetic. See [architecture](docs/architecture.md) for the overall
design and [the warehouse dictionary](warehouse/README.md) for model definitions.

## Source interface and contracts

`generate(output: Path, *, days=7, orders_per_day=20, seed=42) -> Path` returns a
`manifest.json`. Its object contains `synthetic: true`, `seed`, and `files`, with
each file declaring `source`, relative `path`, `sha256`, `schema_version: 1`,
`partition` (`YYYY-MM-DD`), and `row_count`.

Every record has an integer source `version` of at least 1, bounded by PostgreSQL
integer storage. Money uses integer cents, dates use `YYYY-MM-DD`, and timestamps
are UTC ISO values with `Z` or `+00:00`. There is no PII. The authoritative
[contract files](contracts/) specify fields, storage limits, null policies,
cadence, ownership, and keys.

| Source | Format / business key | Fields beyond the key |
| --- | --- | --- |
| `order_lines` | CSV / `line_id` | `order_id`, `location_id`, `product_id`, `sale_date`, positive `quantity`, `unit_price_cents`, `discount_cents`, `status` (paid/fulfilled/cancelled), nullable `paid_at`/`fulfilled_at`, `version` |
| `refunds` | JSONL / `refund_id` | `line_id`, `refund_date`, positive `amount_cents`, `version` |
| `inventory` | JSONL / `snapshot_id` | `location_id`, `product_id`, `snapshot_date`, nonnegative `expected_quantity`/`actual_quantity`, `version`; unique location/product/day |
| `products` | JSONL / `product_id` + `effective_at` | `name`, `category`, `version` |
| `locations` | JSONL / `location_id` + `effective_at` | `name`, `region`, `version` |

Per-order status and lifecycle timestamps must agree across order lines.
Cancelled lines contribute zero sales. Compatible additive fields are retained;
missing/type-invalid required fields and unsupported positive schema versions
are quarantined with immutable raw evidence. File, record, and conflicting-version
issues all block publication while unresolved. A valid matching equal-or-newer
version can resolve an ordinary record issue; a conflict requires a strictly
newer version or an audited operator disposition. File/unkeyed failures require
an explicit disposition. See [quarantine recovery](docs/quarantine.md).

## Canonical storage and reporting horizon

Immutable raw files are content-addressed. `landing.records` holds the current
canonical source state with columns `source`, `business_key`, `version`,
`payload` (JSONB), `payload_hash`, `raw_path`, `run_id`, and `ingested_at`, unique
on `(source, business_key)`. Higher versions replace earlier canonical values;
different payloads claiming the same version are conflicts. Current records are
retained across runs. The staging views `stg_order_lines`, `stg_refunds`,
`stg_inventory`, `stg_products`, and `stg_locations` cast their source's JSON fields.

One PostgreSQL advisory lock serializes ingestion, build, and publication.
The default reporting horizon includes incoming partitions, the last successful
reporting date, and all durably accepted event/effective/lifecycle dates,
including accepted data from failed runs. An explicitly earlier `--as-of` fails.
Expected daily coverage is registered before scheduled manifest access; only
clean accepted deliveries certify coverage. Deterministic reporting-date
freshness and wall-clock source age are reported separately.

## Warehouse boundaries and grains

Each run builds in `build_<uuidhex>` using `DBT_TARGET_SCHEMA`, `COMMERCE_RUN_ID`,
and the standard `PGHOST`, `PGPORT`, `PGUSER`, `PGPASSWORD`, and `PGDATABASE`
profile settings. `meta.reprocess_dates(run_id, business_date)` selects the
inclusive date partitions to recompute. Incremental builds clone prior physical
tables, retaining rows and indexes, then delete and replace selected partitions;
deletion also removes stale output when the recomputed partition becomes empty.
Dimensions and the event-date refund summary rebuild in full. Catalog changes
trigger a full refresh so historical dimension attribution is recomputed.

| Model group | Grain / partition |
| --- | --- |
| `dim_product`, `dim_location` | Business ID/effective timestamp; type-2 half-open validity, `lead(effective_at)`, infinity for the current row, MD5 history surrogate |
| `fct_order_line` | Line ID / sale date |
| `fct_refund` | Refund ID / original sale date; retains refund event date |
| `fct_fulfillment` | Order ID / sale date |
| `fct_inventory_snapshot` | Location/product/day / snapshot date |
| Sales, refund-rate, fulfillment-SLA marts | Sale date/location |
| Inventory-variance mart | Snapshot date/location/product |
| `mart_refunds_by_event_date` | Refund date/location; full rebuild |

Sales use the paid timestamp for dimension history, or sale-day UTC midnight for
unpaid cancellations. Inventory uses the instant immediately before the next
UTC midnight, including changes during the snapshot day and excluding versions
effective exactly at the next midnight.

Gross is quantity * unit price excluding cancellations; discounts apply once per
line. Net subtracts discounts and refunds attributed to the sale date. Refunds
are aggregated per line before joining sales to prevent fanout. Tax and tips
are absent. Refund rate is distinct refunded orders / noncancelled orders;
cancellation rate is cancelled / all orders. Fulfillment median/p95 use
`percentile_cont`. Stockout means zero actual units; variance is actual minus
expected. Line, refund, and inventory facts retain source run, raw path, payload
hash, version, and ingestion timestamp. Order-level fulfillment retains arrays
of source runs and raw paths; its order key joins back to line facts.

## Quality gate and publication

`dbt build` must pass every model and data test in the candidate schema before
publication. One final database transaction replaces the stable `analytics`
views and records publication/source lineage. A failure rolls the switch back;
the last successful physical schema remains available. Retained publication
schemas and source manifests explain corrections and deltas.

Live `analytics` views follow the latest publication. A multi-query consumer
selects a publication and reads its physical schema on the same connection;
starting a transaction alone does not pin those live views. The CLI's named
queries use the [publication-reader interface](docs/reading-publications.md).

Audit metadata includes `meta.runs`, `meta.files`, `meta.quarantine`,
`meta.source_changes`, `meta.publications`, `meta.publication_sources`,
`meta.reprocess_dates`, and `meta.source_expectations`.

## Command-line interface

`commerce generate` accepts output, days, orders per day, and seed. `commerce run`
accepts a manifest, full refresh, explicit start/end dates, as-of date, lookback,
and expected-start coverage. Failure probes are `after-ingest`, `before-publish`,
and `during-publish`. Reader/operator commands are `status`, `publication`,
`query --name sales|refunds|fulfillment|inventory|refund_events|changes` (with optional
`--publication-id`), and `quarantine list|resolve`. `demo` and `benchmark` provide
reproducible synthetic workflows.

`COMMERCE_DATABASE_URL` or standard `PG*` variables configure the connection;
`COMMERCE_ROOT` locates repository resources and `COMMERCE_DATA_DIR` selects the
artifact directory. See [the command walkthrough](docs/local-demo.md) and
[operations](docs/operations.md) for examples and recovery procedures.
