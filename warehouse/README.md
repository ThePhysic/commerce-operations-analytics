# Warehouse models

All commerce data is synthetic. Models run in an isolated `build_<run_id>`
PostgreSQL schema; the Python runner invokes `dbt build` and publishes only
after every model and test succeeds. The `analytics` schema contains reader
views pointing to the latest complete publication. Old physical schemas
remain available for reporting-as-of inspection and publication comparisons.

The profile takes `PGHOST`, `PGPORT`, `PGDATABASE`, `PGUSER`, `PGPASSWORD`,
`DBT_TARGET_SCHEMA`, and `COMMERCE_RUN_ID` from the environment. Use the project
CLI for builds so selected partitions, the writer lock, quality gates, and
publication metadata are initialized together. Running `dbt build` directly
does not perform atomic publication.

## Model dictionary

| Model | Grain | Semantics |
|---|---|---|
| `stg_order_lines` | Line ID | Canonical highest source version; cancellations retained |
| `stg_refunds` | Refund ID | Partial refunds are separate events |
| `stg_inventory` | Snapshot ID; unique location/product/day | Observed expected and actual unit counts |
| `stg_products`, `stg_locations` | Business ID/effective timestamp | Effective-dated catalog versions |
| `dim_product`, `dim_location` | Business ID/valid_from | Type-2 history, half-open UTC validity intervals |
| `fct_order_line` | Line ID | Merchandise gross and discount cents; sale-date partition |
| `fct_refund` | Refund ID | Original sale-date attribution plus separate refund event date |
| `fct_fulfillment` | Order ID | All orders; elapsed duration only for fulfilled orders |
| `fct_inventory_snapshot` | Location/product/day | Actual minus expected variance; zero actual means stockout |
| `mart_daily_location_sales` | Sale date/location | Gross minus discounts minus attributable refunds |
| `mart_refund_rate` | Sale date/location | Refunded/noncancelled orders and cancelled/all orders |
| `mart_fulfillment_sla` | Sale date/location | Continuous median/p95 minutes over fulfilled orders |
| `mart_inventory_variance` | Snapshot date/location/product | Additive stockout/observation counts for frequency queries |
| `mart_refunds_by_event_date` | Refund date/location | Event-date refund amounts, without changing sale attribution |

Amounts use integer cents in facts. Sales marts aggregate integer values into
PostgreSQL `numeric`; the example queries divide by 100 for readable currency
units. Taxes and tips are absent. Discounts apply once per line, rather than
once per item. Cancelled lines contribute zero gross/discount amounts and
cannot have refunds. Cumulative refunds cannot exceed a line's paid amount.

Dimension lookups use the paid timestamp; unpaid cancellations use sale-day
UTC midnight. Inventory uses snapshot-day UTC midnight because feeds contain
dates rather than intraday observation timestamps. History intervals end at
the next effective timestamp, with infinity for the current version. The
reference dataset changes a product category and a location region on January
4 to show that earlier facts retain their historical labels.

Refund facts are aggregated per line before joining sales, preventing multiple
refund events from multiplying revenue. Fulfillment facts collapse lines to
orders; inconsistent per-order lifecycle fields fail uniqueness checks.
Pending/cancelled orders do not enter duration percentiles. A zero denominator
produces a null refund rate; missing fulfillment observations yield null
percentiles. Stockout frequency is the share of observed daily snapshots with
zero actual stock, rather than an estimate of continuous availability or lost
sales.

## Incremental correctness and provenance

The runner clones the previous publication's physical tables and records dates
in `meta.reprocess_dates` for the current `COMMERCE_RUN_ID`. Incremental facts
and marts delete those entire date partitions in a dbt transaction and insert
the recomputed partitions. Deletion is independent of the new query's rows,
so a correction that removes the last row from a partition cannot leave stale
mart output. Old and new sale dates must both be selected when a correction
moves a line. Refund changes reprocess their original sale dates. Catalog
changes trigger a full refresh so historical dimension keys are recomputed.

Dimensions and event-date refund summaries rebuild in full. Atomic publication
is implemented by the runner's final transaction, rather than by claiming
that a multi-model dbt build is itself a single transaction.

Facts retain the source ingestion run, immutable raw file path, payload hash,
source version and ingestion timestamp; order-level fulfillment stores arrays
of source runs and paths. Mart grain keys join back to facts. dbt `ref` and
`source` dependencies supply model lineage; generated manifests, catalog and
run-results artifacts are retained under the runtime artifact directory.

Schema tests cover required/unique fields, relationships and accepted values.
Singular tests cover line amounts, refund limits, event chronology, inventory
grain, dimension intervals, source/fact completeness, rates and reconciliation
of sales, refunds, fulfillment and inventory against facts. The runner uses
contract quarantine and source freshness/completeness gates before dbt, and
publishes only the last complete valid model set.
