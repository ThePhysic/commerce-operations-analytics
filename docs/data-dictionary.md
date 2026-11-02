# Business definitions and data dictionary

All business records are synthetic. Amounts are integer cents and timestamps are UTC. Dates identify business-day partitions in UTC; no regional daylight-saving conversion is implied.

## Source contracts

Each JSON document in `contracts/` defines its owner/producer, cadence, required fields, types, null policy, schema version, business key, and field meanings. Every row has a monotonically increasing positive integer `version` per business key. An identical key/version/payload is a replay. A higher version replaces that canonical record. A conflicting payload at the same version is an actionable conflict. Additive fields are retained in the raw payload; breaking types and unsupported schema versions require explicit remediation.

| Source | Format | Business key | Grain |
| --- | --- | --- | --- |
| `order_lines` | CSV | `line_id` | One line of a synthetic order, including its current lifecycle state |
| `refunds` | JSONL | `refund_id` | One attributable refund event against an order line |
| `inventory` | JSONL | `snapshot_id` | One location/product/day inventory observation |
| `products` | JSONL | `product_id`, `effective_at` | One effective product description/category version |
| `locations` | JSONL | `location_id`, `effective_at` | One effective location description/region version |

The file manifest records the path, source, checksum, schema version, partition, and expected row count. Record provenance links accepted warehouse rows back to the source run and immutable raw file.

## Dimensions and facts

| Model | Grain and keys | Interpretation |
| --- | --- | --- |
| `dim_product` | One product effective interval; history surrogate key | Historical product category/name, with effective start/end and current indicator |
| `dim_location` | One location effective interval; history surrogate key | Historical location region/name, with effective start/end and current indicator |
| `fct_order_line` | One `line_id`, partitioned by `sale_date` | Quantity, unit price, line discount, lifecycle, gross and discounted sales, historical dimension references |
| `fct_refund` | One `refund_id`, partitioned by original `sale_date` | Refund amount and event `refund_date`, joined to its original line |
| `fct_fulfillment` | One `order_id`, partitioned by `sale_date` | Paid-to-fulfilled elapsed seconds and location |
| `fct_inventory_snapshot` | One location/product/snapshot date | Actual and expected quantity, signed variance, stockout indicator |

History intervals are half-open: `valid_from <= event_time < valid_to`; the latest interval uses PostgreSQL's timestamp `infinity` as its end. Historical sales retain the dimension description effective at the sale. A changed current catalog value does not silently rewrite the earlier attribute interval. A correction to a historical interval requires rebuilding the affected facts.

## Metric definitions

| Metric | Definition |
| --- | --- |
| Gross sales | Sum of `quantity * unit_price_cents` on noncancelled lines |
| Applied discounts | Sum of line-level `discount_cents` on noncancelled lines; discount is not multiplied by quantity |
| Refund amount | Sum of attributable refund event cents, after deduplication by refund ID/version |
| Net sales | Gross sales minus applied discounts minus attributable refunds; tax/tips are absent |
| Refund rate | Distinct noncancelled orders with at least one refund divided by distinct noncancelled orders |
| Cancellation rate | Distinct cancelled orders divided by all distinct orders |
| Fulfillment latency | `fulfilled_at - paid_at` for fulfilled orders; incomplete/cancelled orders do not enter latency percentiles |
| Median / p95 | PostgreSQL continuous percentiles of eligible order latency, grouped by sale date and location |
| Inventory variance | `actual_quantity - expected_quantity`; negative means fewer observed units |
| Stockout frequency | Count of zero-actual snapshots divided by all eligible snapshots for the grouping |

Refunds cannot exceed a line's discounted merchandise amount, cannot precede its sale, and cannot attach to cancelled or unknown lines. A fulfilled order must have valid paid/fulfilled timestamps in sequence. Multiple lines of an order must agree on its lifecycle and location. Null or invalid identifiers must never become successful dimensional joins.

## Marts and time semantics

| Model | Grain | Questions |
| --- | --- | --- |
| `mart_daily_location_sales` | Sale date/location | Gross, discounts, attributable refunds, net sales |
| `mart_refund_rate` | Sale date/location | Refund/cancellation rates and their denominators |
| `mart_fulfillment_sla` | Sale date/location | Eligible order count, median and p95 latency |
| `mart_inventory_variance` | Snapshot date/location/product | Actual/expected inventory, variance, stockout indicator/frequency |
| `mart_refunds_by_event_date` | Refund event date/location | Refund cash activity on the day it occurred |

Sales-date attribution and refund-event reporting answer different questions. A refund delivered on January 5 for a January 1 sale adjusts the January 1 sales mart and appears in the January 5 event-date refund mart. The original publication remains evidence of the answer before that delivery. Published history describes what the pipeline knew when it published; it is not a promise that every source's business-event timestamp was available in real time.

## Diagnostics

Run diagnostics expose rows received/accepted/rejected, timing, error category, selected window, produced publication, manifests, and test outcomes. Freshness must be interpreted with its declared as-of basis. The static synthetic fixture is intentionally historical, so wall-clock freshness alone is expected to mark it old. Publication comparisons should include the source-manifest changes that explain a metric delta, including late corrections.
