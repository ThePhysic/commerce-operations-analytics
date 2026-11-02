{{ config(pre_hook="{{ delete_reprocess_partition('sale_date') }}", indexes=[{'columns': ['refund_id'], 'unique': true}, {'columns': ['sale_date', 'line_id']}]) }}
select r.refund_id, r.line_id, l.order_id, l.location_id, l.product_id,
    l.location_key, l.product_key, l.sale_date, r.refund_date,
    r.amount_cents, r.source_version, r.raw_path, r.source_run_id,
    r.payload_hash, r.ingested_at
from {{ ref('stg_refunds') }} r
join {{ ref('fct_order_line') }} l using (line_id)
{{ selected_dates('l.sale_date') }}
