select
    payload ->> 'line_id' as line_id,
    payload ->> 'order_id' as order_id,
    payload ->> 'location_id' as location_id,
    payload ->> 'product_id' as product_id,
    (payload ->> 'sale_date')::date as sale_date,
    (payload ->> 'quantity')::bigint as quantity,
    (payload ->> 'unit_price_cents')::bigint as unit_price_cents,
    (payload ->> 'discount_cents')::bigint as discount_cents,
    payload ->> 'status' as status,
    nullif(payload ->> 'paid_at', '')::timestamptz as paid_at,
    nullif(payload ->> 'fulfilled_at', '')::timestamptz as fulfilled_at,
    version as source_version, raw_path, run_id as source_run_id,
    payload_hash, ingested_at
from {{ source('landing', 'records') }} where source = 'order_lines'
