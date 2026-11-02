select
    payload ->> 'product_id' as product_id,
    payload ->> 'name' as product_name,
    payload ->> 'category' as category,
    (payload ->> 'effective_at')::timestamptz as effective_at,
    version as source_version, raw_path, run_id as source_run_id,
    payload_hash, ingested_at
from {{ source('landing', 'records') }} where source = 'products'
