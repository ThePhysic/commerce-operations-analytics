select
    payload ->> 'snapshot_id' as snapshot_id,
    payload ->> 'location_id' as location_id,
    payload ->> 'product_id' as product_id,
    (payload ->> 'snapshot_date')::date as snapshot_date,
    (payload ->> 'expected_quantity')::bigint as expected_quantity,
    (payload ->> 'actual_quantity')::bigint as actual_quantity,
    version as source_version, raw_path, run_id as source_run_id,
    payload_hash, ingested_at
from {{ source('landing', 'records') }} where source = 'inventory'
