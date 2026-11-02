select
    payload ->> 'location_id' as location_id,
    payload ->> 'name' as location_name,
    payload ->> 'region' as region,
    (payload ->> 'effective_at')::timestamptz as effective_at,
    version as source_version, raw_path, run_id as source_run_id,
    payload_hash, ingested_at
from {{ source('landing', 'records') }} where source = 'locations'
