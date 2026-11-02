select
    payload ->> 'refund_id' as refund_id,
    payload ->> 'line_id' as line_id,
    (payload ->> 'refund_date')::date as refund_date,
    (payload ->> 'amount_cents')::bigint as amount_cents,
    version as source_version, raw_path, run_id as source_run_id,
    payload_hash, ingested_at
from {{ source('landing', 'records') }} where source = 'refunds'
