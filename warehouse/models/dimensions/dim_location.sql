with history as (
    select *,
        lead(effective_at, 1, 'infinity'::timestamptz) over (
            partition by location_id order by effective_at
        ) as valid_to
    from {{ ref('stg_locations') }}
)
select {{ history_key('location_id', 'effective_at') }} as location_key,
    location_id, location_name, region,
    effective_at as valid_from, valid_to,
    valid_to = 'infinity'::timestamptz as is_current,
    source_version, raw_path, source_run_id, payload_hash, ingested_at
from history
