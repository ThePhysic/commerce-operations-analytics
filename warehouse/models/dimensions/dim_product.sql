with history as (
    select *,
        lead(effective_at, 1, 'infinity'::timestamptz) over (
            partition by product_id order by effective_at
        ) as valid_to
    from {{ ref('stg_products') }}
)
select {{ history_key('product_id', 'effective_at') }} as product_key,
    product_id, product_name, category,
    effective_at as valid_from, valid_to,
    valid_to = 'infinity'::timestamptz as is_current,
    source_version, raw_path, source_run_id, payload_hash, ingested_at
from history
