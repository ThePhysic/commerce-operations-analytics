{{ config(pre_hook="{{ delete_reprocess_partition('snapshot_date') }}", indexes=[{'columns': ['snapshot_id'], 'unique': true}, {'columns': ['snapshot_date', 'location_id', 'product_id']}]) }}
with snapshots as (
    select * from {{ ref('stg_inventory') }}
    {{ selected_dates('snapshot_date') }}
)
select s.*, p.product_key, l.location_key,
    s.actual_quantity - s.expected_quantity as variance_quantity,
    s.actual_quantity = 0 as is_stockout
from snapshots s
left join {{ ref('dim_product') }} p
    on s.product_id = p.product_id
    and (s.snapshot_date::timestamp at time zone 'UTC') >= p.valid_from
    and (s.snapshot_date::timestamp at time zone 'UTC') < p.valid_to
left join {{ ref('dim_location') }} l
    on s.location_id = l.location_id
    and (s.snapshot_date::timestamp at time zone 'UTC') >= l.valid_from
    and (s.snapshot_date::timestamp at time zone 'UTC') < l.valid_to
