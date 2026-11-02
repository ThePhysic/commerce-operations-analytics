{{ config(pre_hook="{{ delete_reprocess_partition('snapshot_date') }}", indexes=[{'columns': ['snapshot_date', 'location_id', 'product_id'], 'unique': true}]) }}
select snapshot_date, location_id, product_id,
    expected_quantity, actual_quantity, variance_quantity,
    is_stockout, case when is_stockout then 1 else 0 end as stockout_count,
    1 as observed_snapshot_count
from {{ ref('fct_inventory_snapshot') }}
{{ selected_dates('snapshot_date') }}
