{{ config(pre_hook="{{ delete_reprocess_partition('sale_date') }}", indexes=[{'columns': ['order_id'], 'unique': true}, {'columns': ['sale_date', 'location_id']}]) }}
select order_id, sale_date, location_id, location_key,
    status, paid_at, fulfilled_at,
    case when status = 'fulfilled' then extract(epoch from (fulfilled_at - paid_at)) / 60.0 end as fulfillment_minutes,
    array_agg(distinct source_run_id order by source_run_id) as source_run_ids,
    array_agg(distinct raw_path order by raw_path) as source_raw_paths
from {{ ref('fct_order_line') }}
{{ selected_dates('sale_date') }}
group by order_id, sale_date, location_id, location_key, status, paid_at, fulfilled_at
