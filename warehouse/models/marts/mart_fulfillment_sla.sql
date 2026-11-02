{{ config(pre_hook="{{ delete_reprocess_partition('sale_date') }}", indexes=[{'columns': ['sale_date', 'location_id'], 'unique': true}]) }}
select sale_date, location_id,
    count(*) filter (where status = 'fulfilled') as fulfilled_orders,
    count(*) filter (where status = 'paid') as pending_orders,
    percentile_cont(0.5) within group (order by fulfillment_minutes) filter (where status = 'fulfilled') as median_fulfillment_minutes,
    percentile_cont(0.95) within group (order by fulfillment_minutes) filter (where status = 'fulfilled') as p95_fulfillment_minutes
from {{ ref('fct_fulfillment') }}
{{ selected_dates('sale_date') }}
group by sale_date, location_id
