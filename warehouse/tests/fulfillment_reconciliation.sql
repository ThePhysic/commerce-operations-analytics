with expected as (
    select sale_date, location_id,
        count(*) filter (where status = 'fulfilled') as fulfilled_orders,
        percentile_cont(0.5) within group (order by fulfillment_minutes) filter (where status = 'fulfilled') as median_minutes,
        percentile_cont(0.95) within group (order by fulfillment_minutes) filter (where status = 'fulfilled') as p95_minutes
    from {{ ref('fct_fulfillment') }} group by sale_date, location_id
)
select coalesce(e.sale_date, m.sale_date) as sale_date, coalesce(e.location_id, m.location_id) as location_id
from expected e full join {{ ref('mart_fulfillment_sla') }} m using (sale_date, location_id)
where e.fulfilled_orders is distinct from m.fulfilled_orders
   or e.median_minutes is distinct from m.median_fulfillment_minutes
   or e.p95_minutes is distinct from m.p95_fulfillment_minutes
