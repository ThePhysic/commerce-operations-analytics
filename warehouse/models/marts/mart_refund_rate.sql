{{ config(pre_hook="{{ delete_reprocess_partition('sale_date') }}", indexes=[{'columns': ['sale_date', 'location_id'], 'unique': true}]) }}
with refunded_orders as (
    select distinct order_id from {{ ref('fct_refund') }}
)
select f.sale_date, f.location_id,
    count(*) filter (where f.status <> 'cancelled') as paid_orders,
    count(*) filter (where f.status = 'cancelled') as cancelled_orders,
    count(*) filter (where r.order_id is not null) as refunded_orders,
    count(*) filter (where r.order_id is not null)::numeric /
        nullif(count(*) filter (where f.status <> 'cancelled'), 0) as refund_rate,
    count(*) filter (where f.status = 'cancelled')::numeric / nullif(count(*), 0) as cancellation_rate
from {{ ref('fct_fulfillment') }} f
left join refunded_orders r using (order_id)
{{ selected_dates('f.sale_date') }}
group by f.sale_date, f.location_id
