with refunded as (
    select distinct order_id from {{ ref('fct_refund') }}
), expected as (
    select f.sale_date, f.location_id,
        count(*) filter (where f.status <> 'cancelled') as paid_orders,
        count(*) filter (where f.status = 'cancelled') as cancelled_orders,
        count(*) filter (where r.order_id is not null) as refunded_orders,
        count(*) filter (where r.order_id is not null)::numeric /
            nullif(count(*) filter (where f.status <> 'cancelled'), 0) as refund_rate,
        count(*) filter (where f.status = 'cancelled')::numeric / nullif(count(*), 0) as cancellation_rate
    from {{ ref('fct_fulfillment') }} f
    left join refunded r using (order_id)
    group by f.sale_date, f.location_id
)
select coalesce(e.sale_date, m.sale_date) as sale_date,
    coalesce(e.location_id, m.location_id) as location_id
from expected e full join {{ ref('mart_refund_rate') }} m using (sale_date, location_id)
where e.paid_orders is distinct from m.paid_orders
   or e.cancelled_orders is distinct from m.cancelled_orders
   or e.refunded_orders is distinct from m.refunded_orders
   or e.refund_rate is distinct from m.refund_rate
   or e.cancellation_rate is distinct from m.cancellation_rate
