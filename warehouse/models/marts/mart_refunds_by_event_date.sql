{{ config(materialized='table') }}
select refund_date, location_id, sum(amount_cents) as refund_cents,
    count(*) as refund_events, count(distinct order_id) as refunded_orders
from {{ ref('fct_refund') }}
group by refund_date, location_id
