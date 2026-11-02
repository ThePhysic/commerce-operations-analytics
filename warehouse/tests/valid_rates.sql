select sale_date, location_id from {{ ref('mart_refund_rate') }}
where refund_rate < 0 or refund_rate > 1 or cancellation_rate < 0 or cancellation_rate > 1
   or refunded_orders > paid_orders
