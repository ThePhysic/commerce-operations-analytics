-- Daily order rates, with previous observed day for each location.
select sale_date, location_id, paid_orders, refunded_orders,
    refund_rate, cancellation_rate,
    refund_rate - lag(refund_rate) over (partition by location_id order by sale_date) as refund_rate_change,
    cancellation_rate - lag(cancellation_rate) over (partition by location_id order by sale_date) as cancellation_rate_change
from analytics.mart_refund_rate
order by sale_date, location_id;
