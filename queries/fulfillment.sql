-- Completed orders only enter duration percentiles. Pending orders are exposed separately.
select sale_date, location_id, fulfilled_orders, pending_orders,
    median_fulfillment_minutes, p95_fulfillment_minutes
from analytics.mart_fulfillment_sla
order by sale_date, location_id;
