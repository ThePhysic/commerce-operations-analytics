-- Synthetic merchandise metrics. Sale-attributed refunds known at the latest good publication.
select sale_date, location_id,
    gross_sales_cents / 100.0 as gross_sales,
    discount_cents / 100.0 as discounts,
    refund_cents / 100.0 as attributable_refunds,
    net_sales_cents / 100.0 as net_sales,
    paid_orders, cancelled_orders
from analytics.mart_daily_location_sales
order by sale_date, location_id;
