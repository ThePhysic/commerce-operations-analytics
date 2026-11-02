with sales as (
    select sale_date, location_id, sum(gross_sales_cents) as gross_cents,
        sum(applied_discount_cents) as discount_cents, count(*) as line_count
    from {{ ref('fct_order_line') }} group by sale_date, location_id
), refunds as (
    select sale_date, location_id, sum(amount_cents) as refund_cents
    from {{ ref('fct_refund') }} group by sale_date, location_id
), expected as (
    select s.*, coalesce(r.refund_cents, 0) as refund_cents
    from sales s left join refunds r using (sale_date, location_id)
)
select coalesce(e.sale_date, m.sale_date) as sale_date,
    coalesce(e.location_id, m.location_id) as location_id
from expected e full join {{ ref('mart_daily_location_sales') }} m using (sale_date, location_id)
where e.gross_cents is distinct from m.gross_sales_cents
   or e.discount_cents is distinct from m.discount_cents
   or e.refund_cents is distinct from m.refund_cents
   or e.gross_cents - e.discount_cents - e.refund_cents is distinct from m.net_sales_cents
   or e.line_count is distinct from m.order_line_count
