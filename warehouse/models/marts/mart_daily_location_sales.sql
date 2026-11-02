{{ config(pre_hook="{{ delete_reprocess_partition('sale_date') }}", indexes=[{'columns': ['sale_date', 'location_id'], 'unique': true}]) }}
with refunds_by_line as (
    select line_id, sum(amount_cents) as refund_cents
    from {{ ref('fct_refund') }} group by line_id
)
select l.sale_date, l.location_id,
    sum(l.gross_sales_cents) as gross_sales_cents,
    sum(l.applied_discount_cents) as discount_cents,
    sum(coalesce(r.refund_cents, 0)) as refund_cents,
    sum(l.gross_sales_cents - l.applied_discount_cents - coalesce(r.refund_cents, 0)) as net_sales_cents,
    count(distinct l.order_id) filter (where l.status <> 'cancelled') as paid_orders,
    count(distinct l.order_id) filter (where l.status = 'cancelled') as cancelled_orders,
    count(*) as order_line_count
from {{ ref('fct_order_line') }} l
left join refunds_by_line r using (line_id)
{{ selected_dates('l.sale_date') }}
group by l.sale_date, l.location_id
