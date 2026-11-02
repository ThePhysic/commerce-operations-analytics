with totals as (
    select line_id, sum(amount_cents) as refund_cents,
        min(amount_cents) as minimum_amount, min(refund_date) as first_refund
    from {{ ref('stg_refunds') }} group by line_id
)
select r.line_id
from totals r
join {{ ref('stg_order_lines') }} l using (line_id)
where r.minimum_amount <= 0
   or r.refund_cents > l.quantity * l.unit_price_cents - l.discount_cents
   or r.first_refund < l.sale_date
   or l.status = 'cancelled'
