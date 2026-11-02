{{ config(pre_hook="{{ delete_reprocess_partition('sale_date') }}", indexes=[{'columns': ['line_id'], 'unique': true}, {'columns': ['sale_date', 'location_id']}]) }}
with lines as (
    select * from {{ ref('stg_order_lines') }}
    {{ selected_dates('sale_date') }}
)
select l.*,
    p.product_key, d.location_key,
    case when l.status = 'cancelled' then 0 else l.quantity * l.unit_price_cents end as gross_sales_cents,
    case when l.status = 'cancelled' then 0 else l.discount_cents end as applied_discount_cents
from lines l
left join {{ ref('dim_product') }} p
    on l.product_id = p.product_id
    and coalesce(l.paid_at, l.sale_date::timestamp at time zone 'UTC') >= p.valid_from
    and coalesce(l.paid_at, l.sale_date::timestamp at time zone 'UTC') < p.valid_to
left join {{ ref('dim_location') }} d
    on l.location_id = d.location_id
    and coalesce(l.paid_at, l.sale_date::timestamp at time zone 'UTC') >= d.valid_from
    and coalesce(l.paid_at, l.sale_date::timestamp at time zone 'UTC') < d.valid_to
