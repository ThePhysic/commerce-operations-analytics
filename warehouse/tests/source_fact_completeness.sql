select 'order_lines' as source
where (select count(*) from {{ ref('stg_order_lines') }}) <> (select count(*) from {{ ref('fct_order_line') }})
union all
select 'refunds'
where (select count(*) from {{ ref('stg_refunds') }}) <> (select count(*) from {{ ref('fct_refund') }})
union all
select 'inventory'
where (select count(*) from {{ ref('stg_inventory') }}) <> (select count(*) from {{ ref('fct_inventory_snapshot') }})
