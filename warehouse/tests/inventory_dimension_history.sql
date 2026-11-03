-- End-of-day observations must use the last catalog change on or before the
-- observation's UTC date. A valid foreign key alone cannot prove this.
select f.snapshot_id
from {{ ref('fct_inventory_snapshot') }} f
left join {{ ref('dim_product') }} p using (product_key)
left join {{ ref('dim_location') }} l using (location_key)
where p.product_id is distinct from f.product_id
   or l.location_id is distinct from f.location_id
   or p.valid_from is distinct from (
       select max(effective_at) from {{ ref('stg_products') }} s
       where s.product_id = f.product_id
         and (s.effective_at at time zone 'UTC')::date <= f.snapshot_date
   )
   or l.valid_from is distinct from (
       select max(effective_at) from {{ ref('stg_locations') }} s
       where s.location_id = f.location_id
         and (s.effective_at at time zone 'UTC')::date <= f.snapshot_date
   )
