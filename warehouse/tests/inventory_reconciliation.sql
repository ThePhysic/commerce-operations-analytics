select coalesce(f.snapshot_date, m.snapshot_date) as snapshot_date,
    coalesce(f.location_id, m.location_id) as location_id,
    coalesce(f.product_id, m.product_id) as product_id
from {{ ref('fct_inventory_snapshot') }} f
full join {{ ref('mart_inventory_variance') }} m using (snapshot_date, location_id, product_id)
where f.expected_quantity is distinct from m.expected_quantity
   or f.actual_quantity is distinct from m.actual_quantity
   or f.actual_quantity - f.expected_quantity is distinct from m.variance_quantity
   or f.is_stockout is distinct from m.is_stockout
