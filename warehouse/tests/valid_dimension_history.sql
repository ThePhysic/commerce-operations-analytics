select 'product' as source, product_id as business_id
from {{ ref('dim_product') }} group by product_id
having count(*) filter (where is_current) <> 1 or bool_or(valid_to <= valid_from)
union all
select 'location', location_id from {{ ref('dim_location') }} group by location_id
having count(*) filter (where is_current) <> 1 or bool_or(valid_to <= valid_from)
