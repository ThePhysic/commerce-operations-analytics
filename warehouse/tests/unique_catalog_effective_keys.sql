select 'product' as source, product_id as business_id, effective_at
from {{ ref('stg_products') }} group by product_id, effective_at having count(*) > 1
union all
select 'location', location_id, effective_at
from {{ ref('stg_locations') }} group by location_id, effective_at having count(*) > 1
