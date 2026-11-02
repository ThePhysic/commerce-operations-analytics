select line_id
from {{ ref('stg_order_lines') }}
where quantity <= 0 or unit_price_cents < 0 or discount_cents < 0
   or discount_cents > quantity * unit_price_cents
   or quantity is null or unit_price_cents is null or discount_cents is null
