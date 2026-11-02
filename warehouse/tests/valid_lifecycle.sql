select line_id
from {{ ref('stg_order_lines') }}
where (status in ('paid', 'fulfilled') and paid_at is null)
   or (status = 'fulfilled' and fulfilled_at is null)
   or (status <> 'fulfilled' and fulfilled_at is not null)
   or fulfilled_at < paid_at
   or (paid_at at time zone 'UTC')::date <> sale_date
