select snapshot_id from {{ ref('stg_inventory') }}
where expected_quantity < 0 or actual_quantity < 0
   or expected_quantity is null or actual_quantity is null
union all
select min(snapshot_id) from {{ ref('stg_inventory') }}
group by snapshot_date, location_id, product_id having count(*) <> 1
