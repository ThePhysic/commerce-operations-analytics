-- Observed snapshot frequency, not continuous availability or inferred lost sales.
select location_id, product_id,
    sum(stockout_count)::numeric / nullif(sum(observed_snapshot_count), 0) as stockout_frequency,
    sum(variance_quantity) as cumulative_observed_variance,
    avg(abs(variance_quantity)) as mean_absolute_variance,
    count(*) as observed_days
from analytics.mart_inventory_variance
group by location_id, product_id
order by stockout_frequency desc, mean_absolute_variance desc, location_id, product_id;
