-- Event-date refund activity. Do not subtract this from sale-date net sales a second time.
select refund_date, location_id, refund_cents / 100.0 as refunded_amount,
    refund_events, refunded_orders
from analytics.mart_refunds_by_event_date
order by refund_date, location_id;
