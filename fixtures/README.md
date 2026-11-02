# Synthetic reference fixture

Everything here is fictional and contains no personal or customer data. Regenerate
the versioned dataset with `python fixtures/build_reference.py` after installing the
project. The small fixture is intentionally hand-calculated rather than generated
from warehouse SQL. `reference/expected.json` records its exact expected answers.

Five orders contain six lines. Order O001 has two lines and two partial refunds
against its first line; this catches refund/line join fanout. O002 is fully refunded.
O003 is cancelled and contributes zero sales. O004 is fulfilled after the product
category/location region change on January 4. O005 remains paid and contributes
sales but no fulfillment observation. Refunds on January 3/5 are attributed back to
the original January 1 sales; event-date refund totals remain separately queryable.

January 1 at L001: gross 5,500 cents - discounts 600 - refunds 1,500 = net 3,400.
All dates/locations: gross 9,000 - discounts 600 - refunds 1,500 = net 6,900 cents.
Fulfillment durations at L001 on January 1 are 60 and 120 minutes: median 90,
continuous p95 117. Inventory variance is actual minus expected; zero actual stock
defines a stockout. `build_reference.py` exports `reference_records()` for tests to
split the same inputs into initial and late-arrival manifests.

The full bundle contains explicit empty daily partitions for all three daily
sources from January 1 through the common January 5 reporting horizon. Empty files
prove that a feed was delivered with zero records rather than silently missing.
