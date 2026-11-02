# Independent adversarial review

The requested reviewer is GPT-6 Astra with medium reasoning effort. Reviews cover
the core project requirements, implementation, tests, and observed evidence.
Streaming, cloud infrastructure, Spark, CDC, and dashboards remain optional.

## Round 1: changes required

Reviewed baseline: `0e2fc209f3e692583850f0407ca3ee19917b0657`.
The reviewer independently reproduced two P1 correctness defects in a disposable
PostgreSQL database:

1. A future refund accepted by a failed run could appear in a retry published
   under an earlier reporting date. The retry considered incoming partitions and
   the last publication, but not all durably accepted source state.
2. A rejected correction could be treated as resolved after an unrelated valid
   record arrived in the same source/date partition.

The fixes make the reporting date respect current canonical event/effective and
lifecycle dates, including accepted work from failed runs. An explicitly earlier
date fails. A failed request for a later date without future accepted data does
not advance that boundary.

Quarantine resolution now identifies the rejected source, business key, and
version. Unrelated records and older versions cannot clear it. Valid matching
redeliveries retain a resolution audit; unkeyed inputs require an explicit
operator disposition with a reason. Original payloads and raw objects remain.

Regression coverage includes failed future refunds, lifecycle/catalog dates,
incremental/full retries, freshness-gated recovery, harmless failed future-date
requests, unrelated same-partition records, version ordering, unsupported schema
redelivery, unkeyed inputs, legacy identity migration, and operator writer locks.

## Subsequent review

The revised implementation is awaiting the independent reviewer's updated
verdict. No clean verdict is claimed by this intermediate record.
