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

## Round 2: implementation findings resolved

Reviewed fix commit: `0574479e20525dffa141ad21383965f85700277c`.
The same independent reviewer repeated its probes against the original review
database, including migration of its legacy unresolved quarantine. It verified
earlier-date rejection for incremental/full rebuilds, safe default retries,
unrelated/older-version quarantine rejection, and audited matching resolution.
Its verdict was: **No remaining complaints about the implementation.**

The final benchmark is being recollected against these fixes. A final artifact
verdict will follow review of the refreshed evidence and documentation.

## Round 3: final artifact review

Reviewed final implementation and evidence commit:
`56a8d475ecb415c4a5b99625597851690ecc2e01`.

The reviewer inspected the subsequent strict JSON parser and quarantine changes,
the associated regression tests, refreshed benchmark observations, and final
scope/evidence documentation. It independently ran 70 source, ingestion, and
quarantine-identity unit checks; all passed. Earlier independent database probes
had already verified both P1 fixes against the original retained review evidence.

The reviewer recalculated runtime medians, peak client RSS, and query-latency
medians from the committed raw samples. They match the benchmark notes for both
938-line and 9,338-line workloads; all six recorded paired sales comparisons
passed. The notes clearly disclose that the narrow malformed/non-finite JSON
parser change followed those measurements. They do not claim an exact final
parser remeasurement, an incremental speedup, or production-scale evidence.

Final verdict: **No remaining complaints.** The implemented core, recovery fixes,
quality gates, tests, and documented evidence satisfy this review. Optional
streaming/cloud/distributed/dashboard extensions remain outside the core scope.
Private GitHub publication and remote CI verification remain separate final
workflow steps and are not represented as completed by this review.
