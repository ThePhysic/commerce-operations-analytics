# AI-assisted adversarial code review - historical record

These historical reviews used GPT-6 Astra with medium reasoning effort. This is
an **AI-assisted adversarial code review**, not a third-party human audit,
security certification, or formal independent assessment. Conclusions apply only
to the named revisions, requirements, probes, and evidence inspected. Later
findings are recorded in the [follow-up review](followup-fixes.md).

The review scope was the finite batch platform, tests, recovery, and observed
evidence. Check the [current Actions workflow](https://github.com/ThePhysic/commerce-operations-analytics/actions/workflows/ci.yml)
for revision-specific CI; this historical record does not establish later status.

## Round 1: changes required

Reviewed baseline: `0e2fc209f3e692583850f0407ca3ee19917b0657`.
The AI reviewer reproduced two P1 correctness defects in a disposable
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
The same AI reviewer repeated its probes against the original review
database, including migration of its legacy unresolved quarantine. It verified
earlier-date rejection for incremental/full rebuilds, safe default retries,
unrelated/older-version quarantine rejection, and audited matching resolution.
Its scoped conclusion at that commit was **no remaining implementation complaints
within the inspected paths and probes**.

At this historical stage, refreshed benchmark evidence still awaited review.
That subsequent artifact review is recorded in Round 3 below.

## Round 3: artifact review at the named revision

Reviewed implementation and evidence commit:
`56a8d475ecb415c4a5b99625597851690ecc2e01`.

The reviewer inspected the subsequent strict JSON parser and quarantine changes,
the associated regression tests, refreshed benchmark observations, and final
scope/evidence documentation. It ran 70 source, ingestion, and
quarantine-identity unit checks; all passed. Earlier AI-executed database probes
had already verified both P1 fixes against the original retained review evidence.

The reviewer recalculated runtime medians, peak client RSS, and query-latency
medians from the committed raw samples. They match the [benchmark notes at that reviewed revision](https://github.com/ThePhysic/commerce-operations-analytics/blob/56a8d475ecb415c4a5b99625597851690ecc2e01/docs/evidence/benchmark-notes.md) for both
938-line and 9,338-line workloads; all six recorded paired sales comparisons
passed. The notes clearly disclose that the narrow malformed/non-finite JSON
parser change followed those measurements. They do not claim an exact final
parser remeasurement, an incremental speedup, or production-scale evidence.

Scoped conclusion at `56a8d475ecb415c4a5b99625597851690ecc2e01`: **no remaining
complaints within that review's scope and evidence**. This does not certify the
absence of defects or extend the conclusion to later revisions; the follow-up
review subsequently identified additional cases.

This review preceded publication. The repository is now public. Historical CI
for public `main` at `499b8e4` is recorded in the [follow-up record](followup-fixes.md#publication-and-ci-history);
use the current Actions workflow for later commits.
