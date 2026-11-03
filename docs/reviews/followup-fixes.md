# AI-assisted adversarial code review - follow-up fixes

The follow-up review of `5536f2c` identified seven cases missed by the original
review. This record tracks the corrections and historical validation. The
adversarial reviewer was GPT-6 Astra with medium reasoning effort: this is an
**AI-assisted adversarial code review**, not a third-party human audit, security
certification, or formal independent assessment. Conclusions are limited to the
reviewed revisions and inspected evidence. A prior clean review does not
establish correctness of subsequently changed paths.

| Finding | Correction | Regression evidence |
| --- | --- | --- |
| Linux shared-volume permissions | Unprivileged runner/Airflow identities; bounded initializer for owned artifact and metadata roots; existing-volume migration | Actual Linux mounts, UID 50000 and UID 1000, unchanged raw bytes, writable prior SQLite state, symlink/hardlink refusal |
| Empty reads in old snapshots | Same-connection publication selection and immutable physical-schema pinning; CLI queries use the reader API | Real writer publication overlaps an existing Repeatable Read reader; old/fresh/retained reports have the expected rows and attributes |
| Wrong inventory history | Resolve dimensions immediately before the following UTC midnight | Intraday and final-microsecond changes, next-midnight exclusion, incremental/backfill/full parity; independent dbt check |
| Additive partition-field collision | Select business dates by source contract rather than coalescing arbitrary payload fields | Inventory metadata named `sale_date` survives initial and catalog-triggered full rebuilds |
| Input rejection escapes quarantine | Durable file issues, safely encoded rejected JSON, PostgreSQL representation/range checks, separate file counters | Header/CSV/encoding/count failures, NUL/surrogate values, storage/arithmetic/key limits, clean siblings, persistent blockers and explicit disposition |
| Missing initial daily interval | Persist expected coverage before file access; only clean files certify dates; DAG depends on past success | Missing first manifest, blocked later publication, restored first interval, current-horizon recovery and historical retry |
| Conflicting version replay chooses a winner | Conflict-specific strictly-newer automatic recovery or audited operator disposition; original hashes preserve replay identity | Both conflict orders, all arrival permutations with a newer version, legacy issues, and numeric JSONB round trips |

During this follow-up, the complete native suite passed **142 tests in 555.36 seconds**. Successful
dbt builds contained 16 models and 84 data tests (100 nodes). AI-assisted
Astra medium core verification reran the inventory and scheduling regressions:
**3 passed in 105.36 seconds**. The initial core static/probe review
reported no further findings in its scope; the later revision-scoped conclusion
below also considered the completed operational checks and frozen artifacts.

Both operational UID rounds passed: default UID 50000 on dedicated Linux volumes,
then UID 1000 with an actual Linux bind mount and migration of the previous round's
real SQLite/log volumes. Each exercised the missing first day, corrected range
recovery, and an older-day retry after later publication. The final result kept
54 order lines and the January 2 reporting horizon; healthy publications passed
all 100 dbt nodes. Recovery refusal guards preserved metadata, and repeated
initialization performed zero permission writes. The documented dbt catalog
generation command succeeded as UID 1000.

The Linux workflow deliberately exercises failure and recovery with the pinned
Airflow 2.11 release. Native task clearing can leave an inactive blocked DagRun
occupying the active-run slot. The bounded recovery helper requires an audited
reason and refuses live work; it preserves execution records and releases only
the selected inactive interval range. It does not mark missing data successful or
weaken the warehouse coverage gate. See [operations](../operations.md) and
[reading publications](../reading-publications.md) for the supported interfaces.

Current benchmark observations and limits are in [benchmark notes](../evidence/benchmark-notes.md).
They include concurrent shared-server validation traffic and do not establish an
incremental speedup, isolated SQL operator throughput, or production capacity.

## Scoped AI-assisted review conclusion

Astra medium AI-assisted review of frozen implementation
`5259e5b93a093c6d79871ba1eaa9f8c7e8e39ffb` reported **no remaining complaints
within the inspected core project and seven follow-up fixes at that revision**.
This is a scoped review outcome, not a guarantee that no defects remain. The review covered
the final ingestion, publication-reader, coverage, warehouse, container,
scheduler/recovery, CI, and evidence paths. The AI reviewer ran the
three PostgreSQL/dbt inventory and scheduling regressions above and inspected the
completed Linux UID workflows and recovery-guard logs. The README recovery
command and historical/current evidence distinctions raised during review were
corrected and rechecked.

After the review, the Windows bind-mounted Docker demo also exited 0 with the
expected 189 order lines, 21 refunds, and identical replay results. The owner
explicitly requested retaining the repository's public visibility for
publication.

## Publication and CI history

The repository is public. The subsequent [Actions run 41554399790](https://github.com/ThePhysic/commerce-operations-analytics/actions/runs/41554399790)
completed successfully for both `pipeline` and `airflow` on public `main` commit
`499b8e4`, reviewed November 2, 2026 (Pacific). This historical result applies
only to that commit. The [current Actions workflow](https://github.com/ThePhysic/commerce-operations-analytics/actions/workflows/ci.yml)
is the source for later revisions' CI status.
