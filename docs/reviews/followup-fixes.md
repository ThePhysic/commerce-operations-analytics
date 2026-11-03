# Follow-up audit fixes

The follow-up audit of `5536f2c` identified seven cases missed by the original
review. This record tracks the corrections and fresh validation. A prior clean
review is not used as evidence for these newly changed paths.

| Finding | Correction | Regression evidence |
| --- | --- | --- |
| Linux shared-volume permissions | Unprivileged runner/Airflow identities; bounded initializer for owned artifact and metadata roots; existing-volume migration | Actual Linux mounts, UID 50000 and UID 1000, unchanged raw bytes, writable prior SQLite state, symlink/hardlink refusal |
| Empty reads in old snapshots | Same-connection publication selection and immutable physical-schema pinning; CLI queries use the reader API | Real writer publication overlaps an existing Repeatable Read reader; old/fresh/retained reports have the expected rows and attributes |
| Wrong inventory history | Resolve dimensions immediately before the following UTC midnight | Intraday and final-microsecond changes, next-midnight exclusion, incremental/backfill/full parity; independent dbt check |
| Additive partition-field collision | Select business dates by source contract rather than coalescing arbitrary payload fields | Inventory metadata named `sale_date` survives initial and catalog-triggered full rebuilds |
| Input rejection escapes quarantine | Durable file issues, safely encoded rejected JSON, PostgreSQL representation/range checks, separate file counters | Header/CSV/encoding/count failures, NUL/surrogate values, storage/arithmetic/key limits, clean siblings, persistent blockers and explicit disposition |
| Missing initial daily interval | Persist expected coverage before file access; only clean files certify dates; DAG depends on past success | Missing first manifest, blocked later publication, restored first interval, current-horizon recovery and historical retry |
| Conflicting version replay chooses a winner | Conflict-specific strictly-newer automatic recovery or audited operator disposition; original hashes preserve replay identity | Both conflict orders, all arrival permutations with a newer version, legacy issues, and numeric JSONB round trips |

The complete native suite passed **142 tests in 555.36 seconds**. Successful
current dbt builds contain 16 models and 84 data tests (100 nodes). Independent
Astra medium core verification reran the inventory and scheduling regressions:
**3 passed in 105.36 seconds**. Core static/probe review reported no remaining
complaints; the final whole-project verdict follows the completed operational
checks and frozen artifact review.

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

## Final independent verdict

Pending final operational results and review of the frozen implementation.
