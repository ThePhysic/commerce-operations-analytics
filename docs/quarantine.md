# Record-specific quarantine recovery

Every unresolved rejected record blocks the next publication. This gate applies
across input files, partitions, and runs. A clean row from another order on the
same date does not resolve the rejected correction, and a later empty partition
does not replace its evidence.

## Inspect failures

```sh
docker compose run --rm runner quarantine list
docker compose run --rm runner quarantine list --all
docker compose run --rm runner status
```

The list includes the source, recovered business key and rejected version, raw
path, original failure reason, and resolution audit when present. `status` exposes
`unresolved_quarantine_count`. The immutable raw object and retained quarantine
payload provide the original input even after resolution.

## Redeliver a matching correction

Correct the producer data, regenerate its checksummed manifest, and run the
pipeline again. Validation independently recovers a rejected row's key/version
when its non-key fields are invalid. An unsupported schema version can still have
a readable known business key; its later supported-schema redelivery follows the
same resolution rules.

A valid accepted row resolves outstanding quarantines only for its own source and
normalized business key. Its version must be greater than or equal to the rejected
version. A valid version 1 replay cannot dismiss a rejected version 2 correction.
If a rejected version itself was malformed, a valid redelivery for the same key
can resolve it. Canonical version/conflict processing must succeed first. The
resolution stores its timestamp, the accepting pipeline run ID, and a reason.
Higher-version canonical corrections preserve source-change history as usual.

Successful matching redelivery is required for each affected key. For example,
an invalid `O004_1` correction and valid `O005_1` on the same day remain separate
records. Redelivering `O005_1` unchanged cannot clear `O004_1`.

## Resolve an unkeyed or intentionally discarded input

Malformed JSON or an unreadable/missing key cannot be matched safely. Review the
retained raw bytes and obtain the producer's intended disposition, then explicitly
resolve the listed quarantine ID with a nonempty reason. Replace the example ID
with the ID returned by the list command:

```sh
docker compose run --rm runner quarantine resolve --id 17 --reason "Producer confirmed truncated duplicate; retain the existing canonical refund"
```

This records an operator disposition; it does not repair or load the rejected
payload. Keyed failures can also be explicitly discarded after review. No row is
matched by file position. The command holds the same database advisory writer
lock as the pipeline and updates the quarantine plus its audit run in one
transaction. The `operator-resolved` audit run is excluded from the pipeline
failure-rate denominator. A second resolution cannot overwrite the first audit.
Resolved rows remain visible through `quarantine list --all` and direct SQL.

Resolution alone does not publish a dataset. Rerun the pipeline and satisfy all
remaining source, horizon, transformation, reconciliation, and dbt checks.
Schema migration preserves old quarantine records, derives recoverable legacy
identities from their original payloads, and leaves unkeyed legacy records for
explicit operator disposition.

JSONL parsing rejects nonstandard `NaN`/`Infinity` constants and numbers such as
`1e309` that exceed the client's finite floating-point range. The latter is a
client range limit, although the numeric token is valid JSON syntax. These rows
retain the original line plus an actionable parse/range reason in quarantine;
non-finite values never reach PostgreSQL JSONB adaptation. Manifest preflight
rejects the same inputs explicitly. Finite floating-point additive metadata
remains compatible; required integer fields still enforce their source contracts.
