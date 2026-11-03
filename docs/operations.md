# Shared artifacts and scheduled recovery

Compose runs the batch runner and Airflow with the same UID and group 0. Their
default UID is 50000. On Linux, `export COMMERCE_UID="$(id -u)"` before starting
Compose makes generated files belong to the current host user. Airflow's official
entrypoint supports an arbitrary positive UID in group 0. Keep that entrypoint
when running the Airflow image directly.

The root `data-init` service exits successfully before either application starts.
It changes ownership and permissions only on `/workspace/data` itself, the
project's `raw`, `artifacts`, `incoming`, `scheduled`, `demo`, `benchmark`, and
`dbt-docs` trees, and an existing top-level `benchmark.json`. It also prepares the
dedicated Airflow state/log named volumes at the fixed initializer mounts
`/workspace/airflow-state` and `/workspace/airflow-logs`. Existing bytes are
preserved. Directories gain group write and setgid; files gain owner/group write;
world write is removed. Symlinks and special files in managed data paths are
rejected instead of followed. Airflow's child symlinks (including
`scheduler/latest`) are skipped without changing the link or following its
target; the mounted roots still refuse symlinks. Multiply linked regular files
are rejected so ownership/permission changes cannot affect another hard link
outside the managed trees. Other top-level data files and directories are left
alone. These paths contain only this project's generated artifacts and Airflow
metadata/logs.

This also repairs the root-owned 0755 directories produced by earlier runner
images. To prepare existing artifacts explicitly after choosing a UID, run:

```sh
docker compose run --build --rm data-init
```

The default UID means generated files may show owner 50000 on a Linux host.
Set `COMMERCE_UID` to the host UID and rerun the initializer to change the managed
artifact ownership. Changing ownership does not grant access to unrelated
directories or alter the PostgreSQL named volume.

The daily DAG records its logical date as an expected delivery before opening
the manifest. Once an earlier day has been requested, later daily deliveries
cannot publish through a gap, including a missing initial manifest. The task
also depends on the preceding interval's success. Restore or correct a failed
day's delivery, then pause the DAG and wait for active tasks/backfill jobs to
stop. Use `airflow dags pause commerce_batch` in the Airflow container, or the
UI's pause toggle. Recover the failed and later blocked range with:

```sh
python /opt/commerce/scripts/recover_intervals.py --start 2024-01-01 --end 2024-01-02 --reason "Restored missing synthetic delivery"
```

Run that command in the Airflow container, then rerun the original backfill.
The pinned native clear/reset commands leave already-running DagRuns running,
even when their tasks never started. The bounded helper finishes only inactive
blocked runs and uses native clearing to release their active-run slots. It
requires a paused DAG (or metadata with no registered scheduler), refuses live
backfill/task jobs and running/queued/scheduled tasks, and operates only on
`commerce_batch` within the explicit inclusive UTC dates. Its structured stdout
records the operator reason and old/new states after the transaction commits.
DagRun/task-instance records, task history, logs, and commerce raw/run audits are
retained. Preserve that output with the run logs while investigating; do not mark
failed intervals successful to bypass missing input.

The task rebuilds the logical day and seven preceding dates against the current
durable reporting horizon. This permits clearing an old task after later
publication without moving the clock backward. Corrections whose old/new dates
fall outside that window need an explicit CLI range or full refresh. Failed
quality gates continue to leave the previous analytical publication available.

The regression harness requires Linux Docker, both built images, an empty
disposable PostgreSQL database whose name ends in `_test`, and a dedicated mount.
CI uses a Linux bind mount. A Docker named volume preserves Linux ownership when
running the harness from a Windows Docker Desktop host. For example, from a
POSIX shell with Docker access:

```sh
docker build --tag commerce-batch:ci .
docker build --file Dockerfile.airflow --tag commerce-airflow:ci .
docker volume create commerce-permissions-test
export COMMERCE_TEST_MOUNT=type=volume,source=commerce-permissions-test,target=/workspace/data
export COMMERCE_TEST_NETWORK=bridge
export COMMERCE_TEST_STATE_VOLUME=commerce-permissions-state-test
export COMMERCE_TEST_LOGS_VOLUME=commerce-permissions-logs-test
export COMMERCE_DATABASE_URL=postgresql://commerce:synthetic-local-only@host.docker.internal:55432/commerce_permissions_test
bash scripts/test_container_workflow.sh
docker volume rm commerce-permissions-test
docker volume rm commerce-permissions-state-test commerce-permissions-logs-test
```

Create that empty database first. Use the appropriate database hostname for the
Docker host; CI uses host networking and `127.0.0.1`. `COMMERCE_TEST_NETWORK`
selects the Docker network and defaults to `host`. The harness creates host UID
1000/root-owned permission fixtures, runs initialization twice, verifies unchanged
evidence bytes and unprivileged writes, stages deliveries through the runner,
then executes Airflow's missing-first-day failure, recovery for two days, and an
older-day retry after later publication. The volumes and database are test-only
artifacts; the harness never resets or drops the database itself.
