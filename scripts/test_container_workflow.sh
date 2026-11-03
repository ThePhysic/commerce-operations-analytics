#!/usr/bin/env bash
# Linux Docker harness. The mount and database must be dedicated disposable test fixtures.
set -euo pipefail

: "${COMMERCE_DATABASE_URL:?Set a dedicated empty database ending in _test}"
: "${COMMERCE_TEST_MOUNT:?Set a Docker --mount value targeting /workspace/data}"
: "${COMMERCE_TEST_STATE_VOLUME:?Set a dedicated disposable Airflow metadata volume name}"
: "${COMMERCE_TEST_LOGS_VOLUME:?Set a dedicated disposable Airflow logs volume name}"
runner_image="${COMMERCE_RUNNER_IMAGE:-commerce-batch:ci}"
airflow_image="${COMMERCE_AIRFLOW_IMAGE:-commerce-airflow:ci}"
container_uid="${COMMERCE_UID:-50000}"
mount_args=(--mount "$COMMERCE_TEST_MOUNT")
docker volume create "$COMMERCE_TEST_STATE_VOLUME" >/dev/null
docker volume create "$COMMERCE_TEST_LOGS_VOLUME" >/dev/null
init_mount_args=("${mount_args[@]}"
    --mount "type=volume,source=$COMMERCE_TEST_STATE_VOLUME,target=/workspace/airflow-state"
    --mount "type=volume,source=$COMMERCE_TEST_LOGS_VOLUME,target=/workspace/airflow-logs")
airflow_mount_args=("${mount_args[@]}"
    --mount "type=volume,source=$COMMERCE_TEST_STATE_VOLUME,target=/opt/airflow/state"
    --mount "type=volume,source=$COMMERCE_TEST_LOGS_VOLUME,target=/opt/airflow/logs")

# Emulate both a Linux host user and root-owned 0755 paths left by older images.
docker run --rm --user 0:0 "${init_mount_args[@]}" --entrypoint python "$runner_image" -c '
import os
from pathlib import Path
p=Path("/workspace/data")
os.chown(p,1000,1000)
for name in ("raw", "artifacts"):
    d=p/name
    d.mkdir(exist_ok=True)
    os.chown(d,0,0)
    d.chmod(0o755)
    f=d/"legacy-permission-fixture"
    f.write_bytes(b"original synthetic evidence\n")
    os.chown(f,0,0)
    f.chmod(0o644)
for name in ("airflow-state", "airflow-logs"):
    d=Path("/workspace")/name
    os.chown(d,50000,0)
    d.chmod(0o755)
'
docker run --rm --user 0:0 "${init_mount_args[@]}" -e COMMERCE_UID="$container_uid" \
    --entrypoint python "$runner_image" /app/scripts/prepare_data.py
# The initializer is idempotent and preserves the exact legacy bytes.
docker run --rm --user 0:0 "${init_mount_args[@]}" -e COMMERCE_UID="$container_uid" \
    --entrypoint python "$runner_image" /app/scripts/prepare_data.py
docker run --rm --user "$container_uid:0" "${init_mount_args[@]}" --entrypoint python "$runner_image" -c '
import os
import sqlite3
from pathlib import Path
assert os.geteuid()!=0
for name in ("raw", "artifacts"):
    p=Path("/workspace/data")/name
    assert (p/"legacy-permission-fixture").read_bytes()==b"original synthetic evidence\n"
    child=p/"permission-probe"
    child.mkdir(exist_ok=True)
    (child/"written-by-unprivileged-container").write_text("ok\n")
for name in ("airflow-state", "airflow-logs"):
    p=Path("/workspace")/name
    probe=p/"uid-permission-probe"
    probe.write_text("ok\n")
    probe.unlink()
for database in Path("/workspace/airflow-state").glob("commerce-airflow-smoke.*/airflow.db"):
    with sqlite3.connect(str(database)) as c:
        c.execute("create table if not exists uid_permission_probe (uid integer)")
        c.execute("insert into uid_permission_probe values (?)",(os.geteuid(),))
    print("Existing Airflow SQLite metadata writable after UID migration:",database)
print("Legacy root-owned directories are writable without changing evidence bytes")
'
docker run --rm --user "$container_uid:0" "${mount_args[@]}" "$runner_image" \
    generate --output /workspace/data/incoming --days 7 --orders-per-day 20 --seed 42
docker run --rm --user "$container_uid:0" "${mount_args[@]}" --entrypoint python "$runner_image" \
    /app/scripts/stage_intervals.py --manifest /workspace/data/incoming/manifest.json --output /workspace/data/scheduled
# Retain the official Airflow entrypoint: it supports arbitrary UIDs in group 0.
docker run --rm --network "${COMMERCE_TEST_NETWORK:-host}" --memory 2g --cpus 1.5 --user "$container_uid:0" \
    "${airflow_mount_args[@]}" -e COMMERCE_DATABASE_URL="$COMMERCE_DATABASE_URL" \
    "$airflow_image" bash /opt/commerce/scripts/test_airflow.sh
