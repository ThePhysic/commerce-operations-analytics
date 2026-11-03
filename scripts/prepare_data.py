"""Prepare only this demo's shared artifact paths for its unprivileged containers.

Run by the short-lived Compose data-init service as root. No file contents are
changed and symlinks are never followed. Set COMMERCE_UID to a Linux host UID
when the host user also needs to edit generated files.
"""
from __future__ import annotations

import os
import stat
from pathlib import Path


DATA = Path("/workspace/data")
OWNED_DIRECTORIES = ("raw", "artifacts", "incoming", "scheduled", "demo", "benchmark", "dbt-docs")
OWNED_FILES = ("benchmark.json",)
AIRFLOW_VOLUMES = (Path("/workspace/airflow-state"), Path("/workspace/airflow-logs"))


def prepare() -> None:
    uid = int(os.environ.get("COMMERCE_UID", "50000"))
    if uid < 1:
        raise ValueError("COMMERCE_UID must be a positive, unprivileged UID")
    if os.geteuid() != 0:
        raise PermissionError("Run data preparation through the root data-init service")

    def permissions(path: Path, *, skip_child_link=False) -> None:
        info = path.lstat()
        mode = info.st_mode
        if stat.S_ISLNK(mode):
            if skip_child_link:
                return
            raise ValueError(f"Refusing a symlink in managed artifact paths: {path}")
        if not (stat.S_ISREG(mode) or stat.S_ISDIR(mode)):
            raise ValueError(f"Expected a regular file or directory: {path}")
        if stat.S_ISREG(mode) and info.st_nlink > 1:
            raise ValueError(f"Refusing a multiply linked file in managed paths: {path}")
        # Preserve existing execute bits; new files never become executable.
        target_mode = ((stat.S_IMODE(mode) & 0o775) | 0o2770) if stat.S_ISDIR(mode) else (
            (stat.S_IMODE(mode) & 0o775) | 0o660)
        if info.st_uid != uid or info.st_gid != 0:
            os.chown(path, uid, 0, follow_symlinks=False)
            # chown may clear permission bits, so inspect the resulting mode.
            mode = path.lstat().st_mode
        if stat.S_IMODE(mode) != target_mode:
            os.chmod(path, target_mode, follow_symlinks=False)

    DATA.mkdir(parents=True, exist_ok=True)
    permissions(DATA)
    for name in OWNED_DIRECTORIES:
        directory = DATA / name
        directory.mkdir(exist_ok=True)
        permissions(directory)
        for parent, directories, files in os.walk(directory, followlinks=False):
            for child in directories + files:
                permissions(Path(parent) / child)
    for name in OWNED_FILES:
        path = DATA / name
        if path.exists() or path.is_symlink():
            permissions(path)
    # These optional fixed mounts are the project's dedicated named volumes.
    # Airflow creates child symlinks such as logs/scheduler/latest; leave the
    # links and their targets untouched rather than following them.
    for directory in AIRFLOW_VOLUMES:
        if not directory.exists() and not directory.is_symlink():
            continue
        permissions(directory)
        for parent, directories, files in os.walk(directory, followlinks=False):
            for child in directories + files:
                permissions(Path(parent) / child, skip_child_link=True)
    print(f"Shared demo artifact paths prepared for uid={uid}, gid=0; source bytes preserved")


if __name__ == "__main__":
    prepare()
