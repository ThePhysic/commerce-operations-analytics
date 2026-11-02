"""Prepare self-contained daily source deliveries for the local Airflow demo.

Catalog history effective by each day is included. Other feeds contain that day's
partition, with explicit empty files when the producer has no events. This is a
fixture-delivery utility, not an ingestion or transformation implementation.
"""

from __future__ import annotations

import argparse
import hashlib
import json
from datetime import date, timedelta
from pathlib import Path


SOURCES = {"order_lines", "refunds", "inventory", "products", "locations"}
CATALOGS = {"products", "locations"}


def stage(manifest_path: Path, output: Path) -> dict:
    manifest_path = manifest_path.resolve()
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    if manifest.get("synthetic") is not True:
        raise ValueError("Only explicitly synthetic manifests are supported")
    files = manifest["files"]
    if {entry["source"] for entry in files} != SOURCES:
        raise ValueError("Expected all five source contracts")
    contents = {}
    for entry in files:
        relative = Path(entry["path"])
        path = (manifest_path.parent / relative).resolve()
        if relative.is_absolute() or not path.is_relative_to(manifest_path.parent):
            raise ValueError("Source path escapes manifest directory")
        content = path.read_bytes()
        if hashlib.sha256(content).hexdigest() != entry["sha256"]:
            raise ValueError(f"Checksum mismatch: {relative}")
        contents[entry["path"]] = content
    dates = [date.fromisoformat(entry["partition"]) for entry in files if entry["source"] not in CATALOGS]
    first, last = min(dates), max(dates)
    written = []
    for offset in range((last - first).days + 1):
        day = first + timedelta(days=offset)
        selected = [dict(entry) for entry in files if (
            entry["source"] in CATALOGS and date.fromisoformat(entry["partition"]) <= day
        ) or (entry["source"] not in CATALOGS and entry["partition"] == day.isoformat())]
        present = {entry["source"] for entry in selected}
        if not CATALOGS.issubset(present):
            raise ValueError(f"No effective catalog for {day}")
        bodies = {entry["path"]: contents[entry["path"]] for entry in selected}
        for source in sorted(SOURCES - present):
            extension = "csv" if source == "order_lines" else "jsonl"
            relative = f"{source}/{day}.{extension}"
            if source == "order_lines":
                template = next(entry for entry in files if entry["source"] == source)
                body = contents[template["path"]].splitlines(keepends=True)[0]
            else:
                body = b""
            selected.append({"source": source, "path": relative, "sha256": hashlib.sha256(body).hexdigest(),
                             "schema_version": 1, "partition": day.isoformat(), "row_count": 0})
            bodies[relative] = body
        destination = output / day.isoformat()
        for entry in selected:
            path = destination / entry["path"]
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(bodies[entry["path"]])
        staged = {"synthetic": True, "seed": manifest["seed"], "files": sorted(selected, key=lambda e: (e["source"], e["partition"]))}
        result_path = destination / "manifest.json"
        result_path.write_text(json.dumps(staged, sort_keys=True, indent=2) + "\n", encoding="utf-8", newline="\n")
        written.append(str(result_path))
    return {"synthetic": True, "first_interval": str(first), "last_interval": str(last), "manifests": written}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    print(json.dumps(stage(args.manifest, args.output), indent=2))
