"""Verify manifests, land immutable bytes, and maintain canonical source versions."""
from __future__ import annotations

import csv
import hashlib
import io
import json
import os
import re
import tempfile
from datetime import date
from pathlib import Path, PureWindowsPath

from psycopg.types.json import Jsonb

from .contracts import ContractError, load_contract, validate_record

SOURCES = {"order_lines", "refunds", "inventory", "products", "locations"}


def digest(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def read_manifest(path: Path):
    path = path.resolve()
    data = path.read_bytes()
    manifest = json.loads(data)
    if not isinstance(manifest, dict) or manifest.get("synthetic") is not True or not isinstance(manifest.get("files"), list):
        raise ValueError("Manifest must label synthetic=true and contain a files list")
    verified = []
    seen = set()
    for entry in manifest["files"]:
        required = {"source", "path", "sha256", "schema_version", "partition", "row_count"}
        if not isinstance(entry, dict) or not required.issubset(entry):
            raise ValueError("Each manifest file requires source/path/sha256/schema_version/partition/row_count")
        source = entry["source"]
        if not isinstance(source, str) or source not in SOURCES or not isinstance(entry["path"], str):
            raise ValueError("Unknown source or unsafe manifest path")
        relative = Path(entry["path"])
        if relative.is_absolute() or entry["path"].startswith("/") or ".." in relative.parts or PureWindowsPath(entry["path"]).drive or "\\" in entry["path"] or entry["path"] in {"", "."}:
            raise ValueError("Unknown source or unsafe manifest path")
        file = (path.parent / relative).resolve()
        if not file.is_relative_to(path.parent) or str(file) in seen:
            raise ValueError("Manifest path escapes source directory or repeats a file")
        seen.add(str(file))
        if not isinstance(entry["sha256"], str) or not re.fullmatch(r"[0-9a-f]{64}", entry["sha256"]):
            raise ValueError("Invalid SHA-256 checksum in manifest")
        body = file.read_bytes()
        if digest(body) != entry["sha256"]:
            raise ValueError(f"Checksum mismatch: {relative}")
        if type(entry["schema_version"]) is not int or entry["schema_version"] != 1:
            raise ValueError(f"Unsupported breaking schema version: {relative}")
        if not isinstance(entry["partition"], str) or date.fromisoformat(entry["partition"]).isoformat() != entry["partition"]:
            raise ValueError("Manifest partition must be a YYYY-MM-DD date")
        if type(entry["row_count"]) is not int or entry["row_count"] < 0:
            raise ValueError("Invalid manifest row_count")
        verified.append((entry, body))
    if not verified:
        raise ValueError("Empty manifest")
    return manifest, digest(data), verified


def iter_rows(source, body):
    text = body.decode("utf-8-sig")
    if source == "order_lines":
        reader = csv.DictReader(io.StringIO(text), strict=True)
        if not reader.fieldnames or len(reader.fieldnames) != len(set(reader.fieldnames)):
            raise ValueError("Missing/duplicate CSV headers")
        if not set(load_contract(source)["fields"]).issubset(reader.fieldnames):
            raise ValueError("Missing required CSV headers")
        for row in reader:
            if None in row:
                # DictReader otherwise silently stores surplus unheaded values.
                raise ValueError("CSV row has more values than declared headers")
            yield row
    else:
        for line in text.splitlines():
            if not line.strip():
                continue
            try:
                yield json.loads(line)
            except json.JSONDecodeError:
                yield {"_invalid_json": line}


def land_raw(raw_root: Path, entry: dict, body: bytes) -> str:
    """Atomically create a content-addressed object without replacing old bytes.

    The caller supplies a verified manifest entry. A temporary same-filesystem
    object is fsynced before an exclusive hard link makes the final name visible;
    interruption cannot expose a truncated final object. Existing objects are
    checked on every replay and corruption stops ingestion explicitly.
    """
    if digest(body) != entry["sha256"]:
        raise ValueError("Raw bytes do not match verified checksum")
    raw = raw_root / entry["source"] / entry["partition"] / entry["sha256"]
    raw.parent.mkdir(parents=True, exist_ok=True)
    if raw.exists():
        if digest(raw.read_bytes()) != entry["sha256"]:
            raise ValueError("Immutable raw object corruption")
        return str(raw.relative_to(raw_root))
    temporary = None
    try:
        with tempfile.NamedTemporaryFile(dir=raw.parent, prefix=".landing-", delete=False) as stream:
            temporary = Path(stream.name)
            stream.write(body)
            stream.flush()
            os.fsync(stream.fileno())
        try:
            os.link(temporary, raw)
        except FileExistsError:
            if digest(raw.read_bytes()) != entry["sha256"]:
                raise ValueError("Immutable raw object corruption")
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)
    return str(raw.relative_to(raw_root))


def ingest(conn, run_id, verified, raw_root: Path, as_of: date):
    totals = dict(received=0, accepted=0, rejected=0, changed=0)
    # Each input is landed before the source transaction; orphan raw objects are safe.
    landed = []
    for entry, body in verified:
        # Durable provenance is relative to the configured raw root.
        landed.append((entry, body, land_raw(raw_root, entry, body)))
    with conn.transaction():
        for entry, body, raw_path in landed:
            source = entry["source"]
            counts = dict(received=0, accepted=0, rejected=0)
            for number, record in enumerate(iter_rows(source, body), 1):
                counts["received"] += 1
                try:
                    normalized, key = validate_record(source, record, schema_version=entry["schema_version"])
                    field = {"order_lines": "sale_date", "refunds": "refund_date", "inventory": "snapshot_date"}.get(source)
                    record_date = date.fromisoformat(normalized[field] if field else normalized["effective_at"][:10])
                    if record_date > as_of:
                        raise ContractError("Record is later than the run as_of date")
                    if str(record_date) != entry["partition"]:
                        raise ContractError("Record date does not match manifest partition")
                    payload_hash = digest(json.dumps(normalized, sort_keys=True, separators=(",", ":")).encode())
                    old = conn.execute("select * from landing.records where source=%s and business_key=%s", (source, key)).fetchone()
                    if old and normalized["version"] == old["version"] and payload_hash != old["payload_hash"]:
                        raise ContractError("Conflicting payload at the same source version; increment version")
                    if not old or normalized["version"] > old["version"]:
                        conn.execute("""insert into landing.records(source,business_key,version,payload,payload_hash,raw_path,run_id)
                          values(%s,%s,%s,%s,%s,%s,%s) on conflict(source,business_key) do update set
                          version=excluded.version,payload=excluded.payload,payload_hash=excluded.payload_hash,
                          raw_path=excluded.raw_path,run_id=excluded.run_id,ingested_at=now()""",
                          (source, key, normalized["version"], Jsonb(normalized), payload_hash, raw_path, run_id))
                        conn.execute("""insert into meta.source_changes(run_id,source,business_key,before_payload,after_payload,raw_path)
                          values(%s,%s,%s,%s,%s,%s)""", (run_id, source, key, Jsonb(old["payload"]) if old else None, Jsonb(normalized), raw_path))
                        totals["changed"] += 1
                    counts["accepted"] += 1
                except (ContractError, TypeError, KeyError, ValueError) as exc:
                    counts["rejected"] += 1
                    conn.execute("""insert into meta.quarantine(run_id,source,raw_path,row_number,payload,reason)
                      values(%s,%s,%s,%s,%s,%s)""", (run_id, source, raw_path, number, Jsonb(record), str(exc)))
            if counts["received"] != entry["row_count"]:
                raise ValueError(f"Row count mismatch for {entry['path']}: expected {entry['row_count']}, received {counts['received']}")
            conn.execute("""insert into meta.files(run_id,source,source_path,raw_path,sha256,schema_version,
              partition_date,expected_rows,received,accepted,rejected) values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
              (run_id, source, entry["path"], raw_path, entry["sha256"], entry["schema_version"], entry["partition"],
               entry["row_count"], counts["received"], counts["accepted"], counts["rejected"]))
            for k, v in counts.items():
                totals[k] += v
        conn.execute("update meta.runs set received=%s,accepted=%s,rejected=%s,changed=%s where run_id=%s",
                     (totals["received"], totals["accepted"], totals["rejected"], totals["changed"], run_id))
    return totals
