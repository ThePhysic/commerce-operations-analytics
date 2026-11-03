"""Verify manifests, land immutable bytes, and maintain canonical source versions."""
from __future__ import annotations

import csv
import hashlib
import io
import json
import math
import os
import re
import tempfile
from datetime import date
from pathlib import Path, PureWindowsPath

from psycopg.types.json import Jsonb

from .contracts import (
    INT32_MAX, INT64_MAX, ContractError, load_contract, quarantine_identity,
    validate_jsonb, validate_record, validate_text,
)

SOURCES = {"order_lines", "refunds", "inventory", "products", "locations"}


def digest(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _reject_json_constant(value: str):
    raise ValueError(f"Nonstandard JSON constant is forbidden: {value}")


def _finite_json_float(value: str):
    parsed = float(value)
    if not math.isfinite(parsed):
        raise ValueError(f"JSON number exceeds finite client float range: {value}")
    return parsed


def _strict_json(data):
    # Python's default decoder accepts NaN/Infinity although JSON forbids them;
    # PostgreSQL JSONB rejects them, including inside an attempted quarantine.
    return json.loads(data, parse_constant=_reject_json_constant, parse_float=_finite_json_float)


def read_manifest(path: Path):
    path = path.resolve()
    data = path.read_bytes()
    manifest = _strict_json(data)
    validate_jsonb(manifest)
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
        if type(entry["schema_version"]) is not int or not 1 <= entry["schema_version"] <= INT32_MAX:
            raise ValueError(f"Invalid schema version: {relative}")
        if not isinstance(entry["partition"], str) or date.fromisoformat(entry["partition"]).isoformat() != entry["partition"]:
            raise ValueError("Manifest partition must be a YYYY-MM-DD date")
        if type(entry["row_count"]) is not int or not 0 <= entry["row_count"] <= INT64_MAX:
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
                yield _strict_json(line)
            except (ValueError, RecursionError) as exc:
                yield {"_invalid_json": line, "_invalid_json_reason": str(exc)}


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


def unresolved_quarantine(conn):
    """Return outstanding record, conflict, and unkeyed file failures."""
    return conn.execute("""select id,source,business_key,source_version,raw_path,reason,run_id,created_at,issue_kind
      from meta.quarantine where resolved_at is null order by id""").fetchall()


class VersionConflict(ContractError):
    """Two payloads claim the same source/business key/version."""


def safe_quarantine_payload(record):
    """Keep ordinary payloads intact and safely wrap unrepresentable JSON.

    A JSON ASCII serialization stored as a JSON string preserves escapes rather
    than asking PostgreSQL to decode an embedded NUL or unpaired surrogate. Raw
    bytes remain the authority if even client serialization is not possible.
    """
    try:
        validate_jsonb(record)
        json.dumps(record, allow_nan=False)
        return record
    except (ContractError, ValueError, TypeError, RecursionError):
        try:
            encoded = json.dumps(record, ensure_ascii=True, allow_nan=False)
        except (ValueError, TypeError, RecursionError):
            return {"_quarantine_encoding": "raw-only", "detail": "Inspect the immutable raw object"}
        return {"_quarantine_encoding": "json-ascii", "_quarantine_payload": encoded}


def _safe_reason(error):
    reason = str(error)
    try:
        validate_text(reason)
    except ContractError:
        return json.dumps(reason, ensure_ascii=True)[1:-1]
    return reason


def _quarantine(conn, run_id, source, raw_path, number, record, error, *, issue_kind="record"):
    key, version = (None, None) if issue_kind == "file" else quarantine_identity(source, record)
    conn.execute("""insert into meta.quarantine
      (run_id,source,raw_path,row_number,payload,reason,business_key,source_version,issue_kind)
      values(%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
      (run_id, source, raw_path, number, Jsonb(safe_quarantine_payload(record)),
       _safe_reason(error), key, version, issue_kind))


def ingest(conn, run_id, verified, raw_root: Path, as_of: date):
    totals = dict(received=0, accepted=0, rejected=0, changed=0, file_failures=0)
    # Each input is landed before the source transaction; orphan raw objects are safe.
    landed = []
    for entry, body in verified:
        # Durable provenance is relative to the configured raw root.
        raw_path = land_raw(raw_root, entry, body)
        landed.append((entry, body, raw_path))
    seen_versions = {}
    with conn.transaction():
        for entry, body, raw_path in landed:
            source = entry["source"]
            counts = dict(received=0, accepted=0, rejected=0, file_failures=0)
            observed_rows = None
            # Parse/count the whole file before accepting any of its rows. Data
            # errors persist as file blockers while clean sibling files proceed;
            # database, filesystem, and connection failures still propagate.
            try:
                records = list(iter_rows(source, body))
                observed_rows = len(records)
                if observed_rows != entry["row_count"]:
                    raise ValueError(f"Row count mismatch for {entry['path']}: expected {entry['row_count']}, received {observed_rows}")
                if entry["schema_version"] != load_contract(source)["schema_version"] and not records:
                    raise ValueError("Unsupported schema version in empty file")
            except (ValueError, csv.Error, RecursionError) as exc:
                records = []
                counts["file_failures"] = 1
                _quarantine(conn, run_id, source, raw_path, 0,
                            {"manifest_entry": entry, "observed_rows": observed_rows}, exc, issue_kind="file")
            for number, record in enumerate(records, 1):
                counts["received"] += 1
                try:
                    if isinstance(record, dict) and set(record) == {"_invalid_json", "_invalid_json_reason"}:
                        raise ContractError("Invalid JSON input: " + record["_invalid_json_reason"])
                    normalized, key = validate_record(source, record, schema_version=entry["schema_version"])
                    field = {"order_lines": "sale_date", "refunds": "refund_date", "inventory": "snapshot_date"}.get(source)
                    record_date = date.fromisoformat(normalized[field] if field else normalized["effective_at"][:10])
                    if record_date > as_of:
                        raise ContractError("Record is later than the run as_of date")
                    if source == "order_lines" and any(
                        normalized.get(field) and date.fromisoformat(normalized[field][:10]) > as_of
                        for field in ("paid_at", "fulfilled_at")
                    ):
                        raise ContractError("Order lifecycle timestamp is later than the run as_of date")
                    if str(record_date) != entry["partition"]:
                        raise ContractError("Record date does not match manifest partition")
                    payload_hash = digest(json.dumps(normalized, sort_keys=True, separators=(",", ":")).encode())
                    old = conn.execute("select * from landing.records where source=%s and business_key=%s", (source, key)).fetchone()
                    version_identity = (source, key, normalized["version"])
                    known_hash = seen_versions.get(version_identity)
                    if old and normalized["version"] == old["version"]:
                        known_hash = old["payload_hash"]
                    elif old and normalized["version"] < old["version"]:
                        historical = conn.execute("""select after_payload_hash,after_payload=%s payload_matches
                          from meta.source_changes
                          where source=%s and business_key=%s and after_payload->>'version'=%s
                          order by id desc limit 1""", (Jsonb(normalized), source, key, str(normalized["version"]))).fetchone()
                        if historical:
                            # JSONB may round-trip an integral float as an int.
                            # Retain the original hash for new changes; legacy
                            # changes can only be compared by JSONB value equality.
                            known_hash = historical["after_payload_hash"]
                            if known_hash is None:
                                known_hash = payload_hash if historical["payload_matches"] else ""
                    if known_hash is not None and payload_hash != known_hash:
                        raise VersionConflict("Conflicting payload at the same source version; increment version")
                    seen_versions[version_identity] = payload_hash
                    if not old or normalized["version"] > old["version"]:
                        conn.execute("""insert into landing.records(source,business_key,version,payload,payload_hash,raw_path,run_id)
                          values(%s,%s,%s,%s,%s,%s,%s) on conflict(source,business_key) do update set
                          version=excluded.version,payload=excluded.payload,payload_hash=excluded.payload_hash,
                          raw_path=excluded.raw_path,run_id=excluded.run_id,ingested_at=now()""",
                          (source, key, normalized["version"], Jsonb(normalized), payload_hash, raw_path, run_id))
                        conn.execute("""insert into meta.source_changes(run_id,source,business_key,before_payload,after_payload,raw_path,after_payload_hash)
                          values(%s,%s,%s,%s,%s,%s,%s)""", (run_id, source, key, Jsonb(old["payload"]) if old else None, Jsonb(normalized), raw_path, payload_hash))
                        totals["changed"] += 1
                    counts["accepted"] += 1
                    conn.execute("""update meta.quarantine set resolved_at=now(),resolved_by_run_id=%s,
                      resolution_reason='Validated redelivery of matching source/business_key at sufficient version'
                      where resolved_at is null and source=%s and business_key=%s
                      and ((issue_kind='record' and (source_version is null or source_version<=%s))
                        or (issue_kind='conflict' and source_version<%s))""",
                      (run_id, source, key, normalized["version"], normalized["version"]))
                except (ContractError, TypeError, KeyError, ValueError, RecursionError) as exc:
                    counts["rejected"] += 1
                    _quarantine(conn, run_id, source, raw_path, number, record, exc,
                                issue_kind="conflict" if isinstance(exc, VersionConflict) else "record")
            conn.execute("""insert into meta.files(run_id,source,source_path,raw_path,sha256,schema_version,
              partition_date,expected_rows,received,accepted,rejected,file_failures,observed_rows)
              values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
              (run_id, source, entry["path"], raw_path, entry["sha256"], entry["schema_version"], entry["partition"],
               entry["row_count"], counts["received"], counts["accepted"], counts["rejected"],
               counts["file_failures"], observed_rows))
            for k, v in counts.items():
                totals[k] += v
        conn.execute("update meta.runs set received=%s,accepted=%s,rejected=%s,changed=%s,file_failures=%s where run_id=%s",
                     (totals["received"], totals["accepted"], totals["rejected"], totals["changed"], totals["file_failures"], run_id))
    return totals
