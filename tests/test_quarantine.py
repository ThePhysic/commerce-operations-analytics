"""Record-specific quarantine regressions on a separate disposable database.

COMMERCE_QUARANTINE_TEST_DATABASE_URL may name only commerce_quarantine_test.
When just COMMERCE_TEST_DATABASE_URL is set, create that separate database using
the same local test credentials; never reset the ordinary integration database.
"""
from __future__ import annotations

import importlib.util
import json
import os
import uuid
from datetime import date
from pathlib import Path

import psycopg
import pytest
from psycopg import sql
from psycopg.conninfo import conninfo_to_dict, make_conninfo
from psycopg.rows import dict_row

from commerce.cli import quarantine, status
from commerce.contracts import quarantine_identity
from commerce.db import initialize
from commerce.generate import write_dataset
from commerce.ingest import digest, ingest, read_manifest, unresolved_quarantine
from commerce.pipeline import _check_sources

ROOT = Path(__file__).resolve().parents[1]
AS_OF = date(2024, 1, 5)


def reference():
    spec = importlib.util.spec_from_file_location("quarantine_reference", ROOT / "fixtures" / "build_reference.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module.reference_records()


def test_rejected_identity_uses_only_key_and_version_fields():
    assert quarantine_identity("order_lines", {"line_id": "O004_1", "version": "2", "quantity": "bad"}) == ("O004_1", 2)
    assert quarantine_identity("order_lines", {"line_id": "O004_1", "version": "broken"}) == ("O004_1", None)
    assert quarantine_identity("order_lines", {"version": 2}) == (None, 2)
    assert quarantine_identity("refunds", {"_invalid_json": "truncated"}) == (None, None)
    assert quarantine_identity("products", {"product_id": "P1", "effective_at": "2024-01-01T00:00:00Z", "version": 2}) == ('["P1","2024-01-01T00:00:00+00:00"]', 2)


@pytest.fixture(scope="module")
def quarantine_url():
    explicit = os.getenv("COMMERCE_QUARANTINE_TEST_DATABASE_URL")
    base = explicit or os.getenv("COMMERCE_TEST_DATABASE_URL")
    if not base:
        pytest.skip("No disposable PostgreSQL test credentials configured")
    cfg = conninfo_to_dict(base)
    original_db = cfg.get("dbname", "")
    if explicit:
        if original_db != "commerce_quarantine_test":
            pytest.fail("Refusing quarantine tests outside commerce_quarantine_test")
    else:
        if not original_db.endswith("_test"):
            pytest.fail("Refusing to derive quarantine database from a non-test database")
        with psycopg.connect(base, autocommit=True) as admin:
            if not admin.execute("select 1 from pg_database where datname=%s", ("commerce_quarantine_test",)).fetchone():
                admin.execute(sql.SQL("create database {}").format(sql.Identifier("commerce_quarantine_test")))
        cfg["dbname"] = "commerce_quarantine_test"
    return make_conninfo(**cfg)


@pytest.fixture
def database(quarantine_url, monkeypatch, tmp_path):
    monkeypatch.setenv("COMMERCE_DATABASE_URL", quarantine_url)
    monkeypatch.setenv("COMMERCE_DATA_DIR", str(tmp_path / "runtime"))
    with psycopg.connect(quarantine_url, autocommit=True, row_factory=dict_row) as conn:
        assert conn.info.dbname == "commerce_quarantine_test"
        for name in ("analytics", "landing", "meta"):
            conn.execute(sql.SQL("drop schema if exists {} cascade").format(sql.Identifier(name)))
        initialize(conn)
        ingest_bundle(conn, ROOT / "fixtures" / "reference" / "manifest.json", tmp_path)
        yield conn, tmp_path


def ingest_bundle(conn, manifest, root):
    _, _, verified = read_manifest(manifest)
    run_id = uuid.uuid4().hex
    conn.execute("insert into meta.runs(run_id,status) values(%s,'running')", (run_id,))
    counts = ingest(conn, run_id, verified, root / "raw", AS_OF)
    failed = counts["rejected"] or counts["file_failures"]
    conn.execute("update meta.runs set status=%s,ended_at=now() where run_id=%s", ("failed" if failed else "ingested", run_id))
    return run_id, counts


def line_record(line_id="O004_1", version=2, **updates):
    row = next(row for row in reference()["order_lines"] if row["line_id"] == line_id)
    row.update(version=version, **updates)
    return row


def deliver_line(conn, root, label, row):
    manifest = write_dataset(root / label, {"order_lines": [row]}, validate=False)
    return ingest_bundle(conn, manifest, root)


@pytest.mark.integration
def test_unrelated_clean_same_partition_does_not_clear_rejected_key(database):
    conn, root = database
    _, rejected = deliver_line(conn, root, "bad", line_record(quantity="bad"))
    assert rejected["rejected"] == 1
    before = unresolved_quarantine(conn)
    assert before[0]["issue_kind"] == "record"
    assert [(q["business_key"], q["source_version"]) for q in before] == [("O004_1", 2)]
    _, clean = deliver_line(conn, root, "unrelated", line_record("O005_1", version=1))
    assert clean["changed"] == 0
    assert unresolved_quarantine(conn) == before
    with pytest.raises(ValueError, match="Unresolved quarantined source records"):
        _check_sources(conn, AS_OF)
    fixed_run, _ = deliver_line(conn, root, "fixed", line_record())
    assert not unresolved_quarantine(conn)
    _check_sources(conn, AS_OF)
    retained = conn.execute("select * from meta.quarantine where id=%s", (before[0]["id"],)).fetchone()
    assert retained["resolved_by_run_id"] == fixed_run
    assert retained["resolved_at"] is not None
    assert "Validated redelivery" in retained["resolution_reason"]
    assert b"bad" in (root / "raw" / retained["raw_path"]).read_bytes()


@pytest.mark.integration
def test_older_valid_version_cannot_resolve_newer_rejected_correction(database):
    conn, root = database
    deliver_line(conn, root, "bad-v4", line_record(version=4, quantity="bad"))
    deliver_line(conn, root, "old-v1", line_record(version=1))
    assert unresolved_quarantine(conn)[0]["source_version"] == 4
    deliver_line(conn, root, "valid-v3", line_record(version=3))
    assert unresolved_quarantine(conn)[0]["source_version"] == 4
    deliver_line(conn, root, "valid-v4", line_record(version=4))
    assert not unresolved_quarantine(conn)


@pytest.mark.integration
def test_unsupported_schema_retains_readable_identity_for_supported_redelivery(database):
    conn, root = database
    manifest = write_dataset(root / "schema2", {"order_lines": [line_record()]})
    spec = json.loads(manifest.read_text())
    spec["files"][0]["schema_version"] = 2
    manifest.write_text(json.dumps(spec), encoding="utf-8")
    ingest_bundle(conn, manifest, root)
    rejected = unresolved_quarantine(conn)
    assert rejected[0]["business_key"] == "O004_1"
    assert rejected[0]["source_version"] == 2
    assert rejected[0]["issue_kind"] == "record"
    deliver_line(conn, root, "supported", line_record())
    assert not unresolved_quarantine(conn)


@pytest.mark.integration
def test_unkeyed_malformed_json_requires_audited_operator_resolution(database):
    conn, root = database
    directory = root / "malformed"
    directory.mkdir()
    body = b'{"refund_id":\n'
    (directory / "refunds.jsonl").write_bytes(body)
    manifest = directory / "manifest.json"
    manifest.write_text(json.dumps({"synthetic": True, "seed": 42, "files": [{"source": "refunds", "path": "refunds.jsonl", "partition": "2024-01-05", "sha256": digest(body), "schema_version": 1, "row_count": 1}]}), encoding="utf-8")
    ingest_bundle(conn, manifest, root)
    q = unresolved_quarantine(conn)[0]
    assert q["business_key"] is None
    # A valid row in that same date/source cannot identify truncated JSON.
    valid = next(row for row in reference()["refunds"] if row["refund_id"] == "R003")
    ingest_bundle(conn, write_dataset(root / "unrelated-refund", {"refunds": [valid]}), root)
    assert unresolved_quarantine(conn)[0]["id"] == q["id"]
    assert quarantine("list")[0]["id"] == q["id"]
    with pytest.raises(ValueError, match="nonempty"):
        quarantine("resolve", id=q["id"], reason="  ")
    assert unresolved_quarantine(conn)
    result = quarantine("resolve", id=q["id"], reason="Producer confirmed truncated duplicate; keep prior canonical refund")
    assert not unresolved_quarantine(conn)
    assert result["resolved_by_run_id"]
    assert conn.execute("select status from meta.runs where run_id=%s", (result["resolved_by_run_id"],)).fetchone()["status"] == "operator-resolved"
    assert len(quarantine("list", all=True)) == 1
    assert (root / "raw" / q["raw_path"]).read_bytes() == body
    assert status()["unresolved_quarantine_count"] == 0
    assert float(status()["run_failure_rate"]) == pytest.approx(1 / 3)
    with pytest.raises(ValueError, match="already resolved"):
        quarantine("resolve", id=q["id"], reason="do not rewrite audit")


@pytest.mark.integration
def test_legacy_quarantine_rows_gain_recoverable_identity_on_migration(database):
    conn, root = database
    run_id = uuid.uuid4().hex
    conn.execute("insert into meta.runs(run_id,status) values(%s,'failed')", (run_id,))
    from psycopg.types.json import Jsonb
    conn.execute("insert into meta.quarantine(run_id,source,raw_path,row_number,payload,reason) values(%s,'order_lines','legacy/raw',1,%s,'legacy invalid quantity')", (run_id, Jsonb(line_record(quantity="bad"))))
    initialize(conn)
    rejected = unresolved_quarantine(conn)
    assert rejected[0]["business_key"] == "O004_1"
    assert rejected[0]["source_version"] == 2
    deliver_line(conn, root, "fixed-legacy", line_record())
    assert not unresolved_quarantine(conn)


@pytest.mark.integration
def test_invalid_version_can_be_resolved_only_by_valid_same_key_redelivery(database):
    conn, root = database
    deliver_line(conn, root, "bad-version", line_record(version="broken"))
    assert unresolved_quarantine(conn)[0]["source_version"] is None
    deliver_line(conn, root, "unrelated", line_record("O005_1", version=1))
    assert unresolved_quarantine(conn)
    _, counts = deliver_line(conn, root, "corrected-version", line_record(version=1))
    assert counts["changed"] == 0
    assert not unresolved_quarantine(conn)


@pytest.mark.integration
def test_operator_resolution_obeys_pipeline_writer_lock(database):
    conn, root = database
    deliver_line(conn, root, "bad", line_record(quantity="bad"))
    rejected = unresolved_quarantine(conn)[0]
    conn.execute("select pg_advisory_lock(70402026)")
    try:
        with pytest.raises(RuntimeError, match="writer lock"):
            quarantine("resolve", id=rejected["id"], reason="operator decision")
        assert unresolved_quarantine(conn)[0]["id"] == rejected["id"]
    finally:
        conn.execute("select pg_advisory_unlock(70402026)")


@pytest.mark.integration
def test_nonstandard_nested_json_number_is_durably_quarantined(database):
    conn, root = database
    directory = root / "nonstandard-json"
    directory.mkdir()
    body = b'{"refund_id":"R003","line_id":"O002_1","refund_date":"2024-01-05","amount_cents":1000,"version":2,"extra":{"nested":[NaN]}}\n'
    (directory / "refunds.jsonl").write_bytes(body)
    manifest = directory / "manifest.json"
    manifest.write_text(json.dumps({"synthetic": True, "seed": 42, "files": [{"source": "refunds", "path": "refunds.jsonl", "partition": "2024-01-05", "sha256": digest(body), "schema_version": 1, "row_count": 1}]}), encoding="utf-8")
    _, counts = ingest_bundle(conn, manifest, root)
    assert counts["rejected"] == 1
    rejected = unresolved_quarantine(conn)
    assert len(rejected) == 1
    assert rejected[0]["business_key"] is None
    stored = conn.execute("select payload from meta.quarantine where id=%s", (rejected[0]["id"],)).fetchone()["payload"]
    assert stored["_invalid_json"] == body.decode().strip()
    assert "Nonstandard JSON constant" in stored["_invalid_json_reason"]
    assert "Invalid JSON input" in rejected[0]["reason"]
    assert (root / "raw" / rejected[0]["raw_path"]).read_bytes() == body
    with pytest.raises(ValueError, match="Unresolved quarantined source records"):
        _check_sources(conn, AS_OF)


@pytest.mark.integration
def test_valid_additive_fields_cannot_be_confused_with_internal_json_sentinel(database):
    conn, root = database
    row = line_record(_invalid_json="legitimate producer metadata", _invalid_json_reason="also metadata")
    _, counts = deliver_line(conn, root, "valid-additive-markers", row)
    assert counts["accepted"] == 1
    assert counts["rejected"] == 0
    assert not unresolved_quarantine(conn)
