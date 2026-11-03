"""Durable rejection and conflict regressions on an isolated PostgreSQL database."""
from __future__ import annotations

import importlib.util
import itertools
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
from psycopg.types.json import Jsonb

from commerce.cli import quarantine
from commerce.contracts import INT32_MAX, INT64_MAX, ContractError, validate_record
from commerce.db import initialize
from commerce.generate import write_dataset
from commerce.ingest import digest, ingest, read_manifest, unresolved_quarantine
from commerce.pipeline import _check_sources, run

ROOT = Path(__file__).resolve().parents[1]
AS_OF = date(2024, 1, 5)


def reference():
    spec = importlib.util.spec_from_file_location("ingestion_edge_reference", ROOT / "fixtures/build_reference.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module.reference_records()


def line(**updates):
    return dict(reference()["order_lines"][4], **updates)


@pytest.fixture(scope="module")
def ingestion_url():
    explicit = os.getenv("COMMERCE_INGESTION_TEST_DATABASE_URL")
    base = explicit or os.getenv("COMMERCE_TEST_DATABASE_URL")
    if not base:
        pytest.skip("No disposable PostgreSQL test credentials configured")
    config = conninfo_to_dict(base)
    name = "commerce_ingestion_fix_test"
    if explicit and config.get("dbname") != name:
        pytest.fail(f"Refusing ingestion edge tests outside {name}")
    if not explicit:
        if not config.get("dbname", "").endswith("_test"):
            pytest.fail("Refusing to derive an ingestion database from a non-test database")
        with psycopg.connect(base, autocommit=True) as conn:
            if not conn.execute("select 1 from pg_database where datname=%s", (name,)).fetchone():
                conn.execute(sql.SQL("create database {}").format(sql.Identifier(name)))
        config["dbname"] = name
    return make_conninfo(**config)


def deliver(conn, manifest, root):
    _, _, verified = read_manifest(manifest)
    run_id = uuid.uuid4().hex
    conn.execute("insert into meta.runs(run_id,status,as_of) values(%s,'running',%s)", (run_id, AS_OF))
    counts = ingest(conn, run_id, verified, root / "raw", AS_OF)
    status = "failed" if counts["rejected"] or counts["file_failures"] else "ingested"
    conn.execute("update meta.runs set status=%s,ended_at=now() where run_id=%s", (status, run_id))
    return run_id, counts


@pytest.fixture
def database(ingestion_url, monkeypatch, tmp_path):
    monkeypatch.setenv("COMMERCE_DATABASE_URL", ingestion_url)
    monkeypatch.setenv("COMMERCE_DATA_DIR", str(tmp_path / "runtime"))
    with psycopg.connect(ingestion_url, autocommit=True, row_factory=dict_row) as conn:
        assert conn.info.dbname == "commerce_ingestion_fix_test"
        for row in conn.execute("""select nspname from pg_namespace where nspname in ('analytics','landing','meta')
          or nspname like 'build\\_%' escape '\\'""").fetchall():
            conn.execute(sql.SQL("drop schema {} cascade").format(sql.Identifier(row["nspname"])))
        initialize(conn)
        deliver(conn, ROOT / "fixtures/reference/manifest.json", tmp_path)
        yield conn, tmp_path


def raw_manifest(root, body, *, source="order_lines", rows=1, schema=1):
    root.mkdir(parents=True, exist_ok=True)
    path = root / "source.bin"
    path.write_bytes(body)
    manifest = root / "manifest.json"
    manifest.write_text(json.dumps({"synthetic": True, "files": [{
        "source": source, "path": path.name, "sha256": digest(body), "row_count": rows,
        "schema_version": schema, "partition": "2024-01-04",
    }]}), encoding="utf-8")
    return manifest


@pytest.mark.integration
@pytest.mark.parametrize("failure", ["headers", "duplicate_headers", "encoding", "surplus", "csv_syntax", "count", "empty_schema"])
def test_failed_files_are_unkeyed_durable_blockers(database, failure):
    conn, root = database
    normal = write_dataset(root / "normal", {"order_lines": [line()]})
    entry = json.loads(normal.read_text())["files"][0]
    body = (normal.parent / entry["path"]).read_bytes()
    header = body.splitlines(keepends=True)[0]
    expected_observed = None
    options = {}
    if failure == "headers":
        body = b"line_id,order_id\nO_NEW,O_NEW\n"
    elif failure == "duplicate_headers":
        body = header.rstrip(b"\n") + b",line_id\n"
    elif failure == "encoding":
        body = b"\xff\xfeinvalid"
    elif failure == "surplus":
        body = body.rstrip(b"\n") + b",extra\n"
    elif failure == "csv_syntax":
        body = header + b'"unterminated\n'
    elif failure == "count":
        options["rows"] = 2
        expected_observed = 1
    else:
        body = b""
        options.update(source="refunds", rows=0, schema=2)
        expected_observed = 0
    manifest = raw_manifest(root / failure, body, **options)
    run_id, counts = deliver(conn, manifest, root)
    assert counts == dict(received=0, accepted=0, rejected=0, changed=0, file_failures=1)
    issue = unresolved_quarantine(conn)[0]
    assert issue["issue_kind"] == "file"
    assert issue["business_key"] is None and issue["source_version"] is None
    evidence = conn.execute("select * from meta.files where run_id=%s", (run_id,)).fetchone()
    assert evidence["observed_rows"] == expected_observed
    assert evidence["file_failures"] == 1
    assert (root / "raw" / issue["raw_path"]).read_bytes() == body
    # Neither a sibling key nor an empty delivery of the same partition can
    # identify which business records were lost in a failed file.
    deliver(conn, normal, root)
    empty = raw_manifest(root / "empty", header, rows=0)
    deliver(conn, empty, root)
    assert [q["id"] for q in unresolved_quarantine(conn)] == [issue["id"]]
    with pytest.raises(ValueError, match="Unresolved quarantined"):
        _check_sources(conn, AS_OF)
    quarantine("resolve", id=issue["id"], reason="Producer confirmed disposition of the failed file")
    assert not unresolved_quarantine(conn)


@pytest.mark.integration
@pytest.mark.parametrize("bad_first", [True, False])
def test_failed_file_does_not_rollback_clean_sibling(database, bad_first):
    conn, root = database
    bad = raw_manifest(root / "bundle", b"wrong,headers\nrow,data\n")
    good = write_dataset(root / "clean", {"inventory": [dict(reference()["inventory"][3], version=2, actual_quantity=6)]})
    document = json.loads(bad.read_text())
    entry = json.loads(good.read_text())["files"][0]
    (bad.parent / "good.jsonl").write_bytes((good.parent / entry["path"]).read_bytes())
    document["files"].append(dict(entry, path="good.jsonl"))
    if not bad_first:
        document["files"].reverse()
    bad.write_text(json.dumps(document))
    _, counts = deliver(conn, bad, root)
    assert counts == dict(received=1, accepted=1, rejected=0, changed=1, file_failures=1)
    assert conn.execute("""select payload->>'actual_quantity' qty from landing.records
      where source='inventory' and business_key=%s""", (reference()["inventory"][3]["snapshot_id"],)).fetchone()["qty"] == "6"
    assert unresolved_quarantine(conn)[0]["issue_kind"] == "file"


@pytest.mark.integration
def test_pipeline_replay_cannot_bypass_failed_file(database):
    conn, root = database
    bad = raw_manifest(root / "bad", b"line_id,order_id\nO_NEW,O_NEW\n")
    with pytest.raises(ValueError, match="file|quarantined"):
        run(bad, as_of=AS_OF)
    with pytest.raises(ValueError, match="Unresolved quarantined"):
        run(ROOT / "fixtures/reference/manifest.json", as_of=AS_OF)
    assert conn.execute("select count(*) n from meta.publications").fetchone()["n"] == 0
    assert len(unresolved_quarantine(conn)) == 1


@pytest.mark.integration
@pytest.mark.parametrize("bad", ["\x00", "\ud800", "\udfff"])
@pytest.mark.parametrize("where", ["metadata", "metadata_key", "business_key", "literal_json"])
def test_unrepresentable_json_is_safely_quarantined(database, bad, where):
    conn, root = database
    row = dict(reference()["refunds"][2], version=2)
    if where == "business_key":
        row["refund_id"] += bad
    elif where == "metadata_key":
        row["extra"] = {"unsafe" + bad: "value"}
    else:
        row["extra"] = {"nested": ["unsafe" + bad]}
    body = json.dumps(row, ensure_ascii=True).encode() + b"\n"
    if where == "literal_json":
        # Literal NUL is malformed JSON; literal surrogates are invalid UTF-8.
        body = body.replace(json.dumps(bad)[1:-1].encode(), bad.encode("utf-8", "surrogatepass"))
    manifest = raw_manifest(root / "unsafe", body, source="refunds")
    run_id, counts = deliver(conn, manifest, root)
    issue = unresolved_quarantine(conn)[0]
    assert counts["changed"] == 0
    if where == "literal_json" and bad != "\x00":
        assert counts["file_failures"] == 1
    else:
        assert counts["rejected"] == 1
        stored = conn.execute("select payload from meta.quarantine where run_id=%s", (run_id,)).fetchone()["payload"]
        assert stored["_quarantine_encoding"] == "json-ascii"
        assert stored["_quarantine_payload"].isascii()
    assert (root / "raw" / issue["raw_path"]).read_bytes() == body
    deliver(conn, ROOT / "fixtures/reference/manifest.json", root)
    assert unresolved_quarantine(conn)
    with pytest.raises(ValueError, match="Unresolved quarantined"):
        _check_sources(conn, AS_OF)


@pytest.mark.parametrize("source,field,value", [
    ("order_lines", "version", INT32_MAX + 1),
    ("order_lines", "quantity", INT64_MAX + 1),
    ("order_lines", "unit_price_cents", INT64_MAX + 1),
    ("order_lines", "discount_cents", INT64_MAX + 1),
    ("refunds", "amount_cents", INT64_MAX + 1),
    ("inventory", "actual_quantity", INT64_MAX + 1),
    ("inventory", "expected_quantity", INT64_MAX + 1),
    ("products", "version", INT32_MAX + 1),
    ("locations", "version", INT32_MAX + 1),
])
def test_contracts_reject_storage_overflow(source, field, value):
    row = dict(reference()[source][0], **{field: value})
    with pytest.raises(ContractError, match=field):
        validate_record(source, row)


def test_valid_integer_boundaries_and_gross_arithmetic():
    row = dict(reference()["inventory"][0], version=INT32_MAX, actual_quantity=INT64_MAX)
    assert validate_record("inventory", row)[0] == row
    with pytest.raises(ContractError, match="quantity .* unit_price_cents"):
        validate_record("order_lines", line(quantity=INT64_MAX, unit_price_cents=2))


@pytest.mark.integration
@pytest.mark.parametrize("source,updates", [
    ("order_lines", {"version": INT32_MAX + 1}),
    ("order_lines", {"version": 2, "quantity": INT64_MAX, "unit_price_cents": 2}),
    ("inventory", {"version": 2, "actual_quantity": INT64_MAX + 1}),
    ("refunds", {"version": 2, "amount_cents": INT64_MAX + 1}),
])
def test_overflow_never_poisons_canonical_records(database, source, updates):
    conn, root = database
    before = conn.execute("select source,business_key,version,payload from landing.records order by source,business_key").fetchall()
    manifest = write_dataset(root / "overflow", {source: [dict(reference()[source][0], **updates)]}, validate=False)
    _, counts = deliver(conn, manifest, root)
    assert counts["rejected"] == 1 and counts["changed"] == 0
    assert conn.execute("select source,business_key,version,payload from landing.records order by source,business_key").fetchall() == before
    assert unresolved_quarantine(conn)[0]["issue_kind"] == "record"


@pytest.mark.integration
@pytest.mark.parametrize("prices", [(1000, 1200), (1200, 1000)])
def test_same_version_conflict_requires_newer_version_in_both_orders(database, prices):
    conn, root = database
    rows = [line(version=2, unit_price_cents=price) for price in prices]
    manifest = write_dataset(root / "conflict", {"order_lines": rows})
    _, counts = deliver(conn, manifest, root)
    assert counts["accepted"] == 1 and counts["rejected"] == 1
    issue = unresolved_quarantine(conn)[0]
    assert issue["issue_kind"] == "conflict" and issue["source_version"] == 2
    deliver(conn, write_dataset(root / "replay", {"order_lines": [rows[0]]}), root)
    assert unresolved_quarantine(conn)[0]["id"] == issue["id"]
    with pytest.raises(ValueError, match="Unresolved quarantined"):
        _check_sources(conn, AS_OF)
    newer = dict(rows[1], version=3)
    deliver(conn, write_dataset(root / "newer", {"order_lines": [newer]}), root)
    assert not unresolved_quarantine(conn)
    retained = conn.execute("select * from meta.quarantine where id=%s", (issue["id"],)).fetchone()
    assert retained["resolved_at"] is not None


@pytest.mark.integration
@pytest.mark.parametrize("delivery_order", list(itertools.permutations(("first", "conflict", "newer"))))
def test_newer_record_cannot_hide_conflicting_versions_in_a_bundle(database, delivery_order):
    conn, root = database
    rows = {
        "first": line(version=2, unit_price_cents=1000),
        "conflict": line(version=2, unit_price_cents=1200),
        "newer": line(version=3, unit_price_cents=1100),
    }
    manifest = write_dataset(root / "mixed_versions", {"order_lines": [rows[name] for name in delivery_order]})
    _, counts = deliver(conn, manifest, root)
    assert counts["rejected"] == 1
    canonical = conn.execute("""select version,payload->>'unit_price_cents' price from landing.records
      where source='order_lines' and business_key='O004_1'""").fetchone()
    assert canonical == {"version": 3, "price": "1100"}
    deliver(conn, write_dataset(root / "newer_replay", {"order_lines": [rows["newer"]]}), root)
    assert not unresolved_quarantine(conn)


@pytest.mark.integration
@pytest.mark.parametrize("legacy_hash", [False, True])
def test_historical_replay_preserves_integral_float_metadata(database, legacy_hash):
    conn, root = database
    older = dict(reference()["inventory"][3], version=2, ratio=1e20)
    newer = dict(older, version=3)
    for label, row in (("older", older), ("newer", newer)):
        deliver(conn, write_dataset(root / label, {"inventory": [row]}), root)
    if legacy_hash:
        conn.execute("update meta.source_changes set after_payload_hash=null")
    _, counts = deliver(conn, write_dataset(root / "replay", {"inventory": [older]}), root)
    assert counts["accepted"] == 1 and counts["changed"] == counts["rejected"] == 0
    assert not unresolved_quarantine(conn)
    conflict = dict(older, ratio=True)
    _, counts = deliver(conn, write_dataset(root / "conflict", {"inventory": [conflict]}), root)
    assert counts["rejected"] == 1
    assert unresolved_quarantine(conn)[0]["issue_kind"] == "conflict"


@pytest.mark.integration
def test_legacy_conflict_migrates_to_strict_resolution(database):
    conn, root = database
    run_id = uuid.uuid4().hex
    conn.execute("insert into meta.runs(run_id,status) values(%s,'failed')", (run_id,))
    conn.execute("""insert into meta.quarantine(run_id,source,raw_path,row_number,payload,reason)
      values(%s,'order_lines','legacy/raw',1,%s,'Conflicting payload at the same source version; increment version')""",
      (run_id, Jsonb(line(version=1, unit_price_cents=1200))))
    initialize(conn)
    issue = unresolved_quarantine(conn)[0]
    assert issue["issue_kind"] == "conflict" and issue["source_version"] == 1
    deliver(conn, write_dataset(root / "old_replay", {"order_lines": [line() | {"version": 1}]}), root)
    assert unresolved_quarantine(conn)[0]["id"] == issue["id"]
    deliver(conn, write_dataset(root / "new_version", {"order_lines": [line(version=2)]}), root)
    assert not unresolved_quarantine(conn)


@pytest.mark.integration
def test_database_failures_are_not_misclassified_as_data_rejections(database):
    conn, root = database
    manifest = write_dataset(root / "normal", {"order_lines": [line(version=2)]})
    _, _, verified = read_manifest(manifest)
    run_id = uuid.uuid4().hex
    conn.execute("insert into meta.runs(run_id,status) values(%s,'running')", (run_id,))

    class BrokenConnection:
        def transaction(self):
            return conn.transaction()

        def execute(self, query, parameters=None):
            if query.startswith("select * from landing.records"):
                raise psycopg.OperationalError("Injected connection failure")
            return conn.execute(query, parameters)

    with pytest.raises(psycopg.OperationalError, match="Injected connection failure"):
        ingest(BrokenConnection(), run_id, verified, root / "raw", AS_OF)
    assert not unresolved_quarantine(conn)
