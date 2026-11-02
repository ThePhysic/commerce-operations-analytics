"""Actual PostgreSQL + dbt correctness scenarios against a disposable test database.

Set COMMERCE_TEST_DATABASE_URL to a database whose name ends in ``_test``.
The fixture deletes project schemas there; it never touches another database.
"""
from __future__ import annotations

import copy
import hashlib
import importlib.util
import json
import os
import uuid
from datetime import date
from pathlib import Path

import psycopg
import pytest
from psycopg import sql
from psycopg.conninfo import conninfo_to_dict
from psycopg.rows import dict_row

from commerce.generate import write_dataset
from commerce.cli import query
from commerce.db import locked_connection
from commerce.ingest import ingest, read_manifest
from commerce.pipeline import _check_sources
from commerce.pipeline import run

pytestmark = pytest.mark.integration
ROOT = Path(__file__).resolve().parents[1]
AS_OF = date(2024, 1, 5)
MARTS = (
    "mart_daily_location_sales", "mart_refund_rate", "mart_fulfillment_sla",
    "mart_inventory_variance", "mart_refunds_by_event_date",
)


def records():
    spec = importlib.util.spec_from_file_location("reference_fixture", ROOT / "fixtures" / "build_reference.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module.reference_records()


@pytest.fixture
def database(monkeypatch, tmp_path):
    url = os.getenv("COMMERCE_TEST_DATABASE_URL")
    if not url:
        pytest.skip("COMMERCE_TEST_DATABASE_URL is not configured")
    dbname = conninfo_to_dict(url).get("dbname", "")
    if not dbname.endswith("_test"):
        pytest.fail("Refusing to reset a database whose name does not end in _test")
    monkeypatch.setenv("COMMERCE_DATABASE_URL", url)
    monkeypatch.setenv("COMMERCE_DATA_DIR", str(tmp_path / "runtime"))
    with psycopg.connect(url, autocommit=True, row_factory=dict_row) as conn:
        assert conn.info.dbname == dbname
        for row in conn.execute("""select nspname from pg_namespace
          where nspname in ('landing','meta','analytics') or nspname like 'build\\_%' escape '\\'""").fetchall():
            conn.execute(sql.SQL("drop schema {} cascade").format(sql.Identifier(row["nspname"])))
        yield conn


def snapshot(conn):
    """Compare complete business outputs, including dates with empty percentiles."""
    return {
        name: conn.execute(sql.SQL("select to_jsonb(t) as row from analytics.{} t order by to_jsonb(t)::text").format(
            sql.Identifier(name))).fetchall()
        for name in MARTS
    }


def publication_count(conn):
    return conn.execute("select count(*) as n from meta.publications").fetchone()["n"]


def test_reference_answers_history_and_idempotent_replay(database):
    conn = database
    manifest = ROOT / "fixtures" / "reference" / "manifest.json"
    expected = json.loads((manifest.parent / "expected.json").read_text())
    result = run(manifest, as_of=AS_OF)
    for model, count in expected["fact_counts"].items():
        assert result["row_counts"][model] == count
    actual = conn.execute("""select sale_date::text,location_id,gross_sales_cents,
      discount_cents,refund_cents,net_sales_cents from analytics.mart_daily_location_sales
      order by sale_date,location_id""").fetchall()
    assert actual == expected["sales"]
    inventory = conn.execute("""select snapshot_date::text,location_id,
      count(*) as snapshots,sum(stockout_count) as stockouts,sum(variance_quantity) as variance_quantity
      from analytics.mart_inventory_variance group by snapshot_date,location_id
      order by snapshot_date,location_id""").fetchall()
    assert inventory == expected["inventory"]
    events = conn.execute("""select refund_date::text,sum(refund_cents) as refund_cents
      from analytics.mart_refunds_by_event_date group by refund_date order by refund_date""").fetchall()
    assert events == expected["refund_events"]
    for answer in expected["refund_rates"]:
        rate = conn.execute("""select paid_orders,refunded_orders,refund_rate
          from analytics.mart_refund_rate where sale_date=%s and location_id=%s""",
          (answer["sale_date"], answer["location_id"])).fetchone()
        assert rate["paid_orders"] == answer["paid_orders"]
        assert rate["refunded_orders"] == answer["refunded_orders"]
        assert float(rate["refund_rate"]) == pytest.approx(answer["refund_rate"])
    assert conn.execute("""select cancellation_rate from analytics.mart_refund_rate
      where sale_date='2024-01-01' and location_id='L002'""").fetchone()["cancellation_rate"] == 1
    for answer in expected["fulfillment"]:
        row = conn.execute("""select fulfilled_orders,median_fulfillment_minutes as median_minutes,
          p95_fulfillment_minutes as p95_minutes from analytics.mart_fulfillment_sla
          where sale_date=%s and location_id=%s""", (answer["sale_date"], answer["location_id"])).fetchone()
        assert row["fulfilled_orders"] == answer["fulfilled_orders"]
        assert row["median_minutes"] == pytest.approx(answer["median_minutes"])
        assert row["p95_minutes"] == pytest.approx(answer["p95_minutes"])
    history = conn.execute("""select l.sale_date::text,p.category,d.region
      from analytics.fct_order_line l join analytics.dim_product p using(product_key)
      join analytics.dim_location d using(location_key) where l.line_id in ('O001_1','O004_1')
      order by l.sale_date""").fetchall()
    assert history == [
        {"sale_date": "2024-01-01", "category": "Home", "region": "North"},
        {"sale_date": "2024-01-04", "category": "Gifts", "region": "Central"},
    ]
    before = snapshot(conn)
    source_changes = conn.execute("select count(*) n from meta.source_changes").fetchone()["n"]
    replay = run(manifest, as_of=AS_OF)
    assert replay["ingestion"]["changed"] == 0
    assert not replay["full_refresh"]
    assert snapshot(conn) == before
    assert conn.execute("select count(*) n from meta.source_changes").fetchone()["n"] == source_changes
    assert publication_count(conn) == 2
    assert conn.execute("select count(*) n from meta.runs where status='published'").fetchone()["n"] == 2


def test_late_refund_bounded_window_backfill_and_full_rebuild_parity(database, tmp_path):
    conn = database
    data = records()
    refunds = data["refunds"]
    # Preserve the original reporting horizon and its empty-partition evidence,
    # but certify that the refund feed has delivered no events yet.
    initial = write_dataset(tmp_path / "initial", data)
    initial_manifest = json.loads(initial.read_text())
    for entry in initial_manifest["files"]:
        if entry["source"] == "refunds":
            (initial.parent / entry["path"]).write_bytes(b"")
            entry["sha256"] = hashlib.sha256(b"").hexdigest()
            entry["row_count"] = 0
    initial.write_text(json.dumps(initial_manifest))
    run(initial, as_of=AS_OF)
    before = snapshot(conn)
    assert conn.execute("select sum(net_sales_cents) n from analytics.mart_daily_location_sales").fetchone()["n"] == 8400
    late = write_dataset(tmp_path / "late", {"refunds": refunds})
    with pytest.raises(ValueError, match="outside lookback"):
        run(late, as_of=AS_OF, lookback_days=0)
    assert snapshot(conn) == before
    assert publication_count(conn) == 1
    backfill = run(late, start=date(2024, 1, 1), end=date(2024, 1, 1))
    assert not backfill["full_refresh"]
    corrected = snapshot(conn)
    assert conn.execute("select sum(net_sales_cents) n from analytics.mart_daily_location_sales").fetchone()["n"] == 6900
    assert conn.execute("select count(*) n from analytics.fct_refund").fetchone()["n"] == 3
    changes = query("changes")
    assert sum(delta["delta_cents"] for delta in changes["sales_deltas"]) == -1500
    assert {change["source"] for change in changes["source_changes"]} == {"refunds"}
    assert {change["business_key"] for change in changes["source_changes"]} == {r["refund_id"] for r in refunds}
    assert all(change["raw_path"] and change["before_payload"] is None for change in changes["source_changes"])
    assert changes["row_count_deltas"]["fct_refund"] == len(refunds) == 3
    run(late, as_of=AS_OF, full_refresh=True)
    assert snapshot(conn) == corrected
    assert publication_count(conn) == 3


def test_schema_drift_quarantine_version_conflicts_and_retry(database, tmp_path):
    conn = database
    run(ROOT / "fixtures" / "reference" / "manifest.json", as_of=AS_OF)
    before = snapshot(conn)
    row = copy.deepcopy(records()["order_lines"][4])
    row.update(version=2, producer_note="compatible additive synthetic field")
    additive = write_dataset(tmp_path / "additive", {"order_lines": [row]})
    run(additive, as_of=AS_OF)
    assert snapshot(conn) == before
    assert conn.execute("""select payload->>'producer_note' as note from landing.records
      where source='order_lines' and business_key='O004_1'""").fetchone()["note"] == row["producer_note"]
    conflict = dict(row, unit_price_cents=1200)
    with pytest.raises(ValueError, match="quarantined"):
        run(write_dataset(tmp_path / "conflict", {"order_lines": [conflict]}), as_of=AS_OF)
    assert snapshot(conn) == before
    assert "same source version" in conn.execute("select reason from meta.quarantine order by id desc limit 1").fetchone()["reason"]
    invalid = dict(row, version=3, quantity="not-an-integer")
    with pytest.raises(ValueError, match="quarantined"):
        run(write_dataset(tmp_path / "bad_type", {"order_lines": [invalid]}, validate=False), as_of=AS_OF)
    assert snapshot(conn) == before
    quarantine = conn.execute("select payload,raw_path,reason from meta.quarantine order by id desc limit 1").fetchone()
    assert quarantine["payload"]["quantity"] == "not-an-integer"
    assert quarantine["raw_path"] and quarantine["reason"]
    unrelated = write_dataset(tmp_path / "unrelated", {"inventory": [records()["inventory"][3]]})
    with pytest.raises(ValueError, match="Unresolved quarantined"):
        run(unrelated, as_of=AS_OF)
    assert snapshot(conn) == before
    assert publication_count(conn) == 2
    assert conn.execute("select dbt_results from meta.runs order by started_at desc limit 1").fetchone()["dbt_results"] is None
    fixed = dict(row, version=3)
    run(write_dataset(tmp_path / "fixed", {"order_lines": [fixed]}), as_of=AS_OF)
    assert snapshot(conn) == before
    breaking = json.loads(additive.read_text())
    breaking["files"][0]["schema_version"] = 2
    additive.write_text(json.dumps(breaking))
    runs_before = conn.execute("select count(*) n from meta.runs").fetchone()["n"]
    with pytest.raises(ValueError, match="quarantined"):
        run(additive, as_of=AS_OF)
    assert conn.execute("select count(*) n from meta.runs").fetchone()["n"] == runs_before + 1
    broken = conn.execute("select run_id,status from meta.runs order by started_at desc limit 1").fetchone()
    assert broken["status"] == "failed"
    evidence = conn.execute("select raw_path,reason from meta.quarantine where run_id=%s", (broken["run_id"],)).fetchone()
    assert "unsupported schema version" in evidence["reason"]
    assert (Path(os.environ["COMMERCE_DATA_DIR"]) / "raw" / evidence["raw_path"]).is_file()
    assert snapshot(conn) == before


def test_failure_recovery_preserves_last_good_publication(database, tmp_path):
    conn = database
    run(ROOT / "fixtures" / "reference" / "manifest.json", as_of=AS_OF)
    before = snapshot(conn)
    first_schema = conn.execute("select schema_name from meta.publications").fetchone()["schema_name"]
    first_views = conn.execute("select viewname,definition from pg_views where schemaname='analytics' order by viewname").fetchall()
    row = dict(records()["order_lines"][4], version=2, unit_price_cents=1200)
    correction = write_dataset(tmp_path / "correction", {"order_lines": [row]})
    for stage in ("after-ingest", "before-publish", "during-publish"):
        with pytest.raises(RuntimeError, match="Injected failure"):
            run(correction, as_of=AS_OF, fail_at=stage)
        assert snapshot(conn) == before
        assert publication_count(conn) == 1
        assert conn.execute("select schema_name from meta.publications").fetchone()["schema_name"] == first_schema
        assert conn.execute("select viewname,definition from pg_views where schemaname='analytics' order by viewname").fetchall() == first_views
    assert conn.execute("select count(*) n from meta.runs where status='failed'").fetchone()["n"] == 3
    recovered = run(correction)
    assert not recovered["full_refresh"]
    assert recovered["ingestion"]["changed"] == 0  # the first attempt durably accepted the correction
    assert conn.execute("select sum(net_sales_cents) n from analytics.mart_daily_location_sales").fetchone()["n"] == 7100
    assert publication_count(conn) == 2
    pending = conn.execute("""select count(*) n from meta.source_changes c where not exists
      (select 1 from meta.publication_sources p where p.source_change_id=c.id)""").fetchone()["n"]
    assert pending == 0


def test_stale_and_business_quality_failures_block_publication(database, tmp_path):
    conn = database
    manifest = ROOT / "fixtures" / "reference" / "manifest.json"
    run(manifest, as_of=AS_OF)
    before = snapshot(conn)
    with pytest.raises(ValueError, match="Stale source"):
        run(manifest, as_of=date(2024, 1, 6))
    assert snapshot(conn) == before
    orphan = dict(refund_id="R_BAD", line_id="NO_SUCH_LINE", refund_date="2024-01-05", amount_cents=1, version=1)
    with pytest.raises(RuntimeError, match="dbt build failed"):
        run(write_dataset(tmp_path / "orphan", {"refunds": [orphan]}), as_of=AS_OF, full_refresh=True)
    assert snapshot(conn) == before
    assert publication_count(conn) == 1
    latest = conn.execute("select status,dbt_results from meta.runs order by started_at desc limit 1").fetchone()
    assert latest["status"] == "failed"
    assert any(result["status"] == "fail" for result in latest["dbt_results"])


def test_date_moving_correction_replaces_empty_old_partition(database, tmp_path):
    conn = database
    run(ROOT / "fixtures" / "reference" / "manifest.json", as_of=AS_OF)
    before = snapshot(conn)
    moved = dict(records()["order_lines"][5], version=2,
                 sale_date="2024-01-05", paid_at="2024-01-05T12:00:00+00:00")
    correction = write_dataset(tmp_path / "moved", {"order_lines": [moved]})
    with pytest.raises(ValueError, match="old/new dates|both old and new dates"):
        run(correction, as_of=AS_OF, start=AS_OF, end=AS_OF)
    assert snapshot(conn) == before
    assert publication_count(conn) == 1
    run(correction, as_of=AS_OF, start=date(2024, 1, 4), end=AS_OF)
    assert conn.execute("""select count(*) n from analytics.mart_daily_location_sales
      where sale_date='2024-01-04' and location_id='L002'""").fetchone()["n"] == 0
    assert conn.execute("""select net_sales_cents from analytics.mart_daily_location_sales
      where sale_date='2024-01-05' and location_id='L002'""").fetchone()["net_sales_cents"] == 2500
    assert conn.execute("""select sale_date::text from analytics.fct_fulfillment
      where order_id='O005'""").fetchone()["sale_date"] == "2024-01-05"
    incremental = snapshot(conn)
    run(correction, as_of=AS_OF, full_refresh=True)
    assert snapshot(conn) == incremental


def test_quality_gate_aggregates_all_partition_shards_and_requires_clean_redelivery(database, tmp_path):
    """Exercise the source gate directly: no dbt builds are needed for this failure."""
    bad_row = dict(records()["order_lines"][4], version=2, quantity="invalid")
    bad = write_dataset(tmp_path / "shards", {"order_lines": [bad_row]}, validate=False)
    good = write_dataset(tmp_path / "sibling", {"order_lines": [records()["order_lines"][5]]})
    bad_manifest = json.loads(bad.read_text())
    good_entry = json.loads(good.read_text())["files"][0]
    sibling_bytes = (good.parent / good_entry["path"]).read_bytes()
    sibling_path = "order_lines/clean_sibling.csv"
    (bad.parent / sibling_path).write_bytes(sibling_bytes)
    bad_manifest["files"].append(dict(good_entry, path=sibling_path))
    bad.write_text(json.dumps(bad_manifest))

    with locked_connection() as conn:
        def deliver(manifest):
            run_id = uuid.uuid4().hex
            conn.execute("insert into meta.runs(run_id,status,as_of) values(%s,'failed',%s)", (run_id, AS_OF))
            _, _, verified = read_manifest(manifest)
            return ingest(conn, run_id, verified, tmp_path / "gate_raw", AS_OF)

        deliver(ROOT / "fixtures" / "reference" / "manifest.json")
        _check_sources(conn, AS_OF)
        assert deliver(bad)["rejected"] == 1
        with pytest.raises(ValueError, match="Unresolved quarantined"):
            _check_sources(conn, AS_OF)
        unrelated = write_dataset(tmp_path / "gate_unrelated", {"inventory": [records()["inventory"][3]]})
        deliver(unrelated)
        with pytest.raises(ValueError, match="Unresolved quarantined"):
            _check_sources(conn, AS_OF)
        clean = write_dataset(tmp_path / "gate_fixed", {"order_lines": [dict(bad_row, quantity=1)]})
        assert deliver(clean)["rejected"] == 0
        _check_sources(conn, AS_OF)
        future = dict(bad_row, quantity=1, version=3, fulfilled_at="2024-01-06T00:00:00+00:00")
        future_manifest = write_dataset(tmp_path / "gate_future", {"order_lines": [future]})
        assert deliver(future_manifest)["rejected"] == 1
        canonical = conn.execute("""select version,payload->>'fulfilled_at' as fulfilled_at
          from landing.records where source='order_lines' and business_key='O004_1'""").fetchone()
        assert canonical == {"version": 2, "fulfilled_at": "2024-01-04T11:30:00+00:00"}
        reason = conn.execute("select reason from meta.quarantine order by id desc limit 1").fetchone()["reason"]
        assert "lifecycle timestamp" in reason and "as_of" in reason
