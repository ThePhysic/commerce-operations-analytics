"""Reporting horizons must include all durable accepted state after failed runs."""
from __future__ import annotations

import copy
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

from commerce.db import locked_connection
from commerce.generate import write_dataset
from commerce.ingest import ingest, read_manifest
from commerce.pipeline import accepted_horizon, run

pytestmark = pytest.mark.integration
ROOT = Path(__file__).resolve().parents[1]
REFERENCE = ROOT / "fixtures" / "reference" / "manifest.json"
JAN5 = date(2024, 1, 5)
JAN6 = date(2024, 1, 6)


def reference_records():
    spec = importlib.util.spec_from_file_location("horizon_reference", ROOT / "fixtures" / "build_reference.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module.reference_records()


@pytest.fixture
def database(monkeypatch, tmp_path):
    url = os.getenv("COMMERCE_HORIZON_TEST_DATABASE_URL") or os.getenv("COMMERCE_TEST_DATABASE_URL")
    if not url:
        pytest.skip("A disposable COMMERCE_TEST_DATABASE_URL is not configured")
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


def stable_publication(conn):
    return {
        "publications": conn.execute("select publication_id,as_of,schema_name from meta.publications order by publication_id").fetchall(),
        "views": conn.execute("select viewname,definition from pg_views where schemaname='analytics' order by viewname").fetchall(),
        "sales": conn.execute("select to_jsonb(t) as row from analytics.mart_daily_location_sales t order by sale_date,location_id").fetchall(),
        "refunds": conn.execute("select refund_id,refund_date,amount_cents from analytics.fct_refund order by refund_id").fetchall(),
    }


def empty_daily_delivery(output, day):
    """Certify all three daily feeds through a later day without inventing rows."""
    manifest = write_dataset(output, {source: [] for source in ("order_lines", "inventory", "refunds")})
    document = json.loads(manifest.read_text())
    for entry in document["files"]:
        original = manifest.parent / entry["path"]
        relative = Path(entry["source"]) / f"{day.isoformat()}{original.suffix}"
        destination = manifest.parent / relative
        destination.write_bytes(original.read_bytes())
        if original != destination:
            original.unlink()
        entry.update(path=relative.as_posix(), partition=day.isoformat())
    manifest.write_text(json.dumps(document))
    return manifest


def test_failed_future_refund_cannot_publish_under_earlier_incremental_or_full_as_of(database, tmp_path):
    conn = database
    baseline = run(REFERENCE, as_of=JAN5)
    before = stable_publication(conn)
    future = dict(refund_id="R_FUTURE", line_id="O004_1", refund_date="2024-01-06", amount_cents=100, version=1)
    manifest = write_dataset(tmp_path / "future_refund", {"refunds": [future]})
    with pytest.raises(RuntimeError, match="after-ingest"):
        run(manifest, as_of=JAN6, fail_at="after-ingest")
    assert accepted_horizon(conn) == JAN6
    assert conn.execute("select count(*) n from landing.records where source='refunds'").fetchone()["n"] == 4
    assert stable_publication(conn) == before
    for full_refresh in (False, True):
        with pytest.raises(ValueError, match="precedes accepted source horizon"):
            run(REFERENCE, as_of=JAN5, full_refresh=full_refresh)
        assert stable_publication(conn) == before
    # An implicit retry must advance its horizon, then stop at the freshness gate
    # because the other feeds have not certified January 6 yet.
    with pytest.raises(ValueError, match="Stale source"):
        run(REFERENCE)
    assert conn.execute("select as_of from meta.runs order by started_at desc limit 1").fetchone()["as_of"] == JAN6
    assert stable_publication(conn) == before
    certified = empty_daily_delivery(tmp_path / "certify_jan6", JAN6)
    recovery = run(certified)
    assert not recovery["full_refresh"]
    assert conn.execute("select as_of from meta.publications order by publication_id desc limit 1").fetchone()["as_of"] == JAN6
    assert conn.execute("select sum(net_sales_cents) n from analytics.mart_daily_location_sales").fetchone()["n"] == 6800
    assert conn.execute("select count(*) n from analytics.fct_refund where refund_id='R_FUTURE'").fetchone()["n"] == 1
    assert conn.execute("select count(*) n from analytics.fct_refund").fetchone()["n"] == 4
    corrected = stable_publication(conn)
    # A full rebuild from the older manifest retains the accepted-state floor,
    # consumes the same canonical inputs, and must not duplicate the refund.
    run(REFERENCE, full_refresh=True)
    rebuilt = stable_publication(conn)
    assert rebuilt["sales"] == corrected["sales"]
    assert rebuilt["refunds"] == corrected["refunds"]
    assert rebuilt["publications"][-1]["as_of"] == JAN6
    assert conn.execute(sql.SQL("select sum(net_sales_cents) n from {}.mart_daily_location_sales").format(
        sql.Identifier(baseline["schema"]))).fetchone()["n"] == 6900


def test_lifecycle_and_catalog_dates_advance_durable_floor_without_publication(database, tmp_path):
    conn = database
    # Seed accepted records directly: these checks stop before dbt and need no
    # analytical rebuild to demonstrate the accepted-state horizon invariant.
    with locked_connection() as writer:
        initial_id = uuid.uuid4().hex
        writer.execute("insert into meta.runs(run_id,status,as_of) values(%s,'failed',%s)", (initial_id, JAN5))
        _, _, verified = read_manifest(REFERENCE)
        ingest(writer, initial_id, verified, tmp_path / "seed_raw", JAN5)
    assert accepted_horizon(conn) == JAN5
    lifecycle = dict(reference_records()["order_lines"][5], version=2,
                     status="fulfilled", fulfilled_at="2024-01-06T12:00:00+00:00")
    with pytest.raises(RuntimeError, match="after-ingest"):
        run(write_dataset(tmp_path / "lifecycle", {"order_lines": [lifecycle]}), as_of=JAN6, fail_at="after-ingest")
    # sale_date remains January 4; only fulfilled_at forces the January 6 floor.
    assert accepted_horizon(conn) == JAN6
    with pytest.raises(ValueError, match="precedes accepted source horizon"):
        run(REFERENCE, as_of=JAN5)
    with pytest.raises(ValueError, match="Stale source"):
        run(REFERENCE)
    assert conn.execute("select as_of from meta.runs order by started_at desc limit 1").fetchone()["as_of"] == JAN6
    catalog = copy.deepcopy(reference_records()["products"][0])
    catalog.update(effective_at="2024-01-07T00:00:00+00:00", category="Future synthetic category")
    jan7 = date(2024, 1, 7)
    with pytest.raises(RuntimeError, match="after-ingest"):
        run(write_dataset(tmp_path / "catalog", {"products": [catalog]}), as_of=jan7, fail_at="after-ingest")
    assert accepted_horizon(conn) == jan7
    with pytest.raises(ValueError, match="precedes accepted source horizon"):
        run(REFERENCE, as_of=JAN6, full_refresh=True)
    with pytest.raises(ValueError, match="Stale source"):
        run(REFERENCE)
    assert conn.execute("select as_of from meta.runs order by started_at desc limit 1").fetchone()["as_of"] == jan7
    assert conn.execute("select count(*) n from meta.publications").fetchone()["n"] == 0


def test_failed_future_requested_date_without_future_accepted_data_does_not_poison_horizon(database):
    conn = database
    run(REFERENCE, as_of=JAN5)
    before = stable_publication(conn)
    with pytest.raises(ValueError, match="Stale source"):
        run(REFERENCE, as_of=date(2024, 2, 1))
    assert accepted_horizon(conn) == JAN5
    assert stable_publication(conn) == before
    recovered = run(REFERENCE)
    assert recovered["ingestion"]["changed"] == 0
    after = stable_publication(conn)
    assert after["publications"][-1]["as_of"] == JAN5
    assert after["sales"] == before["sales"]
    assert after["refunds"] == before["refunds"]
