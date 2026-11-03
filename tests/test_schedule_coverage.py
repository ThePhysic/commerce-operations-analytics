"""A missing first scheduled delivery cannot silently redefine complete history."""
from __future__ import annotations

import importlib.util
import os
from datetime import date, timedelta
from pathlib import Path

import psycopg
import pytest
from psycopg import sql
from psycopg.conninfo import conninfo_to_dict
from psycopg.rows import dict_row

from commerce.generate import generate
from commerce.pipeline import run

ROOT = Path(__file__).resolve().parents[1]
pytestmark = pytest.mark.integration
JAN1, JAN2 = date(2024, 1, 1), date(2024, 1, 2)


@pytest.fixture
def database(monkeypatch, tmp_path):
    url = os.getenv("COMMERCE_SCHEDULE_TEST_DATABASE_URL") or os.getenv("COMMERCE_TEST_DATABASE_URL")
    if not url:
        pytest.skip("A disposable PostgreSQL test database is not configured")
    dbname = conninfo_to_dict(url).get("dbname", "")
    if not dbname.endswith("_test"):
        pytest.fail("Refusing to reset a database not ending in _test")
    monkeypatch.setenv("COMMERCE_DATABASE_URL", url)
    monkeypatch.setenv("COMMERCE_DATA_DIR", str(tmp_path / "runtime"))
    with psycopg.connect(url, autocommit=True, row_factory=dict_row) as conn:
        for row in conn.execute("""select nspname from pg_namespace where
          nspname in ('landing','meta','analytics') or nspname like 'build\\_%' escape '\\'"""):
            conn.execute(sql.SQL("drop schema {} cascade").format(sql.Identifier(row["nspname"])))
        yield conn


def stage(tmp_path):
    spec = importlib.util.spec_from_file_location("schedule_stage", ROOT / "scripts" / "stage_intervals.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    manifest = generate(tmp_path / "bundle", days=2, orders_per_day=4)
    destination = tmp_path / "scheduled"
    module.stage(manifest, destination)
    return destination


def scheduled_run(manifest, logical_date):
    return run(manifest, start=logical_date-timedelta(days=7), end=logical_date, expected_start=logical_date)


def test_missing_first_interval_blocks_later_publication_and_allows_old_interval_recovery(database, tmp_path):
    conn = database
    scheduled = stage(tmp_path)
    first = scheduled / str(JAN1) / "manifest.json"
    second = scheduled / str(JAN2) / "manifest.json"
    missing = tmp_path / "missing" / "manifest.json"
    with pytest.raises(FileNotFoundError):
        scheduled_run(missing, JAN1)
    starts = conn.execute("select source,expected_start from meta.source_expectations order by source").fetchall()
    assert len(starts) == 3 and all(row["expected_start"] == JAN1 for row in starts)
    assert conn.execute("select status,error_category from meta.runs").fetchone() == {"status": "failed", "error_category": "FileNotFoundError"}
    # Even direct invocation of tomorrow's task cannot bypass the persisted day.
    with pytest.raises(ValueError, match="Incomplete source"):
        scheduled_run(second, JAN2)
    assert conn.execute("select count(*) n from meta.publications").fetchone()["n"] == 0
    assert all(row["expected_start"] == JAN1 for row in conn.execute("select expected_start from meta.source_expectations"))
    recovered = scheduled_run(first, JAN1)
    assert recovered["full_refresh"]
    assert conn.execute("select as_of from meta.publications order by publication_id desc limit 1").fetchone()["as_of"] == JAN2
    rows = conn.execute("select sale_date,count(*) n from analytics.fct_order_line group by sale_date order by sale_date").fetchall()
    assert [row["sale_date"] for row in rows] == [JAN1, JAN2]
    assert all(row["n"] > 0 for row in rows)
    # Already accepted tomorrow's data publishes only once. Re-running both
    # logical tasks preserves facts while keeping the reporting clock at Jan2.
    expected = conn.execute("select sales_totals,row_counts from meta.publications order by publication_id desc limit 1").fetchone()
    scheduled_run(second, JAN2)
    scheduled_run(first, JAN1)
    actual = conn.execute("select sales_totals,row_counts from meta.publications order by publication_id desc limit 1").fetchone()
    assert actual == expected
    assert conn.execute("select as_of from meta.publications order by publication_id desc limit 1").fetchone()["as_of"] == JAN2


def test_declared_start_precedes_first_actual_manifest_even_without_a_missing_file(database, tmp_path):
    scheduled = stage(tmp_path)
    with pytest.raises(ValueError, match="Incomplete source"):
        run(scheduled / str(JAN2) / "manifest.json", expected_start=JAN1)
    assert database.execute("select count(*) n from meta.publications").fetchone()["n"] == 0
