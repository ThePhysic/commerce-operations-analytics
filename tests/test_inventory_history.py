"""Inventory observes the closing day's catalog, including intraday changes."""
from __future__ import annotations

import importlib.util
import os
from datetime import date
from pathlib import Path

import psycopg
import pytest
from psycopg import sql
from psycopg.conninfo import conninfo_to_dict
from psycopg.rows import dict_row

from commerce.generate import write_dataset
from commerce.pipeline import run

pytestmark = pytest.mark.integration
ROOT = Path(__file__).resolve().parents[1]
JAN4, JAN5 = date(2024, 1, 4), date(2024, 1, 5)


@pytest.fixture
def database(monkeypatch, tmp_path):
    url = os.getenv("COMMERCE_INVENTORY_TEST_DATABASE_URL") or os.getenv("COMMERCE_TEST_DATABASE_URL")
    if not url:
        pytest.skip("A disposable COMMERCE_TEST_DATABASE_URL is not configured")
    dbname = conninfo_to_dict(url).get("dbname", "")
    if not dbname.endswith("_test"):
        pytest.fail("Refusing to reset a database whose name does not end in _test")
    monkeypatch.setenv("COMMERCE_DATABASE_URL", url)
    monkeypatch.setenv("COMMERCE_DATA_DIR", str(tmp_path / "runtime"))
    with psycopg.connect(url, autocommit=True, row_factory=dict_row) as conn:
        for row in conn.execute("""select nspname from pg_namespace
          where nspname in ('landing','meta','analytics') or nspname like 'build\\_%' escape '\\'""").fetchall():
            conn.execute(sql.SQL("drop schema {} cascade").format(sql.Identifier(row["nspname"])))
        yield conn


def inventory(conn):
    return conn.execute("""select f.snapshot_date, f.location_id, f.product_id,
      f.actual_quantity, f.expected_quantity, f.variance_quantity, p.category, l.region
      from analytics.fct_inventory_snapshot f
      join analytics.dim_product p using(product_key)
      join analytics.dim_location l using(location_key)
      order by f.snapshot_date, f.location_id, f.product_id""").fetchall()


def test_end_of_day_history_intraday_late_changes_and_rebuild_parity(database, tmp_path):
    spec = importlib.util.spec_from_file_location("inventory_reference", ROOT / "fixtures" / "build_reference.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    records = module.reference_records()
    jan4_snapshot = dict(records["inventory"][3])
    records["inventory"].append(dict(jan4_snapshot, snapshot_id="JAN5_P001_L001", snapshot_date="2024-01-05"))
    for source, attribute in (("products", "category"), ("locations", "region")):
        template = records[source][-1]
        records[source].extend([
            dict(template, effective_at="2024-01-04T12:00:00+00:00", **{attribute: "Noon"}),
            dict(template, effective_at="2024-01-05T00:00:00+00:00", **{attribute: "Tomorrow"}),
        ])
    initial = write_dataset(tmp_path / "initial", records)
    assert run(initial, as_of=JAN5)["full_refresh"]

    def attributes(day):
        row = next(r for r in inventory(database) if r["snapshot_date"] == day
                   and r["location_id"] == "L001" and r["product_id"] == "P001")
        return row["category"], row["region"]

    assert attributes(JAN4) == ("Noon", "Noon")
    assert attributes(JAN5) == ("Tomorrow", "Tomorrow")
    late_records = {
        source: [dict(records[source][-1], effective_at="2024-01-04T23:59:59.999999+00:00",
                      **{attribute: "Closing"})]
        for source, attribute in (("products", "category"), ("locations", "region"))
    }
    late = write_dataset(tmp_path / "late_catalog", late_records)
    assert run(late, as_of=JAN5)["full_refresh"]
    assert attributes(JAN4) == ("Closing", "Closing")
    assert attributes(JAN5) == ("Tomorrow", "Tomorrow")

    # An ordinary inventory correction must use the same closing catalog as
    # the catalog-triggered rebuild, including when selecting just January 4.
    correction = write_dataset(tmp_path / "inventory_correction", {
        "inventory": [dict(jan4_snapshot, version=2, actual_quantity=4)]})
    assert not run(correction, as_of=JAN5)["full_refresh"]
    incremental = inventory(database)
    assert attributes(JAN4) == ("Closing", "Closing")
    assert not run(correction, as_of=JAN5, start=JAN4, end=JAN4)["full_refresh"]
    assert inventory(database) == incremental
    assert run(correction, as_of=JAN5, full_refresh=True)["full_refresh"]
    assert inventory(database) == incremental
