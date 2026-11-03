"""Additive producer metadata cannot override source-defined business dates."""
from __future__ import annotations

import json
from datetime import date

import pytest
from psycopg import sql

from commerce.generate import write_dataset
from commerce.pipeline import run
from test_publication_reads import AS_OF, ROOT, reference
import test_publication_reads as reader_tests

database = reader_tests.database

pytestmark = pytest.mark.integration


def marts(conn):
    return {name: conn.execute(sql.SQL("select to_jsonb(t) row from analytics.{} t order by to_jsonb(t)::text").format(
        sql.Identifier(name))).fetchall() for name in (
            "mart_daily_location_sales", "mart_refund_rate", "mart_fulfillment_sla",
            "mart_inventory_variance", "mart_refunds_by_event_date")}


def test_full_rebuild_dates_ignore_additive_inventory_sale_date(database, tmp_path):
    records = reference()
    for row in records["inventory"]:
        row["sale_date"] = "producer metadata"
    manifest = write_dataset(tmp_path / "metadata", records)
    first = run(manifest, as_of=AS_OF, full_refresh=True)
    expected = json.loads((ROOT / "fixtures" / "reference" / "expected.json").read_text())
    assert database.execute("""select sale_date::text,location_id,gross_sales_cents,
      discount_cents,refund_cents,net_sales_cents from analytics.mart_daily_location_sales
      order by sale_date,location_id""").fetchall() == expected["sales"]
    assert database.execute("select count(*) n from analytics.fct_inventory_snapshot").fetchone()["n"] == 6
    assert database.execute("""select count(*) n from landing.records where source='inventory'
      and payload->>'sale_date'='producer metadata'""").fetchone()["n"] == 6
    before = marts(database)
    catalog = write_dataset(tmp_path / "catalog", {
        "products": [dict(records["products"][-1], version=2, category="Changed description")]})
    second = run(catalog, as_of=AS_OF)
    assert first["full_refresh"] and second["full_refresh"]
    assert marts(database) == before
    for result in (first, second):
        dates = database.execute("select business_date from meta.reprocess_dates where run_id=%s order by business_date", (result["run_id"],)).fetchall()
        assert dates == [{"business_date": date(2024, 1, 1)}, {"business_date": date(2024, 1, 4)}]
