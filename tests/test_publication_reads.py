"""Readers pin the publication visible to their own PostgreSQL snapshot."""
from __future__ import annotations

import importlib.util
import os
from datetime import date
from pathlib import Path

import psycopg
import pytest
from psycopg import sql
from psycopg.conninfo import conninfo_to_dict, make_conninfo
from psycopg.rows import dict_row

from commerce.cli import query
from commerce.generate import write_dataset
from commerce.pipeline import run
from commerce.reader import pinned_publication

pytestmark = pytest.mark.integration
ROOT = Path(__file__).resolve().parents[1]
AS_OF = date(2024, 1, 5)


def reference():
    spec = importlib.util.spec_from_file_location("reader_reference", ROOT / "fixtures" / "build_reference.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module.reference_records()


@pytest.fixture
def database(monkeypatch, tmp_path):
    explicit = os.getenv("COMMERCE_READER_TEST_DATABASE_URL")
    base = explicit or os.getenv("COMMERCE_TEST_DATABASE_URL")
    if not base:
        pytest.skip("No disposable PostgreSQL test credentials configured")
    config = conninfo_to_dict(base)
    if explicit and config.get("dbname") != "commerce_reader_test":
        pytest.fail("Reader tests require commerce_reader_test")
    if not config.get("dbname", "").endswith("_test"):
        pytest.fail("Refusing to derive reader database from non-test database")
    with psycopg.connect(base, autocommit=True) as admin:
        if not admin.execute("select 1 from pg_database where datname='commerce_reader_test'").fetchone():
            admin.execute("create database commerce_reader_test")
    config["dbname"] = "commerce_reader_test"
    url = make_conninfo(**config)
    monkeypatch.setenv("COMMERCE_DATABASE_URL", url)
    monkeypatch.setenv("COMMERCE_DATA_DIR", str(tmp_path / "runtime"))
    with psycopg.connect(url, autocommit=True, row_factory=dict_row) as conn:
        for row in conn.execute("""select nspname from pg_namespace where
          nspname in ('landing','meta','analytics') or nspname like 'build\\_%' escape '\\'""").fetchall():
            conn.execute(sql.SQL("drop schema {} cascade").format(sql.Identifier(row["nspname"])))
        yield conn


def history(reader):
    with reader.connection.cursor(row_factory=dict_row) as cursor:
        return cursor.execute(sql.SQL("""select f.line_id, f.unit_price_cents, p.category, l.region
          from {} f join {} p using(product_key) join {} l using(location_key)
          order by f.line_id""").format(reader.relation("fct_order_line"),
                                        reader.relation("dim_product"), reader.relation("dim_location"))).fetchall()


def test_old_snapshot_pins_visible_publication_across_actual_publish(database, tmp_path):
    first = run(ROOT / "fixtures" / "reference" / "manifest.json", as_of=AS_OF)
    with pinned_publication() as baseline:
        old_sales, old_history = baseline.query("sales"), history(baseline)
    assert len(old_history) == 6
    assert sum(r["net_sales"] for r in old_sales) == 69
    records = reference()
    correction = write_dataset(tmp_path / "correction", {
        "order_lines": [dict(records["order_lines"][4], version=2, unit_price_cents=1200)],
        "products": [dict(records["products"][-1], version=2, category="Revised")],
        "locations": [dict(records["locations"][-1], version=2, region="Revised")],
    })
    # The public reader also accepts ordinary psycopg tuple-row connections.
    with psycopg.connect(os.environ["COMMERCE_DATABASE_URL"], autocommit=True) as existing:
        with existing.transaction():
            existing.execute("set transaction isolation level repeatable read read only")
            # Establish the reader snapshot BEFORE the other connection commits
            # replacement views and the next publication ledger row.
            assert existing.execute("select max(publication_id) id from meta.publications").fetchone()[0] == first["publication_id"]
            second = run(correction, as_of=AS_OF)
            with pinned_publication(connection=existing) as old:
                assert old.publication_id == first["publication_id"]
                assert old.schema == first["schema"]
                assert old.query("sales") == old_sales
                assert history(old) == old_history
                assert old.publication["row_counts"]["fct_order_line"] == 6
            with pytest.raises(ValueError, match="visible"):
                with pinned_publication(connection=existing, publication_id=second["publication_id"]):
                    pytest.fail("A future publication must not appear in the old snapshot")
            assert not existing.closed
            with pinned_publication() as fresh:
                assert fresh.publication_id == second["publication_id"]
                assert sum(r["net_sales"] for r in fresh.query("sales")) == 71
                revised = next(r for r in history(fresh) if r["line_id"] == "O004_1")
                assert (revised["category"], revised["region"], revised["unit_price_cents"]) == ("Revised", "Revised", 1200)
        assert not existing.closed
    with pinned_publication(publication_id=first["publication_id"]) as retained:
        assert len(history(retained)) == 6
        assert history(retained) == old_history
        for invalid in ("missing_model", 'fct_order_line; drop schema analytics cascade; --'):
            with pytest.raises(ValueError, match="Model"):
                retained.relation(invalid)
        for invalid in ("../sales", "sales; select 1", "missing_query"):
            with pytest.raises(ValueError, match="Unknown query"):
                retained.query(invalid)
    with pytest.raises(ValueError, match="visible"):
        with pinned_publication(publication_id=second["publication_id"] + 1000):
            pytest.fail("Missing publications must fail explicitly")
    assert query("sales", publication_id=first["publication_id"]) == old_sales
    assert sum(r["net_sales"] for r in query("sales")) == 71
