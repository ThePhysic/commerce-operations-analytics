from __future__ import annotations

import csv
import hashlib
import json
from pathlib import Path

import pytest

from commerce.contracts import ContractError, load_contract, validate_record
from commerce.generate import generate, write_dataset


def sample_line():
    return dict(line_id="L1", order_id="O1", location_id="S1", product_id="P1", sale_date="2024-01-01", quantity=2, unit_price_cents=1000, discount_cents=100, status="fulfilled", paid_at="2024-01-01T10:00:00Z", fulfilled_at="2024-01-01T11:00:00Z", version=1)


def test_csv_normalization_and_additive_field():
    row = {key: str(value) for key, value in sample_line().items()}
    row["campaign"] = "synthetic-launch"
    normalized, key = validate_record("order_lines", row)
    assert key == "L1"
    assert normalized["quantity"] == 2
    assert normalized["paid_at"] == "2024-01-01T10:00:00+00:00"
    assert normalized["campaign"] == "synthetic-launch"


@pytest.mark.parametrize("bad", [True, False, 1.0, "1.0", "1e2", "+2", "02", -1, None])
def test_strict_positive_integer_quantities(bad):
    row = sample_line()
    row["quantity"] = bad
    with pytest.raises(ContractError, match="quantity"):
        validate_record("order_lines", row)


@pytest.mark.parametrize("field,value", [("paid_at", "2024-01-01T10:00:00"), ("paid_at", "2024-01-01T10:00:00-08:00"), ("sale_date", "20240101"), ("fulfilled_at", "2024-01-01T09:00:00Z"), ("discount_cents", 2100), ("status", "unknown")])
def test_invalid_semantics(field, value):
    row = sample_line()
    row[field] = value
    with pytest.raises(ContractError):
        validate_record("order_lines", row)


def test_missing_nullable_field_is_still_contract_failure():
    row = sample_line()
    del row["fulfilled_at"]
    with pytest.raises(ContractError, match="required field missing"):
        validate_record("order_lines", row)


def test_null_lifecycle_and_versions():
    row = sample_line()
    row.update(status="cancelled", paid_at="", fulfilled_at="")
    normalized, _ = validate_record("order_lines", row)
    assert normalized["paid_at"] is None
    assert normalized["fulfilled_at"] is None
    with pytest.raises(ContractError, match="schema version"):
        validate_record("order_lines", row, schema_version=2)
    with pytest.raises(ContractError, match="unknown source"):
        load_contract("../order_lines")


def test_history_business_keys_normalize_timestamp():
    record = dict(product_id="P1", name="Synthetic", category="Home", effective_at="2024-01-01T00:00:00Z", version=1)
    payload, key = validate_record("products", record)
    record["effective_at"] = "2024-01-01T00:00:00+00:00"
    assert validate_record("products", record) == (payload, key)


def dataset_bytes(root):
    return {path.relative_to(root).as_posix(): path.read_bytes() for path in root.rglob("*") if path.is_file()}


def test_generator_is_byte_reproducible_with_verified_manifests(tmp_path):
    left, right = tmp_path / "left", tmp_path / "right"
    generate(left, days=4, orders_per_day=12, seed=7)
    manifest = generate(right, days=4, orders_per_day=12, seed=7)
    assert dataset_bytes(left) == dataset_bytes(right)
    spec = json.loads(manifest.read_text())
    assert spec["synthetic"] is True
    assert {entry["source"] for entry in spec["files"]} == {"order_lines", "inventory", "refunds", "products", "locations"}
    formats = set()
    for entry in spec["files"]:
        path = right / entry["path"]
        formats.add(path.suffix)
        assert hashlib.sha256(path.read_bytes()).hexdigest() == entry["sha256"]
        if path.suffix == ".csv":
            with path.open(encoding="utf-8", newline="") as stream:
                rows = list(csv.DictReader(stream))
        else:
            rows = [json.loads(line) for line in path.read_text().splitlines()]
        assert len(rows) == entry["row_count"]
        for row in rows:
            validate_record(entry["source"], row, schema_version=entry["schema_version"])
    assert formats == {".csv", ".jsonl"}


@pytest.mark.parametrize("kwargs", [{"days": 0}, {"orders_per_day": -1}, {"days": True}, {"seed": 3.2}])
def test_generator_rejects_invalid_size(tmp_path, kwargs):
    with pytest.raises(ValueError):
        generate(tmp_path, **kwargs)


def test_fault_fixture_writer_can_preserve_bad_payload(tmp_path):
    row = sample_line()
    row["quantity"] = "broken"
    manifest = write_dataset(tmp_path, {"order_lines": [row]}, validate=False)
    entry = json.loads(manifest.read_text())["files"][0]
    with (tmp_path / entry["path"]).open(newline="", encoding="utf-8") as stream:
        assert list(csv.DictReader(stream))[0]["quantity"] == "broken"


def test_reference_fixture_integrity_and_exact_arithmetic():
    root = Path(__file__).resolve().parents[1] / "fixtures" / "reference"
    manifest = json.loads((root / "manifest.json").read_text())
    for entry in manifest["files"]:
        assert hashlib.sha256((root / entry["path"]).read_bytes()).hexdigest() == entry["sha256"]
    expected = json.loads((root / "expected.json").read_text())
    totals = expected["totals_cents"]
    assert totals["gross_sales"] - totals["discounts"] - totals["refunds"] == totals["net_sales"] == 6900
    assert sum(row["net_sales_cents"] for row in expected["sales"]) == 6900


def test_complete_bundles_prove_daily_empty_partitions_to_reporting_horizon(tmp_path):
    manifest = json.loads(generate(tmp_path, days=1, orders_per_day=3).read_text())
    by_source = {name: {entry["partition"] for entry in manifest["files"] if entry["source"] == name} for name in ("order_lines", "inventory", "refunds")}
    assert by_source["order_lines"] == by_source["inventory"] == by_source["refunds"] == {"2024-01-01", "2024-01-02", "2024-01-03"}
    assert any(entry["source"] == "order_lines" and entry["partition"] == "2024-01-03" and entry["row_count"] == 0 for entry in manifest["files"])


def test_partial_delta_bundle_does_not_invent_other_source_deliveries(tmp_path):
    refund = dict(refund_id="R1", line_id="L1", refund_date="2024-01-05", amount_cents=100, version=1)
    spec = json.loads(write_dataset(tmp_path, {"refunds": [refund]}).read_text())
    assert len(spec["files"]) == 1
    assert spec["files"][0]["partition"] == "2024-01-05"


def test_short_generator_never_emits_future_catalog_rows(tmp_path):
    spec = json.loads(generate(tmp_path, days=1, orders_per_day=1).read_text())
    assert {entry["partition"] for entry in spec["files"] if entry["source"] in {"products", "locations"}} == {"2023-01-01"}
