"""Deterministic synthetic commerce feeds in CSV and JSONL source formats."""

from __future__ import annotations

import csv
import hashlib
import json
import random
from collections import defaultdict
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable

from commerce.contracts import load_contract, validate_record

START_DATE = date(2024, 1, 1)


def write_dataset(
    output: Path,
    records: dict[str, Iterable[dict[str, Any]]],
    *,
    seed: int = 42,
    validate: bool = True,
) -> Path:
    """Write a manifest and deterministic files, useful for fault-injection fixtures.

    By default rows are validated. Set validate=False only to intentionally create
    bad input for a quarantine demonstration. Paths are relative to the manifest.
    """
    output = Path(output)
    output.mkdir(parents=True, exist_ok=True)
    records = {source: list(rows) for source, rows in records.items()}
    daily_fields = {"order_lines": "sale_date", "refunds": "refund_date", "inventory": "snapshot_date"}
    reporting_days: list[date] = []
    # A complete bundle explicitly proves empty partitions at every daily feed.
    # Delta bundles carry only the sources/dates the producer actually supplies.
    if set(daily_fields).issubset(records):
        dates = [date.fromisoformat(row[field]) for source, field in daily_fields.items() for row in records[source]]
        beginning, ending = (min(dates), max(dates)) if dates else (START_DATE, START_DATE)
        reporting_days = [beginning + timedelta(days=n) for n in range((ending - beginning).days + 1)]
    manifest: dict[str, Any] = {"synthetic": True, "seed": seed, "files": []}
    for source in sorted(records):
        contract = load_contract(source)
        partitions: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for row in records[source]:
            row = dict(row)
            if validate:
                row, _ = validate_record(source, row)
            field = contract["partition_field"]
            partition = str(row.get(field, START_DATE.isoformat()))[:10]
            partitions[partition].append(row)
        if source in daily_fields:
            for business_day in reporting_days:
                partitions.setdefault(business_day.isoformat(), [])
        # Empty source partitions are explicit completeness evidence, not absence.
        if not partitions:
            partitions[START_DATE.isoformat()] = []
        for partition, rows in sorted(partitions.items()):
            extension = "csv" if source == "order_lines" else "jsonl"
            relative = Path(source) / f"{partition}.{extension}"
            destination = output / relative
            destination.parent.mkdir(parents=True, exist_ok=True)
            if extension == "csv":
                fields = list(contract["fields"])
                fields += sorted({key for row in rows for key in row} - set(fields))
                with destination.open("w", encoding="utf-8", newline="") as stream:
                    writer = csv.DictWriter(stream, fieldnames=fields, lineterminator="\n")
                    writer.writeheader()
                    writer.writerows(rows)
            else:
                destination.write_text("".join(json.dumps(row, sort_keys=True, separators=(",", ":")) + "\n" for row in rows), encoding="utf-8", newline="\n")
            manifest["files"].append({
                "source": source,
                "path": relative.as_posix(),
                "sha256": hashlib.sha256(destination.read_bytes()).hexdigest(),
                "schema_version": 1,
                "partition": partition,
                "row_count": len(rows),
            })
    manifest_path = output / "manifest.json"
    manifest_path.write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8", newline="\n")
    return manifest_path


def generate(output: Path, *, days: int = 7, orders_per_day: int = 20, seed: int = 42) -> Path:
    """Generate the same file bytes for the same parameters on every platform."""
    for name, value in (("days", days), ("orders_per_day", orders_per_day)):
        if type(value) is not int or value < 1:
            raise ValueError(f"{name} must be a positive integer")
    if type(seed) is not int:
        raise ValueError("seed must be an integer")
    rng = random.Random(seed)
    records: dict[str, list[dict[str, Any]]] = {name: [] for name in ("order_lines", "refunds", "inventory", "products", "locations")}
    products = [("P001", "Synthetic Mug", "Home", 1000), ("P002", "Synthetic Tote", "Accessories", 2500), ("P003", "Synthetic Notebook", "Stationery", 800)]
    old = "2023-01-01T00:00:00+00:00"
    change = "2024-01-04T00:00:00+00:00"
    for product_id, name, category, _ in products:
        records["products"].append(dict(product_id=product_id, name=name, category=category, effective_at=old, version=1))
    records["products"].append(dict(product_id="P001", name="Synthetic Mug", category="Gifts", effective_at=change, version=1))
    for location_id, region in (("L001", "North"), ("L002", "South")):
        records["locations"].append(dict(location_id=location_id, name=f"Synthetic Store {location_id}", region=region, effective_at=old, version=1))
    records["locations"].append(dict(location_id="L001", name="Synthetic Store L001", region="Central", effective_at=change, version=1))
    for day_index in range(days):
        business_day = START_DATE + timedelta(days=day_index)
        for index in range(orders_per_day):
            order_id = f"O{business_day:%Y%m%d}_{index:06d}"
            location_id = f"L{1 + index % 2:03d}"
            status = "cancelled" if index % 11 == 0 else ("paid" if index % 5 == 0 else "fulfilled")
            paid = datetime.combine(business_day, datetime.min.time(), timezone.utc) + timedelta(hours=8, minutes=index % 600)
            fulfillment = paid + timedelta(minutes=rng.randint(10, 240))
            for line_index in range(1 + (index % 3 == 0)):
                product_id, _, _, price = products[(index + line_index) % len(products)]
                quantity = rng.randint(1, 3)
                discount = 100 if index % 4 == 0 else 0
                line_id = f"{order_id}_{line_index + 1}"
                records["order_lines"].append(dict(
                    line_id=line_id, order_id=order_id, location_id=location_id,
                    product_id=product_id, sale_date=business_day.isoformat(),
                    quantity=quantity, unit_price_cents=price, discount_cents=discount,
                    status=status, paid_at=None if status == "cancelled" else paid.isoformat(),
                    fulfilled_at=fulfillment.isoformat() if status == "fulfilled" else None, version=1,
                ))
                if index % 7 == 2 and status != "cancelled" and line_index == 0:
                    records["refunds"].append(dict(
                        refund_id=f"R{line_id}", line_id=line_id,
                        refund_date=(business_day + timedelta(days=2)).isoformat(),
                        amount_cents=(quantity * price - discount) // 2, version=1,
                    ))
        for location_id in ("L001", "L002"):
            for product_id, _, _, _ in products:
                expected = rng.randint(5, 40)
                actual = 0 if (day_index + int(product_id[-1]) + int(location_id[-1])) % 5 == 0 else max(0, expected + rng.randint(-3, 3))
                records["inventory"].append(dict(
                    snapshot_id=f"I{business_day:%Y%m%d}_{location_id}_{product_id}",
                    location_id=location_id, product_id=product_id,
                    snapshot_date=business_day.isoformat(), expected_quantity=expected,
                    actual_quantity=actual, version=1,
                ))
    horizon = max(date.fromisoformat(row[field]) for source, field in (("order_lines", "sale_date"), ("inventory", "snapshot_date"), ("refunds", "refund_date")) for row in records[source])
    for source in ("products", "locations"):
        records[source] = [row for row in records[source] if date.fromisoformat(row["effective_at"][:10]) <= horizon]
    return write_dataset(output, records, seed=seed)
