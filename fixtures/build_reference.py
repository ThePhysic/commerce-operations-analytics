"""Regenerate the tiny, hand-calculated synthetic reference dataset.

From the repository root: python fixtures/build_reference.py
Expected answers are declared here independently of SQL and generation logic.
"""

from pathlib import Path
import json

from commerce.generate import write_dataset


def reference_records():
    records = {name: [] for name in ("order_lines", "refunds", "inventory", "products", "locations")}
    for product_id, name, category in (("P001", "Synthetic Mug", "Home"), ("P002", "Synthetic Tote", "Accessories")):
        records["products"].append(dict(product_id=product_id, name=name, category=category, effective_at="2023-01-01T00:00:00+00:00", version=1))
    records["products"].append(dict(product_id="P001", name="Synthetic Mug", category="Gifts", effective_at="2024-01-04T00:00:00+00:00", version=1))
    for location_id, region in (("L001", "North"), ("L002", "South")):
        records["locations"].append(dict(location_id=location_id, name=f"Synthetic Store {location_id}", region=region, effective_at="2023-01-01T00:00:00+00:00", version=1))
    records["locations"].append(dict(location_id="L001", name="Synthetic Store L001", region="Central", effective_at="2024-01-04T00:00:00+00:00", version=1))
    lines = [
        ("O001_1", "O001", "L001", "P001", "2024-01-01", 2, 1000, 100, "fulfilled", "10:00", "11:00"),
        ("O001_2", "O001", "L001", "P002", "2024-01-01", 1, 2500, 500, "fulfilled", "10:00", "11:00"),
        ("O002_1", "O002", "L001", "P001", "2024-01-01", 1, 1000, 0, "fulfilled", "10:30", "12:30"),
        ("O003_1", "O003", "L002", "P002", "2024-01-01", 1, 2500, 0, "cancelled", None, None),
        ("O004_1", "O004", "L001", "P001", "2024-01-04", 1, 1000, 0, "fulfilled", "11:00", "11:30"),
        ("O005_1", "O005", "L002", "P002", "2024-01-04", 1, 2500, 0, "paid", "12:00", None),
    ]
    for line_id, order_id, location_id, product_id, sale_date, quantity, price, discount, status, paid, fulfilled in lines:
        records["order_lines"].append(dict(
            line_id=line_id, order_id=order_id, location_id=location_id,
            product_id=product_id, sale_date=sale_date, quantity=quantity,
            unit_price_cents=price, discount_cents=discount, status=status,
            paid_at=f"{sale_date}T{paid}:00+00:00" if paid else None,
            fulfilled_at=f"{sale_date}T{fulfilled}:00+00:00" if fulfilled else None, version=1,
        ))
    for refund_id, line_id, refund_date, amount in (("R001", "O001_1", "2024-01-03", 400), ("R002", "O001_1", "2024-01-05", 100), ("R003", "O002_1", "2024-01-05", 1000)):
        records["refunds"].append(dict(refund_id=refund_id, line_id=line_id, refund_date=refund_date, amount_cents=amount, version=1))
    inventory = [
        ("2024-01-01", "L001", "P001", 10, 0), ("2024-01-01", "L001", "P002", 5, 7),
        ("2024-01-01", "L002", "P001", 8, 8), ("2024-01-04", "L001", "P001", 6, 5),
        ("2024-01-04", "L001", "P002", 7, 7), ("2024-01-04", "L002", "P002", 10, 0),
    ]
    for snapshot_date, location_id, product_id, expected, actual in inventory:
        records["inventory"].append(dict(snapshot_id=f"I{snapshot_date}_{location_id}_{product_id}", location_id=location_id, product_id=product_id, snapshot_date=snapshot_date, expected_quantity=expected, actual_quantity=actual, version=1))
    return records


EXPECTED = {
    "synthetic": True,
    "as_of": "2024-01-05",
    "fact_counts": {"fct_order_line": 6, "fct_refund": 3, "fct_fulfillment": 5, "fct_inventory_snapshot": 6},
    "sales": [
        {"sale_date": "2024-01-01", "location_id": "L001", "gross_sales_cents": 5500, "discount_cents": 600, "refund_cents": 1500, "net_sales_cents": 3400},
        {"sale_date": "2024-01-01", "location_id": "L002", "gross_sales_cents": 0, "discount_cents": 0, "refund_cents": 0, "net_sales_cents": 0},
        {"sale_date": "2024-01-04", "location_id": "L001", "gross_sales_cents": 1000, "discount_cents": 0, "refund_cents": 0, "net_sales_cents": 1000},
        {"sale_date": "2024-01-04", "location_id": "L002", "gross_sales_cents": 2500, "discount_cents": 0, "refund_cents": 0, "net_sales_cents": 2500},
    ],
    "totals_cents": {"gross_sales": 9000, "discounts": 600, "refunds": 1500, "net_sales": 6900},
    "fulfillment": [
        {"sale_date": "2024-01-01", "location_id": "L001", "fulfilled_orders": 2, "median_minutes": 90, "p95_minutes": 117},
        {"sale_date": "2024-01-04", "location_id": "L001", "fulfilled_orders": 1, "median_minutes": 30, "p95_minutes": 30},
    ],
    "inventory": [
        {"snapshot_date": "2024-01-01", "location_id": "L001", "snapshots": 2, "stockouts": 1, "variance_quantity": -8},
        {"snapshot_date": "2024-01-01", "location_id": "L002", "snapshots": 1, "stockouts": 0, "variance_quantity": 0},
        {"snapshot_date": "2024-01-04", "location_id": "L001", "snapshots": 2, "stockouts": 0, "variance_quantity": -1},
        {"snapshot_date": "2024-01-04", "location_id": "L002", "snapshots": 1, "stockouts": 1, "variance_quantity": -10},
    ],
    "refund_events": [{"refund_date": "2024-01-03", "refund_cents": 400}, {"refund_date": "2024-01-05", "refund_cents": 1100}],
    "refund_rates": [{"sale_date": "2024-01-01", "location_id": "L001", "paid_orders": 2, "refunded_orders": 2, "refund_rate": 1.0}],
    "history": {"P001_category_2024_01_01": "Home", "P001_category_2024_01_04": "Gifts", "L001_region_2024_01_01": "North", "L001_region_2024_01_04": "Central"},
}


if __name__ == "__main__":
    target = Path(__file__).parent / "reference"
    write_dataset(target, reference_records(), seed=42)
    (target / "expected.json").write_text(json.dumps(EXPECTED, indent=2, sort_keys=True) + "\n", encoding="utf-8", newline="\n")
