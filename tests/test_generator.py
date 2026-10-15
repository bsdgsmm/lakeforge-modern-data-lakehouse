import csv
import json
from datetime import date

import pytest

from lakeforge.ingestion import generator
from lakeforge.schemas import (
    CUSTOMER_FIELDS,
    ORDER_FIELDS,
    ORDER_ITEM_FIELDS,
    PRODUCT_FIELDS,
    TRANSACTION_FIELDS,
)


def _read_bytes(root):
    return {
        path.relative_to(root).as_posix(): path.read_bytes()
        for path in sorted(root.rglob("*"))
        if path.is_file()
    }


def test_same_seed_produces_identical_files(tmp_path):
    generator.generate(tmp_path / "a", days=3, seed=7)
    generator.generate(tmp_path / "b", days=3, seed=7)

    assert _read_bytes(tmp_path / "a") == _read_bytes(tmp_path / "b")


def test_different_seed_changes_data():
    assert generator.build_batch(1, 0).orders != generator.build_batch(2, 0).orders


def test_single_day_regeneration_matches_full_run(tmp_path):
    generator.generate(tmp_path / "full", days=3)
    generator.generate(tmp_path / "single", days=1, first_day_index=2)

    third_day = "batch_date=2026-01-07"
    full = {k: v for k, v in _read_bytes(tmp_path / "full").items() if third_day in k}
    assert full == _read_bytes(tmp_path / "single")


def test_files_follow_landing_layout_and_headers(tmp_path):
    paths = generator.write_batch(generator.build_batch(42, 0), tmp_path)

    expected_headers = {
        "customers": CUSTOMER_FIELDS,
        "products": PRODUCT_FIELDS,
        "transactions": TRANSACTION_FIELDS,
    }
    for entity, fields in expected_headers.items():
        assert paths[entity].parent.name == "batch_date=2026-01-05"
        with paths[entity].open(encoding="utf-8") as handle:
            assert tuple(next(csv.reader(handle))) == fields

    parsed, malformed = [], 0
    for line in paths["orders"].read_text(encoding="utf-8").splitlines():
        try:
            parsed.append(json.loads(line))
        except json.JSONDecodeError:
            malformed += 1
    assert malformed == 1
    for order in parsed:
        assert set(order) == {*ORDER_FIELDS, "items"}
        for item in order["items"]:
            assert set(item) == set(ORDER_ITEM_FIELDS)


def test_batches_contain_the_documented_defects():
    batch = generator.build_batch(42, 1)
    order_ids = [order["order_id"] for order in batch.orders]
    customers = [order["customer_id"] for order in batch.orders]

    assert len(order_ids) - len(set(order_ids)) >= 2
    assert "C999999" in customers
    assert None in customers
    assert any(item["quantity"] == "0" for order in batch.orders for item in order["items"])
    assert any(row["amount"] == "N/A" for row in batch.transactions)
    assert any(row["email"] is None for row in batch.customers)


def test_customer_ids_continue_across_days():
    day_two = generator.build_batch(42, 1)
    new_ids = sorted(
        row["customer_id"]
        for row in day_two.customers
        if row["customer_id"] and row["signup_date"] == "2026-01-06"
    )
    assert new_ids[0] == "C000121"
    assert new_ids[-1] == generator.customer_id(generator.customer_count(1))


def test_late_orders_belong_to_the_previous_day():
    batch = generator.build_batch(42, 3)
    order_days = {order["order_ts"][:10] for order in batch.orders if order["order_ts"]}
    assert "2026-01-07" in order_days
    assert order_days <= {"2026-01-06", "2026-01-07", "2026-01-08", "2026-13-45"}


def test_transactions_match_order_totals():
    batch = generator.build_batch(42, 0)
    totals = {order["order_id"]: generator.order_total(order) for order in batch.orders}
    captured = [row for row in batch.transactions if row["status"] == "captured"]
    matched = [row for row in captured if row["order_id"] in totals and row["amount"] != "N/A"]

    assert matched
    assert all(str(totals[row["order_id"]]) == row["amount"] for row in matched)


def test_generate_rejects_invalid_arguments(tmp_path):
    with pytest.raises(ValueError):
        generator.generate(tmp_path, days=0)
    with pytest.raises(ValueError):
        generator.generate(tmp_path, days=1, first_day_index=-1)


def test_start_date_is_configurable():
    assert generator.build_batch(42, 2, date(2025, 3, 1)).batch_date == date(2025, 3, 3)
