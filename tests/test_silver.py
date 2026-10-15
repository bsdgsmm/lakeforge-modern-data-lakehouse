from datetime import date, datetime
from decimal import Decimal

from pyspark.sql import functions as F

from lakeforge.quality.rules import REASONS_COLUMN, apply_rules, split_valid
from lakeforge.transformations import silver
from tests.helpers import bronze_rows, item, silver_customers, silver_orders, silver_products

TS = "2026-01-05T10:00:00Z"


def _reasons(frame, key):
    return {row[key]: sorted(row[REASONS_COLUMN]) for row in frame.collect()}


def _order(order_id, customer_id="C000001", items=None, **overrides):
    if items is None:
        items = [
            {"product_id": "P0001", "quantity": "2", "unit_price": "10.00", "discount_pct": "0.10"}
        ]
    return {
        "order_id": order_id,
        "customer_id": customer_id,
        "order_ts": TS,
        "status": "placed",
        "updated_at": TS,
        "items": items,
        **overrides,
    }


def test_customer_standardization_cleans_values(spark):
    raw = bronze_rows(
        spark,
        "customers",
        [
            {
                "customer_id": " c000001 ",
                "first_name": " ada",
                "last_name": "SILVA",
                "email": "  Ada.Silva@Example.COM ",
                "country_code": "us",
                "signup_date": "2026-01-05",
                "updated_at": TS,
            },
            {
                "customer_id": "C000002",
                "first_name": "Liam",
                "last_name": "Novak",
                "email": "",
                "country_code": None,
                "signup_date": "2026-01-05",
                "updated_at": TS,
            },
        ],
    )
    checked = apply_rules(silver.standardize_customers(raw), silver.customer_rules())
    valid, rejected = split_valid(checked)

    assert rejected.count() == 0
    rows = {row["std_customer_id"]: row for row in valid.collect()}
    assert rows["C000001"]["std_email"] == "ada.silva@example.com"
    assert rows["C000001"]["std_first_name"] == "Ada"
    assert rows["C000001"]["std_last_name"] == "Silva"
    assert rows["C000001"]["std_country_code"] == "US"
    assert rows["C000002"]["std_email"] is None
    assert rows["C000002"]["std_country_code"] == silver.UNKNOWN_COUNTRY


def test_customer_rules_reject_missing_keys_and_bad_values(spark):
    raw = bronze_rows(
        spark,
        "customers",
        [
            {
                "customer_id": None,
                "signup_date": "2026-01-05",
                "updated_at": TS,
                "country_code": "US",
                "email": "x",
            },
            {
                "customer_id": "C000003",
                "signup_date": "yesterday",
                "updated_at": TS,
                "country_code": "USA",
                "email": "not-an-email",
            },
        ],
    )
    _, rejected = split_valid(
        apply_rules(silver.standardize_customers(raw), silver.customer_rules())
    )
    reasons = _reasons(rejected, "_record_hash")

    assert "missing_customer_id" in reasons["hash-000"]
    assert reasons["hash-001"] == ["invalid_country_code", "invalid_email", "invalid_signup_date"]


def test_product_standardization_handles_nulls_and_invalid_prices(spark):
    raw = bronze_rows(
        spark,
        "products",
        [
            {
                "product_id": "P0001",
                "product_name": "Mug",
                "category": None,
                "unit_price": "12.50",
                "is_active": "FALSE",
                "updated_at": TS,
            },
            {
                "product_id": "P0002",
                "product_name": "Lamp",
                "category": "Home",
                "unit_price": "not-a-price",
                "is_active": "true",
                "updated_at": TS,
            },
            {
                "product_id": "P0003",
                "product_name": "Pen",
                "category": "home",
                "unit_price": "-1",
                "is_active": "maybe",
                "updated_at": TS,
            },
        ],
    )
    valid, rejected = split_valid(
        apply_rules(silver.standardize_products(raw), silver.product_rules())
    )

    row = valid.collect()[0]
    assert row["std_category"] == silver.UNCATEGORIZED
    assert row["std_is_active"] is False
    assert row["std_unit_price"] == Decimal("12.50")
    assert _reasons(rejected, "std_product_id") == {
        "P0002": ["invalid_unit_price"],
        "P0003": ["invalid_is_active", "invalid_unit_price"],
    }


def test_order_rules_cover_items_orphans_and_malformed_lines(spark):
    raw = bronze_rows(
        spark,
        "orders",
        [
            _order("O202601050001"),
            _order("O202601050002", customer_id="C999999"),
            _order(
                "O202601050003",
                items=[
                    {
                        "product_id": "P0404",
                        "quantity": "1",
                        "unit_price": "5.00",
                        "discount_pct": "0",
                    }
                ],
            ),
            _order(
                "O202601050004",
                items=[
                    {
                        "product_id": "P0001",
                        "quantity": "0",
                        "unit_price": "5.00",
                        "discount_pct": "0",
                    }
                ],
            ),
            _order("O202601050005", customer_id=None, order_ts="2026-13-45T25:00:00Z"),
            _order("O202601050006", status="lost", items=[]),
            {"_corrupt_record": '{"order_id": "O20260105'},
        ],
    )
    standardized = silver.standardize_orders(
        raw, silver_customers(spark, ["C000001"]), silver_products(spark, ["P0001"])
    )
    valid, rejected = split_valid(apply_rules(standardized, silver.order_rules()))

    assert [row["std_order_id"] for row in valid.collect()] == ["O202601050001"]
    assert _reasons(rejected, "_record_hash") == {
        "hash-001": ["orphan_customer"],
        "hash-002": ["orphan_product"],
        "hash-003": ["invalid_item"],
        "hash-004": ["invalid_order_ts", "missing_customer_id"],
        "hash-005": ["empty_items", "invalid_status"],
        "hash-006": ["malformed_record"],
    }


def test_order_items_are_typed_and_numbered(spark):
    raw = bronze_rows(
        spark,
        "orders",
        [
            _order(
                "O202601050001",
                items=[
                    {
                        "product_id": " p0001",
                        "quantity": "2",
                        "unit_price": "10.00",
                        "discount_pct": "0.10",
                    },
                    {
                        "product_id": "P0002",
                        "quantity": "1",
                        "unit_price": "3.5",
                        "discount_pct": "0",
                    },
                ],
            )
        ],
    )
    standardized = silver.standardize_orders(
        raw, silver_customers(spark, ["C000001"]), silver_products(spark, ["P0001", "P0002"])
    )
    items = standardized.select(F.explode("std_items").alias("i")).select("i.*").collect()

    assert [(i.line_number, i.product_id, i.quantity) for i in items] == [
        (1, "P0001", 2),
        (2, "P0002", 1),
    ]
    assert items[0].unit_price == Decimal("10.00")
    assert items[0].discount_pct == Decimal("0.1000")


def test_transaction_rules(spark):
    orders = silver_orders(
        spark,
        [
            (
                "O202601050001",
                "C000001",
                datetime(2026, 1, 5, 10),
                "placed",
                [item(1, "P0001", 1, "10.00")],
            )
        ],
    )
    base = {
        "order_id": "O202601050001",
        "transaction_ts": TS,
        "payment_method": "Card",
        "amount": "10.00",
        "currency": "usd",
        "status": "CAPTURED",
    }
    raw = bronze_rows(
        spark,
        "transactions",
        [
            {**base, "transaction_id": "T2026010500001"},
            {**base, "transaction_id": "T2026010500002", "amount": "N/A"},
            {**base, "transaction_id": "T2026010500003", "order_id": "O000000000000"},
            {**base, "transaction_id": "T2026010500004", "currency": "EUR", "amount": "-5"},
        ],
    )
    valid, rejected = split_valid(
        apply_rules(silver.standardize_transactions(raw, orders), silver.transaction_rules())
    )

    accepted = valid.collect()
    assert [row["std_transaction_id"] for row in accepted] == ["T2026010500001"]
    assert accepted[0]["std_currency"] == "USD"
    assert accepted[0]["std_status"] == "captured"
    assert _reasons(rejected, "std_transaction_id") == {
        "T2026010500002": ["invalid_amount"],
        "T2026010500003": ["orphan_order"],
        "T2026010500004": ["invalid_amount", "unsupported_currency"],
    }


def test_latest_per_key_prefers_newest_update_then_latest_batch(spark):
    frame = spark.createDataFrame(
        [
            ("C1", datetime(2026, 1, 5, 9), date(2026, 1, 5), "a", "old"),
            ("C1", datetime(2026, 1, 6, 9), date(2026, 1, 6), "b", "new"),
            ("C2", datetime(2026, 1, 5, 9), date(2026, 1, 5), "c", "first batch"),
            ("C2", datetime(2026, 1, 5, 9), date(2026, 1, 7), "d", "replayed later"),
            ("C3", datetime(2026, 1, 5, 9), date(2026, 1, 5), "e", "dup"),
            ("C3", datetime(2026, 1, 5, 9), date(2026, 1, 5), "e", "dup"),
        ],
        "id STRING, updated_at TIMESTAMP, _batch_date DATE, _record_hash STRING, note STRING",
    )
    winners = {
        row["id"]: row["note"] for row in silver.latest_per_key(frame, "id", "updated_at").collect()
    }

    assert winners == {"C1": "new", "C2": "replayed later", "C3": "dup"}
