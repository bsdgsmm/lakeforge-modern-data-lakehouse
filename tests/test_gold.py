from datetime import date, datetime
from decimal import Decimal

import pytest

from lakeforge.transformations import gold
from tests.helpers import (
    item,
    silver_customers,
    silver_orders,
    silver_products,
    silver_transactions,
)

JAN_5, JAN_6 = date(2026, 1, 5), date(2026, 1, 6)


@pytest.fixture
def facts(spark):
    orders = silver_orders(
        spark,
        [
            (
                "O1",
                "C000001",
                datetime(2026, 1, 5, 10),
                "delivered",
                [item(1, "P0001", 3, "10.00", "0.1000"), item(2, "P0002", 1, "5.00")],
            ),
            ("O2", "C000001", datetime(2026, 1, 6, 9), "placed", [item(1, "P0001", 1, "10.00")]),
            ("O3", "C000002", datetime(2026, 1, 6, 11), "cancelled", [item(1, "P0002", 2, "5.00")]),
        ],
    )
    return gold.build_fact_order_items(orders)


@pytest.fixture
def transactions(spark):
    return silver_transactions(
        spark,
        [
            ("T1", "O1", datetime(2026, 1, 5, 10), "32.00", "captured"),
            ("T2", "O2", datetime(2026, 1, 6, 9), "10.00", "failed"),
            ("T3", "O2", datetime(2026, 1, 6, 9, 5), "10.00", "captured"),
            ("T4", "O3", datetime(2026, 1, 6, 12), "10.00", "refunded"),
        ],
    )


def test_fact_line_amounts(facts):
    lines = {(row.order_id, row.line_number): row for row in facts.collect()}

    first = lines[("O1", 1)]
    assert first.gross_amount == Decimal("30.00")
    assert first.discount_amount == Decimal("3.00")
    assert first.net_amount == Decimal("27.00")
    assert first.is_revenue is True
    assert lines[("O3", 1)].is_revenue is False
    assert facts.count() == 4


def test_daily_revenue_excludes_cancelled_orders(facts, transactions):
    days = {
        row.revenue_date: row for row in gold.build_daily_revenue(facts, transactions).collect()
    }

    assert days[JAN_5].asDict() == {
        "revenue_date": JAN_5,
        "orders": 1,
        "cancelled_orders": 0,
        "customers": 1,
        "units_sold": 4,
        "gross_revenue": Decimal("35.00"),
        "discounts": Decimal("3.00"),
        "net_revenue": Decimal("32.00"),
        "average_order_value": Decimal("32.00"),
        "captured_payments": Decimal("32.00"),
        "refunded_payments": Decimal("0.00"),
        "failed_payments": 0,
    }
    assert days[JAN_6].orders == 1
    assert days[JAN_6].cancelled_orders == 1
    assert days[JAN_6].net_revenue == Decimal("10.00")
    assert days[JAN_6].refunded_payments == Decimal("10.00")
    assert days[JAN_6].failed_payments == 1


def test_product_performance_ranks_and_includes_unsold_products(spark, facts):
    products = gold.build_dim_product(silver_products(spark, ["P0001", "P0002", "P0003"]))
    rows = {
        row.product_id: row for row in gold.build_product_performance(facts, products).collect()
    }

    assert (rows["P0001"].units_sold, rows["P0001"].orders) == (4, 2)
    assert rows["P0001"].net_revenue == Decimal("37.00")
    assert rows["P0002"].net_revenue == Decimal("5.00")
    assert rows["P0003"].units_sold == 0
    assert [rows[p].revenue_rank for p in ("P0001", "P0002", "P0003")] == [1, 2, 3]
    assert rows["P0001"].category_revenue_share == pytest.approx(0.881, abs=1e-3)
    assert rows["P0001"].first_sale_date == JAN_5
    assert rows["P0001"].last_sale_date == JAN_6


def test_customer_activity_segments(spark, facts):
    customers = gold.build_dim_customer(silver_customers(spark, ["C000001", "C000002", "C000003"]))
    rows = {
        row.customer_id: row for row in gold.build_customer_activity(facts, customers).collect()
    }

    active = rows["C000001"]
    assert active.orders == 2
    assert active.lifetime_net_revenue == Decimal("42.00")
    assert active.average_order_value == Decimal("21.00")
    assert (active.first_order_date, active.last_order_date) == (JAN_5, JAN_6)
    assert active.active_days == 2
    assert active.days_since_last_order == 0
    assert active.segment == "repeat"

    assert rows["C000002"].segment == "no_orders"
    assert rows["C000002"].cancelled_orders == 1
    assert rows["C000003"].orders == 0
    assert rows["C000003"].as_of_date == JAN_6


def test_dim_customer_derives_full_name_and_email_flag(spark):
    dim = gold.build_dim_customer(silver_customers(spark, ["C000001"])).collect()[0]

    assert dim.full_name == "Ada Silva"
    assert dim.has_email is False
