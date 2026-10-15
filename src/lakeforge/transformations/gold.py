"""Gold layer: analytics-ready star schema and business aggregates.

``fact_order_items`` is maintained incrementally by order-date partition. The aggregates
are recomputed from the fact table on every run: they span the full history (lifetime
customer value, product rank), so a partial update would need the same full scan, and at
this data volume a recompute is cheaper than the bookkeeping required to merge them.
"""

from __future__ import annotations

import logging
from collections.abc import Iterable
from datetime import date
from pathlib import Path

from pyspark.sql import Column, DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType

from lakeforge.config import Settings
from lakeforge.quality import checks
from lakeforge.schemas import SILVER_CUSTOMERS, SILVER_ORDERS, SILVER_PRODUCTS, SILVER_TRANSACTIONS
from lakeforge.storage.parquet import (
    overwrite_table,
    read_partitions,
    read_table,
    replace_partitions,
    table_exists,
)

logger = logging.getLogger(__name__)

AMOUNT = DecimalType(18, 2)
REVENUE_STATUSES = ("placed", "shipped", "delivered")


def _money(column: Column) -> Column:
    return F.round(column, 2).cast(AMOUNT)


def build_dim_customer(customers: DataFrame) -> DataFrame:
    return customers.select(
        "customer_id",
        "first_name",
        "last_name",
        F.concat_ws(" ", "first_name", "last_name").alias("full_name"),
        "email",
        F.col("email").isNotNull().alias("has_email"),
        "country_code",
        "signup_date",
        F.col("updated_at").alias("source_updated_at"),
    )


def build_dim_product(products: DataFrame) -> DataFrame:
    return products.select(
        "product_id",
        "product_name",
        "category",
        F.col("unit_price").alias("current_unit_price"),
        "is_active",
        F.col("updated_at").alias("source_updated_at"),
    )


def build_fact_order_items(orders: DataFrame) -> DataFrame:
    lines = orders.select(
        "order_id",
        "order_date",
        "order_ts",
        "customer_id",
        F.col("status").alias("order_status"),
        F.explode("items").alias("item"),
    )
    gross = F.col("item.unit_price") * F.col("item.quantity")
    net = _money(gross * (F.lit(1) - F.col("item.discount_pct")))
    return lines.select(
        "order_id",
        F.col("item.line_number").alias("line_number"),
        "order_ts",
        "customer_id",
        F.col("item.product_id").alias("product_id"),
        "order_status",
        F.col("order_status").isin(*REVENUE_STATUSES).alias("is_revenue"),
        F.col("item.quantity").alias("quantity"),
        F.col("item.unit_price").alias("unit_price"),
        F.col("item.discount_pct").alias("discount_pct"),
        _money(gross).alias("gross_amount"),
        _money(gross - net).alias("discount_amount"),
        net.alias("net_amount"),
        "order_date",
    )


def build_daily_revenue(facts: DataFrame, transactions: DataFrame) -> DataFrame:
    revenue = facts.filter("is_revenue")
    orders = (
        revenue.groupBy("order_date")
        .agg(
            F.countDistinct("order_id").alias("orders"),
            F.countDistinct("customer_id").alias("customers"),
            F.sum("quantity").alias("units_sold"),
            _money(F.sum("gross_amount")).alias("gross_revenue"),
            _money(F.sum("discount_amount")).alias("discounts"),
            _money(F.sum("net_amount")).alias("net_revenue"),
        )
        .withColumn("average_order_value", _money(F.col("net_revenue") / F.col("orders")))
    )
    cancelled = (
        facts.filter(F.col("order_status") == "cancelled")
        .groupBy("order_date")
        .agg(F.countDistinct("order_id").alias("cancelled_orders"))
    )
    payments = transactions.groupBy(F.col("transaction_date").alias("order_date")).agg(
        _money(F.sum(F.when(F.col("status") == "captured", F.col("amount")))).alias(
            "captured_payments"
        ),
        _money(F.sum(F.when(F.col("status") == "refunded", F.col("amount")))).alias(
            "refunded_payments"
        ),
        F.count(F.when(F.col("status") == "failed", True)).alias("failed_payments"),
    )
    zero = F.lit(0).cast(AMOUNT)
    return (
        orders.join(cancelled, "order_date", "full")
        .join(payments, "order_date", "full")
        .select(
            F.col("order_date").alias("revenue_date"),
            F.coalesce("orders", F.lit(0)).alias("orders"),
            F.coalesce("cancelled_orders", F.lit(0)).alias("cancelled_orders"),
            F.coalesce("customers", F.lit(0)).alias("customers"),
            F.coalesce("units_sold", F.lit(0)).alias("units_sold"),
            F.coalesce("gross_revenue", zero).alias("gross_revenue"),
            F.coalesce("discounts", zero).alias("discounts"),
            F.coalesce("net_revenue", zero).alias("net_revenue"),
            F.coalesce("average_order_value", zero).alias("average_order_value"),
            F.coalesce("captured_payments", zero).alias("captured_payments"),
            F.coalesce("refunded_payments", zero).alias("refunded_payments"),
            F.coalesce("failed_payments", F.lit(0)).alias("failed_payments"),
        )
        .orderBy("revenue_date")
    )


def build_product_performance(facts: DataFrame, products: DataFrame) -> DataFrame:
    sales = (
        facts.filter("is_revenue")
        .groupBy("product_id")
        .agg(
            F.sum("quantity").alias("units_sold"),
            F.countDistinct("order_id").alias("orders"),
            F.countDistinct("customer_id").alias("customers"),
            _money(F.sum("gross_amount")).alias("gross_revenue"),
            _money(F.sum("net_amount")).alias("net_revenue"),
            F.min("order_date").alias("first_sale_date"),
            F.max("order_date").alias("last_sale_date"),
        )
    )
    zero = F.lit(0).cast(AMOUNT)
    combined = products.join(sales, "product_id", "left").select(
        "product_id",
        "product_name",
        "category",
        "current_unit_price",
        "is_active",
        F.coalesce("units_sold", F.lit(0)).alias("units_sold"),
        F.coalesce("orders", F.lit(0)).alias("orders"),
        F.coalesce("customers", F.lit(0)).alias("customers"),
        F.coalesce("gross_revenue", zero).alias("gross_revenue"),
        F.coalesce("net_revenue", zero).alias("net_revenue"),
        "first_sale_date",
        "last_sale_date",
    )
    category_total = F.sum("net_revenue").over(Window.partitionBy("category"))
    return combined.select(
        "*",
        F.dense_rank()
        .over(Window.orderBy(F.col("net_revenue").desc(), F.col("product_id")))
        .alias("revenue_rank"),
        F.when(category_total > 0, F.round(F.col("net_revenue") / category_total, 4))
        .otherwise(F.lit(0.0))
        .cast("double")
        .alias("category_revenue_share"),
    )


def build_customer_activity(facts: DataFrame, customers: DataFrame) -> DataFrame:
    as_of = facts.agg(F.max("order_date").alias("as_of_date"))
    revenue = facts.filter("is_revenue")
    orders = revenue.groupBy("customer_id", "order_id", "order_date").agg(
        F.sum("net_amount").alias("order_net")
    )
    activity = orders.groupBy("customer_id").agg(
        F.count("order_id").alias("orders"),
        _money(F.sum("order_net")).alias("lifetime_net_revenue"),
        F.min("order_date").alias("first_order_date"),
        F.max("order_date").alias("last_order_date"),
        F.countDistinct("order_date").alias("active_days"),
    )
    cancellations = (
        facts.filter(F.col("order_status") == "cancelled")
        .groupBy("customer_id")
        .agg(F.countDistinct("order_id").alias("cancelled_orders"))
    )
    order_count = F.coalesce("orders", F.lit(0))
    zero = F.lit(0).cast(AMOUNT)
    return (
        customers.select("customer_id", "full_name", "country_code", "signup_date")
        .join(activity, "customer_id", "left")
        .join(cancellations, "customer_id", "left")
        .crossJoin(as_of)
        .select(
            "customer_id",
            "full_name",
            "country_code",
            "signup_date",
            order_count.alias("orders"),
            F.coalesce("cancelled_orders", F.lit(0)).alias("cancelled_orders"),
            F.coalesce("lifetime_net_revenue", zero).alias("lifetime_net_revenue"),
            F.when(order_count > 0, _money(F.col("lifetime_net_revenue") / order_count))
            .otherwise(zero)
            .alias("average_order_value"),
            "first_order_date",
            "last_order_date",
            F.coalesce("active_days", F.lit(0)).alias("active_days"),
            F.date_diff("as_of_date", "last_order_date").alias("days_since_last_order"),
            F.when(order_count == 0, "no_orders")
            .when(order_count == 1, "one_time")
            .when(order_count < 5, "repeat")
            .otherwise("loyal")
            .alias("segment"),
            "as_of_date",
        )
    )


def _all_partitions(path: Path, column: str) -> set[date]:
    if not path.is_dir():
        return set()
    prefix = f"{column}="
    return {
        date.fromisoformat(child.name.removeprefix(prefix))
        for child in path.iterdir()
        if child.is_dir() and child.name.startswith(prefix)
    }


def refresh_gold(
    spark: SparkSession, settings: Settings, order_dates: Iterable[date]
) -> dict[str, int]:
    silver, gold = settings.silver_dir, settings.gold_dir
    fact_path = gold / "fact_order_items"

    affected = set(order_dates)
    if not table_exists(fact_path):
        affected |= _all_partitions(silver / "orders", "order_date")
        logger.info("Gold fact table missing; rebuilding all %d order dates", len(affected))

    rows = {}
    customers = build_dim_customer(read_table(spark, silver / "customers", SILVER_CUSTOMERS))
    products = build_dim_product(read_table(spark, silver / "products", SILVER_PRODUCTS))
    rows["dim_customer"] = overwrite_table(customers, gold / "dim_customer", settings.staging_dir)
    rows["dim_product"] = overwrite_table(products, gold / "dim_product", settings.staging_dir)

    if affected:
        orders = read_partitions(spark, silver / "orders", "order_date", affected, SILVER_ORDERS)
        replace_partitions(
            build_fact_order_items(orders),
            fact_path,
            "order_date",
            settings.staging_dir,
            partitions=affected,
        )

    facts = read_table(spark, fact_path)
    rows["fact_order_items"] = facts.count()
    dim_customer = read_table(spark, gold / "dim_customer")
    dim_product = read_table(spark, gold / "dim_product")
    transactions = read_table(spark, silver / "transactions", SILVER_TRANSACTIONS)

    rows["daily_revenue"] = overwrite_table(
        build_daily_revenue(facts, transactions), gold / "daily_revenue", settings.staging_dir
    )
    rows["product_performance"] = overwrite_table(
        build_product_performance(facts, dim_product),
        gold / "product_performance",
        settings.staging_dir,
    )
    rows["customer_activity"] = overwrite_table(
        build_customer_activity(facts, dim_customer),
        gold / "customer_activity",
        settings.staging_dir,
    )

    checks.enforce(
        [
            checks.duplicate_keys(facts, ["order_id", "line_number"], "unique_fact_grain"),
            checks.orphans(facts, dim_customer, "customer_id", "fact_references_dim_customer"),
            checks.orphans(facts, dim_product, "product_id", "fact_references_dim_product"),
            checks.null_values(facts, ["net_amount", "order_date"], "fact_amounts_not_null"),
        ]
    )
    logger.info("Gold refreshed for %d order dates: %s", len(affected), rows)
    return rows
