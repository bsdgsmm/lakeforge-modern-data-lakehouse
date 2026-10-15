from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType

from lakeforge.schemas import (
    BRONZE_METADATA,
    RAW_SCHEMAS,
    SILVER_CUSTOMERS,
    SILVER_ORDERS,
    SILVER_PRODUCTS,
    SILVER_TRANSACTIONS,
)

BATCH = date(2026, 1, 5)


def bronze_rows(spark: SparkSession, entity: str, rows: list[dict]) -> DataFrame:
    schema = StructType([*RAW_SCHEMAS[entity].fields, *BRONZE_METADATA.fields])
    defaults = {
        "_source_file": f"{entity}.test",
        "_ingested_at": datetime(2026, 1, 5, 23, 0),
        "_batch_date": BATCH,
    }
    records = []
    for index, row in enumerate(rows):
        record = {**defaults, "_record_hash": f"hash-{index:03d}", **row}
        records.append(tuple(record.get(name) for name in schema.fieldNames()))
    return spark.createDataFrame(records, schema)


def silver_customers(spark: SparkSession, ids: list[str]) -> DataFrame:
    rows = [
        (cid, "Ada", "Silva", None, "US", BATCH, datetime(2026, 1, 5), BATCH, f"c-{cid}")
        for cid in ids
    ]
    return spark.createDataFrame(rows, SILVER_CUSTOMERS)


def silver_products(spark: SparkSession, ids: list[str]) -> DataFrame:
    rows = [
        (pid, "Mug", "home", Decimal("10.00"), True, datetime(2026, 1, 5), BATCH, f"p-{pid}")
        for pid in ids
    ]
    return spark.createDataFrame(rows, SILVER_PRODUCTS)


def item(line, product_id, quantity, price, discount="0.0000"):
    return (line, product_id, quantity, Decimal(price), Decimal(discount))


def silver_orders(spark: SparkSession, rows: list[tuple]) -> DataFrame:
    """rows: (order_id, customer_id, order_ts, status, items)."""
    records = [
        (oid, cid, ts, status, ts, items, BATCH, f"o-{oid}", ts.date())
        for oid, cid, ts, status, items in rows
    ]
    return spark.createDataFrame(records, SILVER_ORDERS)


def silver_transactions(spark: SparkSession, rows: list[tuple]) -> DataFrame:
    """rows: (transaction_id, order_id, ts, amount, status)."""
    records = [
        (tid, oid, ts, "card", Decimal(amount), "USD", status, BATCH, f"t-{tid}", ts.date())
        for tid, oid, ts, amount, status in rows
    ]
    return spark.createDataFrame(records, SILVER_TRANSACTIONS)
