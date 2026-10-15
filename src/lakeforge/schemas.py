"""Explicit schemas for every layer.

Landing and Bronze keep every business field as a string so that malformed values are
preserved for auditing instead of being silently nulled by Spark's type coercion.
Silver and Gold use proper types.
"""

from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    DateType,
    DecimalType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

ENTITIES = ("customers", "products", "orders", "transactions")

MONEY = DecimalType(12, 2)
RATE = DecimalType(5, 4)


def _strings(*names: str) -> StructType:
    return StructType([StructField(name, StringType(), True) for name in names])


CUSTOMER_FIELDS = (
    "customer_id",
    "first_name",
    "last_name",
    "email",
    "country_code",
    "signup_date",
    "updated_at",
)
PRODUCT_FIELDS = ("product_id", "product_name", "category", "unit_price", "is_active", "updated_at")
ORDER_FIELDS = ("order_id", "customer_id", "order_ts", "status", "updated_at")
ORDER_ITEM_FIELDS = ("product_id", "quantity", "unit_price", "discount_pct")
TRANSACTION_FIELDS = (
    "transaction_id",
    "order_id",
    "transaction_ts",
    "payment_method",
    "amount",
    "currency",
    "status",
)

RAW_CUSTOMERS = _strings(*CUSTOMER_FIELDS)
RAW_PRODUCTS = _strings(*PRODUCT_FIELDS)
RAW_TRANSACTIONS = _strings(*TRANSACTION_FIELDS)
RAW_ORDER_ITEM = _strings(*ORDER_ITEM_FIELDS)
RAW_ORDERS = StructType(
    [
        *[StructField(name, StringType(), True) for name in ORDER_FIELDS],
        StructField("items", ArrayType(RAW_ORDER_ITEM), True),
        StructField("_corrupt_record", StringType(), True),
    ]
)

RAW_SCHEMAS = {
    "customers": RAW_CUSTOMERS,
    "products": RAW_PRODUCTS,
    "orders": RAW_ORDERS,
    "transactions": RAW_TRANSACTIONS,
}

LANDING_FORMATS = {
    "customers": "csv",
    "products": "csv",
    "orders": "jsonl",
    "transactions": "csv",
}

BRONZE_METADATA = StructType(
    [
        StructField("_source_file", StringType(), False),
        StructField("_ingested_at", TimestampType(), False),
        StructField("_record_hash", StringType(), False),
        StructField("_batch_date", DateType(), False),
    ]
)

SILVER_CUSTOMERS = StructType(
    [
        StructField("customer_id", StringType(), False),
        StructField("first_name", StringType(), True),
        StructField("last_name", StringType(), True),
        StructField("email", StringType(), True),
        StructField("country_code", StringType(), False),
        StructField("signup_date", DateType(), False),
        StructField("updated_at", TimestampType(), False),
        StructField("_batch_date", DateType(), False),
        StructField("_record_hash", StringType(), False),
    ]
)

SILVER_PRODUCTS = StructType(
    [
        StructField("product_id", StringType(), False),
        StructField("product_name", StringType(), False),
        StructField("category", StringType(), False),
        StructField("unit_price", MONEY, False),
        StructField("is_active", BooleanType(), False),
        StructField("updated_at", TimestampType(), False),
        StructField("_batch_date", DateType(), False),
        StructField("_record_hash", StringType(), False),
    ]
)

SILVER_ORDER_ITEM = StructType(
    [
        StructField("line_number", IntegerType(), False),
        StructField("product_id", StringType(), False),
        StructField("quantity", IntegerType(), False),
        StructField("unit_price", MONEY, False),
        StructField("discount_pct", RATE, False),
    ]
)

SILVER_ORDERS = StructType(
    [
        StructField("order_id", StringType(), False),
        StructField("customer_id", StringType(), False),
        StructField("order_ts", TimestampType(), False),
        StructField("status", StringType(), False),
        StructField("updated_at", TimestampType(), False),
        StructField("items", ArrayType(SILVER_ORDER_ITEM, False), False),
        StructField("_batch_date", DateType(), False),
        StructField("_record_hash", StringType(), False),
        StructField("order_date", DateType(), False),
    ]
)

SILVER_TRANSACTIONS = StructType(
    [
        StructField("transaction_id", StringType(), False),
        StructField("order_id", StringType(), False),
        StructField("transaction_ts", TimestampType(), False),
        StructField("payment_method", StringType(), False),
        StructField("amount", MONEY, False),
        StructField("currency", StringType(), False),
        StructField("status", StringType(), False),
        StructField("_batch_date", DateType(), False),
        StructField("_record_hash", StringType(), False),
        StructField("transaction_date", DateType(), False),
    ]
)
