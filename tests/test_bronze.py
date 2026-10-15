import shutil
from datetime import date

import pytest
from pyspark.sql import functions as F

from lakeforge.ingestion import bronze, generator

DAY_ONE = date(2026, 1, 5)


@pytest.fixture
def landed(settings):
    generator.generate(settings.landing_dir, days=2)
    return settings


def _landing_lines(settings, entity):
    path = next((settings.landing_dir / entity / f"batch_date={DAY_ONE}").iterdir())
    lines = path.read_text(encoding="utf-8").splitlines()
    return len(lines) - (0 if entity == "orders" else 1)


def test_discover_batches_requires_every_entity(landed):
    shutil.rmtree(landed.landing_dir / "transactions" / "batch_date=2026-01-06")

    assert bronze.discover_batches(landed.landing_dir) == [DAY_ONE]


def test_bronze_preserves_every_landed_row(spark, landed):
    counts = bronze.ingest_batch(spark, landed, DAY_ONE)

    for entity in ("customers", "products", "orders", "transactions"):
        assert counts[entity] == _landing_lines(landed, entity)


def test_bronze_keeps_raw_values_and_adds_metadata(spark, landed):
    bronze.ingest_batch(spark, landed, DAY_ONE)
    orders = bronze.read_bronze(spark, landed, "orders", DAY_ONE)
    transactions = bronze.read_bronze(spark, landed, "transactions", DAY_ONE)

    assert orders.filter(F.col("_corrupt_record").isNotNull()).count() == 1
    assert transactions.filter(F.col("amount") == "N/A").count() == 1
    assert transactions.filter(F.col("currency") == "usd").count() == 1

    metadata = orders.select("_source_file", "_ingested_at", "_record_hash", "_batch_date")
    assert metadata.filter(F.col("_source_file") != "orders_20260105.jsonl").count() == 0
    assert metadata.dropna().count() == orders.count()
    assert {row["_batch_date"] for row in metadata.collect()} == {DAY_ONE}


def test_bronze_is_partitioned_by_batch_date(spark, landed):
    bronze.ingest_batch(spark, landed, DAY_ONE)
    bronze.ingest_batch(spark, landed, date(2026, 1, 6))

    partitions = sorted(p.name for p in (landed.bronze_dir / "orders").iterdir() if p.is_dir())
    assert partitions == ["_batch_date=2026-01-05", "_batch_date=2026-01-06"]
    assert list((landed.bronze_dir / "orders" / partitions[0]).glob("*.parquet"))


def test_reingesting_a_batch_replaces_instead_of_appending(spark, landed):
    first = bronze.ingest_batch(spark, landed, DAY_ONE)
    second = bronze.ingest_batch(spark, landed, DAY_ONE)

    assert first == second
    total = spark.read.parquet(str(landed.bronze_dir / "orders")).count()
    assert total == first["orders"]
    assert not any(landed.staging_dir.iterdir())


def test_missing_landing_directory_raises_actionable_error(spark, settings):
    with pytest.raises(bronze.LandingDataError, match="lakeforge generate"):
        bronze.ingest_batch(spark, settings, DAY_ONE)
