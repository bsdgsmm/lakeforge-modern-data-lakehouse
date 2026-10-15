"""End-to-end tests for incremental processing, reruns and partition layout."""

import shutil
from datetime import date

import pytest
from pyspark.sql import functions as F

from lakeforge.ingestion import generator
from lakeforge.pipeline import run_pipeline
from lakeforge.storage.state import Manifest
from tests.conftest import make_settings

pytestmark = pytest.mark.integration

DAY_1, DAY_2, DAY_3 = date(2026, 1, 5), date(2026, 1, 6), date(2026, 1, 7)
SNAPSHOT_TABLES = (
    "silver/customers",
    "silver/products",
    "silver/orders",
    "silver/transactions",
    "gold/fact_order_items",
    "gold/daily_revenue",
    "gold/product_performance",
    "gold/customer_activity",
)


def snapshot(spark, settings):
    tables = {}
    for table in SNAPSHOT_TABLES:
        frame = spark.read.parquet(str(settings.data_dir / table))
        tables[table] = sorted(
            frame.select(F.to_json(F.struct(*sorted(frame.columns)))).collect(), key=str
        )
    return tables


@pytest.fixture(scope="module")
def two_day_lake(spark, tmp_path_factory):
    settings = make_settings(tmp_path_factory.mktemp("two-day") / "lake")
    generator.generate(settings.landing_dir, days=2)
    summary = run_pipeline(spark, settings)
    assert summary.processed == [DAY_1, DAY_2]
    return settings


@pytest.fixture
def lake(two_day_lake, tmp_path):
    settings = make_settings(tmp_path / "lake")
    shutil.copytree(two_day_lake.data_dir, settings.data_dir)
    return settings


def test_rerun_without_new_data_is_a_no_op(spark, lake):
    before = snapshot(spark, lake)

    summary = run_pipeline(spark, lake)

    assert summary.processed == []
    assert summary.skipped == [DAY_1, DAY_2]
    assert snapshot(spark, lake) == before


def test_reprocessing_a_batch_is_idempotent(spark, lake):
    before = snapshot(spark, lake)

    summary = run_pipeline(spark, lake, reprocess=[DAY_1])

    assert summary.processed == [DAY_1]
    assert snapshot(spark, lake) == before


def test_new_batch_is_processed_incrementally_and_matches_full_refresh(spark, lake):
    generator.generate(lake.landing_dir, days=1, first_day_index=2)

    summary = run_pipeline(spark, lake)
    assert summary.processed == [DAY_3]
    touched = Manifest.load(lake.state_dir).batches[DAY_3.isoformat()]["order_dates_touched"]
    assert DAY_2.isoformat() in touched, "late orders and status updates must reopen day 2"
    incremental = snapshot(spark, lake)

    rebuilt = run_pipeline(spark, lake, full_refresh=True)
    assert rebuilt.processed == [DAY_1, DAY_2, DAY_3]
    assert snapshot(spark, lake) == incremental


def test_status_updates_replace_earlier_versions(spark, two_day_lake):
    updates = generator.build_batch(generator.DEFAULT_SEED, 1).orders
    original = {
        o["order_id"]: o["status"] for o in generator.build_batch(generator.DEFAULT_SEED, 0).orders
    }
    changed = {
        o["order_id"]: o["status"]
        for o in updates
        if o["order_id"] in original and o["status"] != original[o["order_id"]]
    }
    assert changed

    orders = spark.read.parquet(str(two_day_lake.silver_dir / "orders"))
    stored = {
        row.order_id: row.status
        for row in orders.filter(F.col("order_id").isin(*changed)).collect()
    }
    assert stored == changed
    assert orders.count() == orders.select("order_id").distinct().count()


def test_layers_are_partitioned_by_the_documented_columns(spark, two_day_lake):
    def partitions(path):
        return sorted(child.name for child in path.iterdir() if child.is_dir())

    root = two_day_lake.data_dir
    assert partitions(root / "bronze" / "orders") == [
        "_batch_date=2026-01-05",
        "_batch_date=2026-01-06",
    ]
    assert partitions(root / "silver" / "orders") == [
        "order_date=2026-01-05",
        "order_date=2026-01-06",
    ]
    assert partitions(root / "gold" / "fact_order_items") == partitions(root / "silver" / "orders")
    assert partitions(root / "quarantine" / "orders") == partitions(root / "bronze" / "orders")

    for partition in partitions(root / "silver" / "orders"):
        files = list((root / "silver" / "orders" / partition).glob("*.parquet"))
        assert files
        expected = date.fromisoformat(partition.split("=")[1])
        stored = spark.read.parquet(*map(str, files)).select(F.to_date("order_ts")).distinct()
        assert [row[0] for row in stored.collect()] == [expected]

    assert not any(two_day_lake.staging_dir.iterdir())


def test_quality_metrics_are_recorded_per_batch(spark, two_day_lake):
    manifest = Manifest.load(two_day_lake.state_dir)
    orders = manifest.batches[DAY_2.isoformat()]["silver"]["orders"]

    assert orders["rejection_reasons"] == {
        "invalid_item": 1,
        "invalid_order_ts": 1,
        "malformed_record": 1,
        "missing_customer_id": 1,
        "orphan_customer": 1,
    }
    assert orders["rejected_rows"] == 5
    assert orders["duplicates_in_batch"] == 2
    assert orders["valid_rows"] + orders["rejected_rows"] == orders["input_rows"]

    quarantined = spark.read.parquet(str(two_day_lake.quarantine_dir / "orders"))
    assert quarantined.filter(F.col("_batch_date") == DAY_2).count() == 5
    for batch in manifest.batches.values():
        assert set(batch["silver_checks"].values()) == {0}
