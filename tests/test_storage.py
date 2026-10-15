from datetime import date

import pytest

from lakeforge.storage.parquet import overwrite_table, read_partitions, replace_partitions
from lakeforge.storage.state import Manifest

JAN_1, JAN_2, JAN_3 = date(2026, 1, 1), date(2026, 1, 2), date(2026, 1, 3)


def _frame(spark, rows):
    return spark.createDataFrame(rows, "id INT, value STRING, day DATE")


def _all_rows(spark, path):
    return sorted(tuple(row) for row in spark.read.parquet(str(path)).collect())


def test_replace_partitions_only_touches_written_partitions(spark, settings):
    target = settings.silver_dir / "events"
    replace_partitions(
        _frame(spark, [(1, "a", JAN_1), (2, "b", JAN_2)]), target, "day", settings.staging_dir
    )
    replace_partitions(_frame(spark, [(3, "c", JAN_2)]), target, "day", settings.staging_dir)

    assert _all_rows(spark, target) == [(1, "a", JAN_1), (3, "c", JAN_2)]


def test_replace_partitions_clears_requested_partitions_without_new_rows(spark, settings):
    target = settings.silver_dir / "events"
    replace_partitions(
        _frame(spark, [(1, "a", JAN_1), (2, "b", JAN_2)]), target, "day", settings.staging_dir
    )
    replace_partitions(
        _frame(spark, [(3, "c", JAN_3)]), target, "day", settings.staging_dir, partitions=[JAN_2]
    )

    assert _all_rows(spark, target) == [(1, "a", JAN_1), (3, "c", JAN_3)]
    assert not any(settings.staging_dir.iterdir())


def test_merge_can_read_and_rewrite_the_same_partition(spark, settings):
    target = settings.silver_dir / "events"
    replace_partitions(_frame(spark, [(1, "a", JAN_1)]), target, "day", settings.staging_dir)

    schema = _frame(spark, []).schema
    current = read_partitions(spark, target, "day", [JAN_1], schema)
    merged = current.unionByName(_frame(spark, [(2, "b", JAN_1)]))
    replace_partitions(merged, target, "day", settings.staging_dir)

    assert _all_rows(spark, target) == [(1, "a", JAN_1), (2, "b", JAN_1)]


def test_read_partitions_returns_empty_frame_for_missing_partitions(spark, settings):
    schema = _frame(spark, []).schema
    frame = read_partitions(spark, settings.silver_dir / "missing", "day", [JAN_1], schema)

    assert frame.count() == 0
    assert frame.schema.fieldNames() == ["id", "value", "day"]


def test_overwrite_table_replaces_content(spark, settings):
    target = settings.gold_dir / "dim"
    overwrite_table(_frame(spark, [(1, "a", JAN_1)]), target, settings.staging_dir)
    written = overwrite_table(_frame(spark, [(2, "b", JAN_2)]), target, settings.staging_dir)

    assert written == 1
    assert _all_rows(spark, target) == [(2, "b", JAN_2)]


def test_manifest_round_trip(tmp_path):
    manifest = Manifest.load(tmp_path)
    manifest.record(JAN_2, {"rows": 3})
    manifest.gold_pending = {JAN_2}
    manifest.save()

    loaded = Manifest.load(tmp_path)
    assert loaded.is_processed(JAN_2)
    assert not loaded.is_processed(JAN_1)
    assert loaded.batches["2026-01-02"]["rows"] == 3
    assert loaded.gold_pending == {JAN_2}


def test_corrupt_manifest_fails_with_recovery_hint(tmp_path):
    (tmp_path / "manifest.json").write_text("{not json", encoding="utf-8")

    with pytest.raises(RuntimeError, match="--full-refresh"):
        Manifest.load(tmp_path)
