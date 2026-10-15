"""Parquet read/write helpers with idempotent overwrite semantics.

Spark refuses to overwrite a path it is reading in the same plan, and Silver/Gold
merges always read the partitions they are about to replace. Every write therefore goes
to a staging directory first and is then swapped into place partition by partition.
The swap is a local rename, which is atomic per directory on POSIX filesystems but not
across partitions; a crash mid-swap is recovered by rerunning the same batch.
"""

from __future__ import annotations

import logging
import shutil
import uuid
from collections.abc import Iterable
from datetime import date
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType

logger = logging.getLogger(__name__)


def table_exists(path: Path) -> bool:
    return path.is_dir() and any(
        child.suffix == ".parquet" or "=" in child.name for child in path.iterdir()
    )


def read_table(spark: SparkSession, path: Path, schema: StructType | None = None) -> DataFrame:
    if table_exists(path):
        return spark.read.parquet(str(path))
    if schema is None:
        raise FileNotFoundError(f"Parquet table not found: {path}")
    return spark.createDataFrame([], schema)


def read_partitions(
    spark: SparkSession,
    path: Path,
    partition_column: str,
    values: Iterable[date],
    schema: StructType,
) -> DataFrame:
    """Read only the requested partitions, returning an empty frame when none exist."""
    existing = [
        str(path / f"{partition_column}={value.isoformat()}")
        for value in sorted(set(values))
        if (path / f"{partition_column}={value.isoformat()}").is_dir()
    ]
    if not existing:
        return spark.createDataFrame([], schema)
    frame = spark.read.option("basePath", str(path)).parquet(*existing)
    return frame.select(*schema.fieldNames())


def _staging_path(staging_root: Path, target: Path) -> Path:
    return staging_root / f"{target.name}-{uuid.uuid4().hex}"


def overwrite_table(frame: DataFrame, target: Path, staging_root: Path) -> int:
    """Replace an unpartitioned table and return the number of rows written."""
    staging = _staging_path(staging_root, target)
    frame.write.mode("overwrite").parquet(str(staging))
    rows = _count_rows(frame.sparkSession, staging)

    target.parent.mkdir(parents=True, exist_ok=True)
    backup = target.with_name(f".{target.name}.previous")
    if backup.exists():
        shutil.rmtree(backup)
    if target.exists():
        target.rename(backup)
    staging.rename(target)
    if backup.exists():
        shutil.rmtree(backup)
    logger.info("Wrote %d rows to %s", rows, target)
    return rows


def replace_partitions(
    frame: DataFrame,
    target: Path,
    partition_column: str,
    staging_root: Path,
    partitions: Iterable[date] = (),
) -> int:
    """Overwrite the partitions present in ``frame`` and leave every other partition untouched.

    ``partitions`` lists additional partition values that must be cleared even when the
    new frame no longer contains rows for them.
    """
    staging = _staging_path(staging_root, target)
    frame.write.mode("overwrite").partitionBy(partition_column).parquet(str(staging))
    rows = _count_rows(frame.sparkSession, staging)

    target.mkdir(parents=True, exist_ok=True)
    produced = {child.name for child in staging.iterdir() if child.is_dir() and "=" in child.name}
    cleared = {f"{partition_column}={value.isoformat()}" for value in partitions}

    for name in sorted(cleared - produced):
        stale = target / name
        if stale.exists():
            shutil.rmtree(stale)

    for name in sorted(produced):
        destination = target / name
        if destination.exists():
            shutil.rmtree(destination)
        (staging / name).rename(destination)

    shutil.rmtree(staging)
    logger.info("Wrote %d rows across %d partitions of %s", rows, len(produced), target)
    return rows


def _count_rows(spark: SparkSession, path: Path) -> int:
    if not table_exists(path):
        return 0
    return spark.read.parquet(str(path)).count()
