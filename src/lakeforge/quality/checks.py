"""Table-level checks that run after a layer has been written."""

from __future__ import annotations

import logging
from dataclasses import dataclass

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

logger = logging.getLogger(__name__)


class DataQualityError(RuntimeError):
    pass


@dataclass(frozen=True)
class CheckResult:
    name: str
    failed_rows: int

    @property
    def passed(self) -> bool:
        return self.failed_rows == 0


def duplicate_keys(frame: DataFrame, keys: list[str], name: str) -> CheckResult:
    failed = frame.groupBy(*keys).count().filter(F.col("count") > 1).count()
    return CheckResult(name, failed)


def null_values(frame: DataFrame, columns: list[str], name: str) -> CheckResult:
    condition = F.lit(False)
    for column in columns:
        condition = condition | F.col(column).isNull()
    return CheckResult(name, frame.filter(condition).count())


def orphans(child: DataFrame, parent: DataFrame, key: str, name: str) -> CheckResult:
    missing = child.select(key).distinct().join(parent.select(key).distinct(), key, "left_anti")
    return CheckResult(name, missing.count())


def enforce(results: list[CheckResult]) -> dict[str, int]:
    failures = [result for result in results if not result.passed]
    for result in results:
        logger.info("Check %s: %d failing rows", result.name, result.failed_rows)
    if failures:
        summary = ", ".join(f"{result.name}={result.failed_rows}" for result in failures)
        raise DataQualityError(
            f"Post-write checks failed ({summary}). The previous batch state was kept in the "
            "manifest; inspect the quarantine tables and rerun the batch with --reprocess."
        )
    return {result.name: result.failed_rows for result in results}
