"""Row-level validation rules and quarantine routing.

A rule is a named boolean expression that is true when a row violates it. Rules must be
null-safe: a rule that evaluates to null is treated as passing, so every rule wraps its
inputs explicitly instead of relying on comparison semantics.
"""

from __future__ import annotations

from dataclasses import dataclass

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

REASONS_COLUMN = "_rejection_reasons"


@dataclass(frozen=True)
class Rule:
    name: str
    violated: Column
    description: str


def missing(column: str) -> Column:
    return F.col(column).isNull() | (F.trim(F.col(column)) == "")


def not_matching(column: str, pattern: str) -> Column:
    return F.col(column).isNotNull() & ~F.col(column).rlike(pattern)


def not_in(column: str, allowed: tuple[str, ...]) -> Column:
    return F.col(column).isNull() | ~F.col(column).isin(*allowed)


def apply_rules(frame: DataFrame, rules: list[Rule]) -> DataFrame:
    flags = [F.when(F.coalesce(rule.violated, F.lit(False)), F.lit(rule.name)) for rule in rules]
    reasons = F.filter(F.array(*flags), lambda reason: reason.isNotNull())
    return frame.withColumn(REASONS_COLUMN, reasons)


def split_valid(frame: DataFrame) -> tuple[DataFrame, DataFrame]:
    valid = frame.filter(F.size(REASONS_COLUMN) == 0).drop(REASONS_COLUMN)
    rejected = frame.filter(F.size(REASONS_COLUMN) > 0)
    return valid, rejected


def reason_counts(rejected: DataFrame) -> dict[str, int]:
    rows = (
        rejected.select(F.explode(REASONS_COLUMN).alias("reason"))
        .groupBy("reason")
        .count()
        .collect()
    )
    return {row["reason"]: row["count"] for row in sorted(rows, key=lambda row: row["reason"])}
