"""Incremental orchestration across Bronze, Silver and Gold."""

from __future__ import annotations

import logging
import shutil
from collections.abc import Iterable
from dataclasses import dataclass, field
from datetime import date

from pyspark.sql import SparkSession

from lakeforge.config import Settings
from lakeforge.ingestion import bronze
from lakeforge.storage.state import Manifest
from lakeforge.transformations import gold, silver

logger = logging.getLogger(__name__)


@dataclass
class RunSummary:
    processed: list[date] = field(default_factory=list)
    skipped: list[date] = field(default_factory=list)
    gold_rows: dict[str, int] = field(default_factory=dict)


def _reset_outputs(settings: Settings) -> None:
    for directory in (
        settings.bronze_dir,
        settings.silver_dir,
        settings.gold_dir,
        settings.quarantine_dir,
        settings.staging_dir,
    ):
        if directory.exists():
            shutil.rmtree(directory)
    logger.info("Full refresh: removed Bronze, Silver, Gold and quarantine outputs")


def run_pipeline(
    spark: SparkSession,
    settings: Settings,
    full_refresh: bool = False,
    reprocess: Iterable[date] = (),
) -> RunSummary:
    landed = bronze.discover_batches(settings.landing_dir)
    if not landed:
        raise bronze.LandingDataError(
            f"No complete landing batches found under {settings.landing_dir}. "
            "Generate sample data with 'lakeforge generate --days 7' first."
        )

    manifest = Manifest.load(settings.state_dir)
    if full_refresh:
        _reset_outputs(settings)
        manifest.reset()
        manifest.save()

    requested = set(reprocess)
    unknown = requested - set(landed)
    if unknown:
        raise ValueError(
            "Cannot reprocess batches without landing data: "
            + ", ".join(sorted(value.isoformat() for value in unknown))
        )

    summary = RunSummary()
    pending = [day for day in landed if not manifest.is_processed(day) or day in requested]
    summary.skipped = [day for day in landed if day not in pending]
    if summary.skipped:
        logger.info("Skipping %d already processed batches", len(summary.skipped))
    if not pending and not manifest.gold_pending:
        logger.info("All %d landed batches are already processed; nothing to do", len(landed))
        return summary

    for batch_date in pending:
        logger.info("Processing batch %s", batch_date)
        bronze_counts = bronze.ingest_batch(spark, settings, batch_date)
        silver_result = silver.process_batch(spark, settings, batch_date)
        silver_checks = silver.verify_silver(spark, settings)
        manifest.gold_pending |= silver_result.order_dates
        manifest.record(
            batch_date,
            {
                "bronze_rows": bronze_counts,
                "silver": silver_result.metrics(),
                "silver_checks": silver_checks,
                "order_dates_touched": sorted(day.isoformat() for day in silver_result.order_dates),
            },
        )
        manifest.save()
        summary.processed.append(batch_date)

    summary.gold_rows = gold.refresh_gold(spark, settings, manifest.gold_pending)
    manifest.gold_pending = set()
    manifest.save()
    logger.info(
        "Run complete: %d batches processed, %d skipped",
        len(summary.processed),
        len(summary.skipped),
    )
    return summary
