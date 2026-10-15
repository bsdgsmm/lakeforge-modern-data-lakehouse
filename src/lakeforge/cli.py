from __future__ import annotations

import argparse
import json
import logging
import sys
from datetime import date
from pathlib import Path

from lakeforge.config import Settings
from lakeforge.ingestion import generator
from lakeforge.logging_config import configure_logging

logger = logging.getLogger("lakeforge")

GOLD_TABLES = (
    "dim_customer",
    "dim_product",
    "fact_order_items",
    "daily_revenue",
    "product_performance",
    "customer_activity",
)


def _parse_date(value: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as error:
        raise argparse.ArgumentTypeError(f"expected YYYY-MM-DD, got {value!r}") from error


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="lakeforge")
    parser.add_argument("--data-dir", help="Root of the lakehouse (default: $LAKEFORGE_DATA_DIR)")
    commands = parser.add_subparsers(dest="command", required=True)

    generate = commands.add_parser("generate", help="Write deterministic synthetic landing data")
    generate.add_argument("--days", type=int, default=7)
    generate.add_argument("--seed", type=int, default=generator.DEFAULT_SEED)
    generate.add_argument("--start-date", type=_parse_date, default=generator.DEFAULT_START_DATE)
    generate.add_argument(
        "--first-day-index",
        type=int,
        default=0,
        help="Offset of the first generated day; use it to append new batches later",
    )

    run = commands.add_parser("run", help="Process new landing batches through every layer")
    run.add_argument("--full-refresh", action="store_true", help="Rebuild all layers")
    run.add_argument(
        "--reprocess",
        type=_parse_date,
        nargs="+",
        default=[],
        metavar="DATE",
        help="Reprocess specific batch dates even if already processed",
    )

    sql = commands.add_parser("sql", help="Run SQL files against the Gold tables")
    sql.add_argument("files", nargs="+", type=Path)
    sql.add_argument("--limit", type=int, default=20)

    commands.add_parser("report", help="Write the data-quality manifest as JSON to stdout")
    return parser


def _spark(settings: Settings):
    from lakeforge.spark import build_spark_session

    return build_spark_session(settings)


def _run(settings: Settings, args: argparse.Namespace) -> None:
    from lakeforge.pipeline import run_pipeline

    spark = _spark(settings)
    try:
        run_pipeline(spark, settings, full_refresh=args.full_refresh, reprocess=args.reprocess)
    finally:
        spark.stop()


def register_gold_views(spark, settings: Settings) -> None:
    for table in GOLD_TABLES:
        path = settings.gold_dir / table
        if not path.is_dir():
            raise FileNotFoundError(
                f"Gold table {table} not found at {path}. Run 'lakeforge run' first."
            )
        spark.read.parquet(str(path)).createOrReplaceTempView(table)


def _sql(settings: Settings, args: argparse.Namespace) -> None:
    spark = _spark(settings)
    try:
        register_gold_views(spark, settings)
        for path in args.files:
            query = path.read_text(encoding="utf-8").strip().rstrip(";")
            logger.info("Running %s", path)
            sys.stdout.write(f"\n-- {path}\n")
            sys.stdout.flush()
            spark.sql(query).show(args.limit, truncate=False)
    finally:
        spark.stop()


def _report(settings: Settings) -> None:
    from lakeforge.storage.state import Manifest

    manifest = Manifest.load(settings.state_dir)
    if not manifest.batches:
        raise SystemExit("No processed batches yet. Run 'lakeforge run' first.")
    sys.stdout.write(json.dumps(manifest.batches, indent=2) + "\n")


def main(argv: list[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    settings = Settings.from_env(args.data_dir)
    configure_logging(settings.log_level)

    try:
        if args.command == "generate":
            generator.generate(
                settings.landing_dir,
                days=args.days,
                seed=args.seed,
                start_date=args.start_date,
                first_day_index=args.first_day_index,
            )
        elif args.command == "run":
            _run(settings, args)
        elif args.command == "sql":
            _sql(settings, args)
        elif args.command == "report":
            _report(settings)
    except (FileNotFoundError, ValueError, RuntimeError) as error:
        logger.error("%s", error)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
