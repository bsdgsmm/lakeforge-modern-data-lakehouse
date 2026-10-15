"""Render README charts from the current Gold tables and quality manifest.

Run after `lakeforge run`; the charts reflect whatever data is in LAKEFORGE_DATA_DIR.
"""

from __future__ import annotations

import logging
from collections import Counter
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.ticker import FuncFormatter

from lakeforge.config import Settings
from lakeforge.logging_config import configure_logging
from lakeforge.spark import build_spark_session
from lakeforge.storage.state import Manifest

logger = logging.getLogger("render_charts")

OUTPUT_DIR = Path(__file__).resolve().parents[1] / "docs" / "images"
SURFACE = "#fcfcfb"
SERIES = "#2a78d6"
TEXT = "#0b0b0b"
TEXT_MUTED = "#52514e"
GRID = "#e4e3df"


def _style(ax, title: str, subtitle: str) -> None:
    ax.set_facecolor(SURFACE)
    ax.figure.set_facecolor(SURFACE)
    for side in ("top", "right", "left"):
        ax.spines[side].set_visible(False)
    ax.spines["bottom"].set_color(GRID)
    ax.tick_params(colors=TEXT_MUTED, length=0, labelsize=9)
    ax.set_title(title, loc="left", fontsize=13, color=TEXT, pad=24, fontweight="bold")
    ax.text(0, 1.02, subtitle, transform=ax.transAxes, fontsize=9, color=TEXT_MUTED)


def render_daily_revenue(spark, settings: Settings) -> Path:
    rows = (
        spark.read.parquet(str(settings.gold_dir / "daily_revenue"))
        .orderBy("revenue_date")
        .collect()
    )
    labels = [row.revenue_date.strftime("%b %d") for row in rows]
    values = [float(row.net_revenue) for row in rows]

    fig, ax = plt.subplots(figsize=(9, 4.2), dpi=150)
    bars = ax.bar(labels, values, color=SERIES, width=0.6)
    ax.bar_label(
        bars, labels=[f"${v / 1000:,.1f}k" for v in values], padding=4, fontsize=8, color=TEXT
    )
    ax.yaxis.set_major_formatter(FuncFormatter(lambda v, _: f"${v / 1000:,.0f}k"))
    ax.yaxis.grid(True, color=GRID, linewidth=0.8)
    ax.set_axisbelow(True)
    _style(
        ax,
        "Net revenue by order date",
        "gold.daily_revenue - cancelled orders excluded - synthetic data, seed 42",
    )
    fig.tight_layout()
    path = OUTPUT_DIR / "daily_revenue.png"
    fig.savefig(path, facecolor=SURFACE)
    plt.close(fig)
    return path


def render_quarantine(settings: Settings) -> Path:
    manifest = Manifest.load(settings.state_dir)
    totals: Counter[str] = Counter()
    for batch in manifest.batches.values():
        for entity, metrics in batch["silver"].items():
            for reason, count in metrics["rejection_reasons"].items():
                totals[f"{entity}: {reason}"] += count

    ranked = sorted(totals.items(), key=lambda item: item[1])
    fig, ax = plt.subplots(figsize=(9, 0.42 * len(ranked) + 1.4), dpi=150)
    bars = ax.barh([label for label, _ in ranked], [count for _, count in ranked], color=SERIES)
    ax.bar_label(bars, padding=4, fontsize=8, color=TEXT)
    ax.xaxis.set_visible(False)
    ax.spines["bottom"].set_visible(False)
    _style(
        ax,
        "Rows routed to quarantine",
        f"Silver validation across {len(manifest.batches)} batches - from _state/manifest.json",
    )
    fig.tight_layout()
    path = OUTPUT_DIR / "quarantine_reasons.png"
    fig.savefig(path, facecolor=SURFACE)
    plt.close(fig)
    return path


def main() -> None:
    settings = Settings.from_env()
    configure_logging(settings.log_level)
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    spark = build_spark_session(settings, "lakeforge-charts")
    try:
        logger.info("Wrote %s", render_daily_revenue(spark, settings))
    finally:
        spark.stop()
    logger.info("Wrote %s", render_quarantine(settings))


if __name__ == "__main__":
    main()
