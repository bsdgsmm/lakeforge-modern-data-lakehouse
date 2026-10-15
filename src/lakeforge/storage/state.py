"""Batch manifest used to make incremental runs skip work that is already done."""

from __future__ import annotations

import json
import os
from dataclasses import dataclass, field
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any


@dataclass
class Manifest:
    path: Path
    batches: dict[str, dict[str, Any]] = field(default_factory=dict)
    gold_pending: set[date] = field(default_factory=set)

    @classmethod
    def load(cls, state_dir: Path) -> Manifest:
        path = state_dir / "manifest.json"
        if not path.exists():
            return cls(path=path)
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except json.JSONDecodeError as error:
            raise RuntimeError(
                f"State manifest {path} is corrupt. Restore it from backup or rerun with "
                "--full-refresh to rebuild every layer from landing data."
            ) from error
        return cls(
            path=path,
            batches=payload.get("batches", {}),
            gold_pending={date.fromisoformat(value) for value in payload.get("gold_pending", [])},
        )

    def is_processed(self, batch_date: date) -> bool:
        return batch_date.isoformat() in self.batches

    def processed_dates(self) -> list[date]:
        return sorted(date.fromisoformat(value) for value in self.batches)

    def record(self, batch_date: date, metrics: dict[str, Any]) -> None:
        self.batches[batch_date.isoformat()] = {
            "processed_at": datetime.now(UTC).isoformat(timespec="seconds"),
            **metrics,
        }

    def reset(self) -> None:
        self.batches = {}
        self.gold_pending = set()

    def save(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        temporary = self.path.with_suffix(".json.tmp")
        temporary.write_text(
            json.dumps(
                {
                    "batches": dict(sorted(self.batches.items())),
                    "gold_pending": sorted(value.isoformat() for value in self.gold_pending),
                },
                indent=2,
            )
            + "\n",
            encoding="utf-8",
        )
        os.replace(temporary, self.path)
