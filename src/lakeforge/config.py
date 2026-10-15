from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


def _env(name: str, default: str) -> str:
    value = os.environ.get(name, "").strip()
    return value or default


@dataclass(frozen=True)
class Settings:
    data_dir: Path
    spark_master: str = "local[*]"
    shuffle_partitions: int = 4
    log_level: str = "INFO"
    spark_log_level: str = "ERROR"

    @property
    def landing_dir(self) -> Path:
        return self.data_dir / "landing"

    @property
    def bronze_dir(self) -> Path:
        return self.data_dir / "bronze"

    @property
    def silver_dir(self) -> Path:
        return self.data_dir / "silver"

    @property
    def gold_dir(self) -> Path:
        return self.data_dir / "gold"

    @property
    def quarantine_dir(self) -> Path:
        return self.data_dir / "quarantine"

    @property
    def state_dir(self) -> Path:
        return self.data_dir / "_state"

    @property
    def staging_dir(self) -> Path:
        return self.data_dir / "_staging"

    @classmethod
    def from_env(cls, data_dir: str | Path | None = None) -> Settings:
        resolved = Path(data_dir) if data_dir else Path(_env("LAKEFORGE_DATA_DIR", "data"))
        return cls(
            data_dir=resolved.resolve(),
            spark_master=_env("LAKEFORGE_SPARK_MASTER", "local[*]"),
            shuffle_partitions=int(_env("LAKEFORGE_SHUFFLE_PARTITIONS", "4")),
            log_level=_env("LAKEFORGE_LOG_LEVEL", "INFO").upper(),
            spark_log_level=_env("LAKEFORGE_SPARK_LOG_LEVEL", "ERROR").upper(),
        )
