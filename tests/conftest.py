from __future__ import annotations

from pathlib import Path

import pytest
from pyspark.sql import SparkSession

from lakeforge.config import Settings
from lakeforge.spark import build_spark_session


def make_settings(data_dir: Path) -> Settings:
    return Settings(data_dir=data_dir, spark_master="local[2]", shuffle_partitions=2)


@pytest.fixture(scope="session")
def spark(tmp_path_factory: pytest.TempPathFactory) -> SparkSession:
    session = build_spark_session(
        make_settings(tmp_path_factory.mktemp("spark")), "lakeforge-tests"
    )
    yield session
    session.stop()


@pytest.fixture
def settings(tmp_path: Path) -> Settings:
    return make_settings(tmp_path / "lake")
