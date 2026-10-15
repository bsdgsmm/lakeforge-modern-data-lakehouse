from __future__ import annotations

from pyspark.sql import SparkSession

from lakeforge.config import Settings


def build_spark_session(settings: Settings, app_name: str = "lakeforge") -> SparkSession:
    spark = (
        SparkSession.builder.appName(app_name)
        .master(settings.spark_master)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", str(settings.shuffle_partitions))
        .config("spark.sql.parquet.compression.codec", "snappy")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.ui.enabled", "false")
        .config("spark.driver.extraJavaOptions", "-Duser.timezone=UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel(settings.spark_log_level)
    return spark
