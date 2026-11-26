"""Spark session factory with Delta Lake and optional Maven packages enabled."""
from __future__ import annotations

import os


def create_spark(app_name: str, extra_packages: list[str] | None = None):
    try:
        from pyspark.sql import SparkSession
        from delta import configure_spark_with_delta_pip
    except ImportError as exc:  # pragma: no cover - depends on runtime environment
        raise RuntimeError(
            "PySpark and delta-spark are required. Install the project extras and configure Java 17."
        ) from exc

    packages = [
        value.strip()
        for value in os.getenv("SPARK_EXTRA_PACKAGES", "").split(",")
        if value.strip()
    ]
    if extra_packages:
        packages.extend(extra_packages)
    # Preserve order while removing duplicates.
    packages = list(dict.fromkeys(packages))

    builder = (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.shuffle.partitions", os.getenv("SPARK_SHUFFLE_PARTITIONS", "8"))
        .config("spark.sql.session.timeZone", os.getenv("SPARK_SESSION_TIMEZONE", "UTC"))
    )
    return configure_spark_with_delta_pip(
        builder, extra_packages=packages or None
    ).getOrCreate()
