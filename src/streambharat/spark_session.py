"""Spark session factory for local mode or the bundled standalone Spark cluster."""
from __future__ import annotations

import os
import socket


def create_spark(app_name: str, extra_packages: list[str] | None = None):
    try:
        from pyspark.sql import SparkSession
    except ImportError as exc:  # pragma: no cover - depends on runtime environment
        raise RuntimeError(
            "PySpark is required. Install the project extras and configure Java 17."
        ) from exc

    master = os.getenv("SPARK_MASTER", "local[*]")
    packages = [
        value.strip()
        for value in os.getenv("SPARK_EXTRA_PACKAGES", "").split(",")
        if value.strip()
    ]
    if extra_packages:
        packages.extend(extra_packages)
    packages = list(dict.fromkeys(packages))

    builder = (
        SparkSession.builder
        .appName(app_name)
        .master(master)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.shuffle.partitions", os.getenv("SPARK_SHUFFLE_PARTITIONS", "4"))
        # Source timestamps without an explicit offset are treated as wall-clock
        # values. UTC avoids silently shifting them by the host's local offset
        # when Spark converts between JVM timestamps and Python datetimes.
        .config("spark.sql.session.timeZone", os.getenv("SPARK_SESSION_TIMEZONE", "UTC"))
        .config("spark.driver.memory", os.getenv("SPARK_DRIVER_MEMORY", "1g"))
        .config("spark.executor.memory", os.getenv("SPARK_EXECUTOR_MEMORY", "1500m"))
    )

    if master.startswith("spark://"):
        driver_host = os.getenv("SPARK_DRIVER_HOST", socket.gethostname())
        builder = (
            builder
            .config("spark.driver.host", driver_host)
            .config("spark.driver.bindAddress", "0.0.0.0")
            .config("spark.executorEnv.PYTHONPATH", os.getenv("SPARK_EXECUTOR_PYTHONPATH", os.getenv("PYTHONPATH", "")))
        )

    # The Docker Spark image already contains the exact Kafka, Delta and
    # PostgreSQL connector jars. CI and non-Docker development use the Delta
    # helper and Maven coordinates instead.
    if os.getenv("SPARK_USE_PREINSTALLED_JARS", "").lower() == "true":
        spark = builder.getOrCreate()
    else:
        try:
            from delta import configure_spark_with_delta_pip
        except ImportError as exc:  # pragma: no cover
            raise RuntimeError(
                "delta-spark is required. Install the project dependencies."
            ) from exc
        spark = configure_spark_with_delta_pip(
            builder, extra_packages=packages or None
        ).getOrCreate()

    spark.sparkContext.setLogLevel(os.getenv("SPARK_LOG_LEVEL", "WARN"))
    return spark
