"""Spark session factory with Delta Lake extensions enabled."""


def create_spark(app_name: str):
    try:
        from pyspark.sql import SparkSession
        from delta import configure_spark_with_delta_pip
    except ImportError as exc:  # pragma: no cover - depends on runtime environment
        raise RuntimeError(
            "PySpark and delta-spark are required. Install requirements.txt and configure Java 17."
        ) from exc

    builder = (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.shuffle.partitions", "8")
    )
    return configure_spark_with_delta_pip(builder).getOrCreate()
