from pyspark.sql import SparkSession
from pyspark.sql.types import DoubleType, StringType, StructField, StructType

from streambharat.published_metrics_reconciliation import compare_published_metrics


def test_published_metric_reconciliation_applies_half_percent_tolerance_and_flags_unmapped():
    spark = (
        SparkSession.builder
        .master("local[1]")
        .appName("streambharat-published-metrics-contract-test")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    try:
        published_schema = StructType([
            StructField("month", StringType(), False),
            StructField("metric", StringType(), False),
            StructField("value", DoubleType(), False),
            StructField("published_ts", StringType(), True),
        ])
        published = spark.createDataFrame([
            ("2026-10", "Total Watch Hours", 100.0, "2026-11-01T00:00:00"),
            ("2026-10", "Total Watch Hours - Revised", 100.0, "2026-11-02T00:00:00"),
            ("2026-10", "Unmapped Course KPI", 42.0, "2026-11-01T00:00:00"),
        ], published_schema)
        actual_schema = StructType([
            StructField("month", StringType(), False),
            StructField("actual_metric", StringType(), False),
            StructField("actual_value", DoubleType(), False),
        ])
        actual = spark.createDataFrame([
            ("2026-10-01", "watch_hours", 100.4),
            ("2026-10-01", "watch_hours_bad", 101.0),
        ], actual_schema)
        alias_schema = StructType([
            StructField("published_metric", StringType(), False),
            StructField("actual_metric", StringType(), False),
        ])
        aliases = spark.createDataFrame([
            ("total_watch_hours", "watch_hours"),
            ("total_watch_hours_revised", "watch_hours_bad"),
        ], alias_schema)

        result = compare_published_metrics(published, actual, aliases, 0.5)
        statuses = {
            row.metric: row.reconciliation_status
            for row in result.collect()
        }
        assert statuses["Total Watch Hours"] == "WITHIN_TOLERANCE"
        assert statuses["Total Watch Hours - Revised"] == "VARIANCE_OVER_TOLERANCE"
        assert statuses["Unmapped Course KPI"] == "UNMAPPED_METRIC"
    finally:
        spark.stop()
