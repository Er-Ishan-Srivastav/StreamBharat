import os

from pyspark.sql.types import LongType, StringType, StructField, StructType

from streambharat.cdn_analytics import build_cdn_egress_daily
from streambharat.spark_session import create_spark


def test_cdn_egress_aggregates_cost_and_missing_rates():
    os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")
    spark = create_spark("streambharat-cdn-analytics-test")
    spark.sparkContext.setLogLevel("ERROR")
    try:
        request_schema = StructType([
            StructField("cdn", StringType(), False),
            StructField("region", StringType(), False),
            StructField("request_ts", StringType(), False),
            StructField("status", LongType(), False),
            StructField("bytes", LongType(), False),
            StructField("content_id", StringType(), False),
        ])
        requests = spark.createDataFrame([
            ("AkashCDN", "West", "2026-09-01T00:00:00", 200, 1024 ** 3, "C1"),
            ("AkashCDN", "West", "2026-09-01T00:01:00", 404, 1024 ** 2, "C1"),
            ("NimbusEdge", "North", "2026-09-01T00:02:00", 503, 1024 ** 3, "C2"),
        ], request_schema)
        rate_schema = StructType([
            StructField("cdn", StringType(), False),
            StructField("region", StringType(), False),
            StructField("rate_inr_per_gb", StringType(), False),
        ])
        rates = spark.createDataFrame([
            ("akashcdn", "west", "5.0"),
        ], rate_schema)

        result = build_cdn_egress_daily(requests, rates)
        rows = {
            row["cdn"]: row
            for row in result.collect()
        }
        assert rows["AkashCDN"]["request_count"] == 2
        assert round(float(rows["AkashCDN"]["egress_gb"]), 4) == 1.25
        assert round(float(rows["AkashCDN"]["cost_inr"]), 4) == 6.25
        assert rows["AkashCDN"]["http_4xx"] == 1
        assert rows["NimbusEdge"]["missing_rate_requests"] == 1
        assert rows["NimbusEdge"]["cost_inr"] is None
    finally:
        spark.stop()
