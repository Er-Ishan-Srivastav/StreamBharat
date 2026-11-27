"""Event-time stream-stream join for ad breaks and completed impressions (Q10)."""
from __future__ import annotations

import argparse
import os

from delta.tables import DeltaTable
from pyspark.sql import functions as F
from pyspark.sql.types import MapType, StringType

from streambharat.monitoring import attach_streaming_metrics
from streambharat.spark_session import create_spark


def read_break_stream(spark, bootstrap_servers, topic, starting_offsets, watermark):
    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", bootstrap_servers)
        .option("subscribe", topic)
        .option("startingOffsets", starting_offsets)
        .option("includeHeaders", "true")
        .option("failOnDataLoss", "true")
        .load()
    )
    payload = F.from_json(F.col("value").cast("string"), MapType(StringType(), StringType()))
    return (
        raw.select(
            payload.alias("_p"),
            F.col("partition").alias("break_partition"),
            F.col("offset").alias("break_offset"),
            F.col("timestamp").alias("kafka_timestamp"),
        )
        .select(
            F.col("_p")["break_id"].alias("break_id"),
            F.col("_p")["impression_id"].alias("impression_id"),
            F.col("_p")["user_id"].alias("user_id"),
            F.col("_p")["profile_id"].alias("profile_id"),
            F.col("_p")["content_id"].alias("content_id"),
            F.to_timestamp(F.col("_p")["break_ts"]).alias("break_ts"),
            F.col("_p")["filled"].cast("int").alias("filled"),
            "break_partition", "break_offset", "kafka_timestamp",
        )
        .filter(
            F.col("break_id").isNotNull()
            & F.col("user_id").isNotNull()
            & F.col("content_id").isNotNull()
            & F.col("break_ts").isNotNull()
        )
        .withWatermark("break_ts", watermark)
    )


def read_impression_stream(spark, bootstrap_servers, topic, starting_offsets, watermark):
    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", bootstrap_servers)
        .option("subscribe", topic)
        .option("startingOffsets", starting_offsets)
        .option("includeHeaders", "true")
        .option("failOnDataLoss", "true")
        .load()
    )
    payload = F.from_json(F.col("value").cast("string"), MapType(StringType(), StringType()))
    parsed = raw.select(
        payload.alias("_p"),
        F.col("partition").alias("impression_partition"),
        F.col("offset").alias("impression_offset"),
        F.col("timestamp").alias("kafka_timestamp"),
    ).select(
        F.col("_p")["impression_id"].alias("impression_id"),
        F.col("_p")["user_id"].alias("user_id"),
        F.col("_p")["profile_id"].alias("profile_id"),
        F.col("_p")["content_id"].alias("content_id"),
        F.col("_p")["campaign_id"].alias("campaign_id"),
        F.to_timestamp(F.col("_p")["ad_break_ts"]).alias("ad_break_ts"),
        F.col("_p")["duration_sec"].cast("int").alias("duration_sec"),
        F.col("_p")["completed"].cast("int").alias("completed"),
        F.col("_p")["cpm_inr"].cast("decimal(18,4)").alias("cpm_inr"),
        "impression_partition", "impression_offset", "kafka_timestamp",
    ).filter(
        F.col("impression_id").isNotNull()
        & F.col("user_id").isNotNull()
        & F.col("content_id").isNotNull()
        & F.col("ad_break_ts").isNotNull()
        & F.col("duration_sec").isNotNull()
        & (F.col("duration_sec") > 0)
    )
    return (
        parsed
        .withColumn(
            "impression_end_ts",
            F.from_unixtime(
                F.unix_timestamp("ad_break_ts") + F.col("duration_sec")
            ).cast("timestamp"),
        )
        .withWatermark("impression_end_ts", watermark)
    )


def _merge_delivery(batch_df, batch_id, output_path):
    batch = batch_df.dropDuplicates(["delivery_id"])
    if batch.isEmpty():
        return
    if DeltaTable.isDeltaTable(batch.sparkSession, output_path):
        (
            DeltaTable.forPath(batch.sparkSession, output_path).alias("target")
            .merge(batch.alias("source"), "target.delivery_id = source.delivery_id")
            .whenNotMatchedInsertAll()
            .execute()
        )
    else:
        batch.write.format("delta").mode("append").save(output_path)


def run_ad_matching_stream(
    bootstrap_servers=None,
    breaks_topic="ott.ads.breaks",
    impressions_topic="ott.ads.impressions",
    matched_path="lake/silver/ad_delivery_stream",
    orphan_path="lake/silver/orphan_impressions_stream",
    matched_checkpoint="lake/_checkpoints/ad_break_impression_join",
    orphan_checkpoint="lake/_checkpoints/orphan_impression_join",
    starting_offsets="latest",
    watermark="45 minutes",
    lateness_minutes=15,
):
    bootstrap_servers = bootstrap_servers or os.getenv(
        "KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"
    )
    if lateness_minutes < 0:
        raise ValueError("lateness_minutes cannot be negative")
    spark = create_spark(
        "StreamBharat-RealTime-Ad-Matching",
        extra_packages=["org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3"],
    )
    listener = attach_streaming_metrics(spark, bootstrap_servers)
    queries = []
    try:
        breaks = read_break_stream(
            spark, bootstrap_servers, breaks_topic, starting_offsets, watermark
        )
        impressions = read_impression_stream(
            spark, bootstrap_servers, impressions_topic, starting_offsets, watermark
        )
        time_bound = F.expr(
            f"b.break_ts + INTERVAL {lateness_minutes} MINUTES + INTERVAL 30 SECONDS"
        )
        condition = (
            (F.col("b.impression_id") == F.col("i.impression_id"))
            & (F.col("b.user_id") == F.col("i.user_id"))
            & (F.col("b.content_id") == F.col("i.content_id"))
            & (F.col("i.impression_end_ts") >= F.col("b.break_ts"))
            & (F.col("i.impression_end_ts") <= time_bound)
        )

        matched = (
            breaks.alias("b").join(impressions.alias("i"), condition, "leftOuter")
            .select(
                F.coalesce(
                    F.col("i.impression_id"),
                    F.concat(F.lit("BREAK:"), F.col("b.break_id")),
                ).alias("delivery_id"),
                F.col("b.break_id").alias("break_id"),
                F.col("i.impression_id").alias("impression_id"),
                F.col("i.campaign_id").alias("campaign_id"),
                F.col("b.user_id").alias("user_id"),
                F.coalesce(F.col("i.profile_id"), F.col("b.profile_id")).alias("profile_id"),
                F.col("b.content_id").alias("content_id"),
                F.col("b.filled").alias("break_filled"),
                F.col("b.break_ts").alias("break_ts"),
                F.col("i.ad_break_ts").alias("ad_break_ts"),
                F.col("i.impression_end_ts").alias("impression_end_ts"),
                F.col("i.duration_sec").alias("duration_sec"),
                F.col("i.completed").alias("completed"),
                F.col("i.cpm_inr").alias("cpm_inr"),
                F.when(F.col("i.impression_id").isNotNull(), F.lit("DELIVERED"))
                 .when(F.coalesce(F.col("b.filled"), F.lit(0)) == 0, F.lit("UNFILLED_BREAK"))
                 .otherwise(F.lit("FILLED_WITHOUT_IMPRESSION")).alias("delivery_status"),
                F.to_date("b.break_ts").alias("activity_date"),
                F.col("b.break_partition").alias("break_partition"),
                F.col("b.break_offset").alias("break_offset"),
                F.col("i.impression_partition").alias("impression_partition"),
                F.col("i.impression_offset").alias("impression_offset"),
            )
        )
        matched_query = (
            matched.writeStream.outputMode("append")
            .foreachBatch(lambda frame, batch_id: _merge_delivery(frame, batch_id, matched_path))
            .option("checkpointLocation", matched_checkpoint)
            .queryName("streambharat_ad_break_impression_join")
            .start()
        )
        queries.append(matched_query)

        orphan = (
            impressions.alias("i").join(breaks.alias("b"), condition, "leftOuter")
            .filter(F.col("b.break_id").isNull())
            .select(
                F.concat(F.lit("IMPRESSION:"), F.col("i.impression_id")).alias("delivery_id"),
                F.lit(None).cast("string").alias("break_id"),
                F.col("i.impression_id").alias("impression_id"),
                F.col("i.campaign_id").alias("campaign_id"),
                F.col("i.user_id").alias("user_id"),
                F.col("i.profile_id").alias("profile_id"),
                F.col("i.content_id").alias("content_id"),
                F.lit(None).cast("int").alias("break_filled"),
                F.lit(None).cast("timestamp").alias("break_ts"),
                F.col("i.ad_break_ts").alias("ad_break_ts"),
                F.col("i.impression_end_ts").alias("impression_end_ts"),
                F.col("i.duration_sec").alias("duration_sec"),
                F.col("i.completed").alias("completed"),
                F.col("i.cpm_inr").alias("cpm_inr"),
                F.lit("ORPHAN_IMPRESSION").alias("delivery_status"),
                F.to_date("i.ad_break_ts").alias("activity_date"),
                F.lit(None).cast("int").alias("break_partition"),
                F.lit(None).cast("long").alias("break_offset"),
                F.col("i.impression_partition").alias("impression_partition"),
                F.col("i.impression_offset").alias("impression_offset"),
            )
        )
        orphan_query = (
            orphan.writeStream.outputMode("append")
            .foreachBatch(lambda frame, batch_id: _merge_delivery(frame, batch_id, orphan_path))
            .option("checkpointLocation", orphan_checkpoint)
            .queryName("streambharat_orphan_impressions")
            .start()
        )
        queries.append(orphan_query)
        spark.streams.awaitAnyTermination()
    finally:
        for query in queries:
            if query.isActive:
                query.stop()
        spark.streams.removeListener(listener)
        spark.stop()


def main():
    parser = argparse.ArgumentParser(description="Run the Q10 Kafka stream-stream ad join")
    parser.add_argument("--bootstrap-servers", default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"))
    parser.add_argument("--breaks-topic", default="ott.ads.breaks")
    parser.add_argument("--impressions-topic", default="ott.ads.impressions")
    parser.add_argument("--matched-output", default="lake/silver/ad_delivery_stream")
    parser.add_argument("--orphan-output", default="lake/silver/orphan_impressions_stream")
    parser.add_argument("--matched-checkpoint", default="lake/_checkpoints/ad_break_impression_join")
    parser.add_argument("--orphan-checkpoint", default="lake/_checkpoints/orphan_impression_join")
    parser.add_argument("--starting-offsets", choices=["earliest", "latest"], default="latest")
    parser.add_argument("--watermark", default="45 minutes")
    parser.add_argument("--lateness-minutes", type=int, default=15)
    args = parser.parse_args()
    run_ad_matching_stream(
        args.bootstrap_servers, args.breaks_topic, args.impressions_topic,
        args.matched_output, args.orphan_output, args.matched_checkpoint,
        args.orphan_checkpoint, args.starting_offsets, args.watermark,
        args.lateness_minutes,
    )


if __name__ == "__main__":
    main()
