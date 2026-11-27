"""Run the workbook data-quality rules and maintain quarantine/quality metrics."""
from __future__ import annotations

import argparse
import os

from delta.tables import DeltaTable
from pyspark.sql import functions as F
from pyspark.sql import Window

from streambharat.dq import (
    RULES,
    build_rule_metrics,
    classify_player_quality,
    classify_subscription_quality,
)
from streambharat.spark_session import create_spark


def _merge_quarantine(spark, path: str, frame) -> None:
    if frame.isEmpty():
        return
    if DeltaTable.isDeltaTable(spark, path):
        (
            DeltaTable.forPath(spark, path).alias("target")
            .merge(frame.alias("source"), "target.quarantine_id = source.quarantine_id")
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
    else:
        frame.write.format("delta").mode("append").save(path)


def run_quality(
    player_path: str = "lake/silver/player_events",
    title_path: str = "lake/silver/titles_history",
    profiles_path: str = "lake/silver/profiles",
    licences_path: str = "lake/silver/licences",
    city_state_path: str = "data/raw/reference/city_state.csv",
    subscription_path: str = "lake/bronze/billing_subscr",
    plans_path: str = "lake/silver/plans",
    output_path: str = "lake/gold/dq_metrics",
    quarantine_root: str = "lake/silver/quarantine",
    fail_on_fail: bool = False,
) -> None:
    spark = create_spark("StreamBharat-Data-Quality")
    try:
        player = spark.read.format("delta").load(player_path)
        title_history = spark.read.format("delta").load(title_path)
        title_order = Window.partitionBy("content_id").orderBy(F.col("valid_from").desc())
        current_titles = (
            title_history.withColumn("_rn", F.row_number().over(title_order))
            .filter(F.col("_rn") == 1)
            .select("content_id", "duration_sec", "age_rating")
        )
        profiles = spark.read.format("delta").load(profiles_path)
        licences = spark.read.format("delta").load(licences_path)
        city_state = spark.read.option("header", True).option("inferSchema", True).csv(city_state_path)

        player_frames = classify_player_quality(
            player, current_titles, profiles, licences, city_state
        )
        subscriptions = spark.read.format("delta").load(subscription_path)
        subscriptions = exclude_dlq_offsets(
            subscriptions, "ott.billing.subscriptions"
        )
        plans = spark.read.format("delta").load(plans_path)
        price_violations = classify_subscription_quality(subscriptions, plans)

        violations = []
        for frame in player_frames:
            rule = frame.select("rule_name", "severity").first()
            if rule is not None:
                violations.append((rule["rule_name"], rule["severity"], frame))
        violations.append(("PRICE_PLAN_MISMATCH", "FAIL", price_violations))

        counts = {name: player.count() for name, _ in RULES if name != "PRICE_PLAN_MISMATCH"}
        counts["PRICE_PLAN_MISMATCH"] = subscriptions.count()
        metrics = build_rule_metrics(counts, violations, "full-history")
        metrics.write.format("delta").mode("overwrite").option(
            "overwriteSchema", "true"
        ).save(output_path)

        kids = next(
            (frame for name, _, frame in violations if name == "KIDS_AGE_RESTRICTION"),
            None,
        )
        if kids is not None:
            (
                kids.groupBy("age_rating", "device_type", "app_version")
                .agg(F.countDistinct("event_id").alias("failing_events"))
                .orderBy(F.col("failing_events").desc())
                .write.format("delta").mode("overwrite").option(
                    "overwriteSchema", "true"
                ).save("lake/gold/dq_kids_profile_finding")
            )

        for rule_name, severity, frame in violations:
            if frame.isEmpty():
                continue
            if "event_id" in frame.columns:
                source_identity = F.col("event_id").cast("string")
            elif "quarantine_id" in frame.columns:
                source_identity = F.col("quarantine_id").cast("string")
            else:
                source_identity = F.sha2(F.to_json(F.struct(*frame.columns)), 256)
            partition_identity = (
                F.col("kafka_partition").cast("string")
                if "kafka_partition" in frame.columns else F.lit("")
            )
            offset_identity = (
                F.col("kafka_offset").cast("string")
                if "kafka_offset" in frame.columns else F.lit("")
            )
            quarantine = frame.withColumn(
                "quarantine_id",
                F.sha2(
                    F.concat_ws(
                        "|", source_identity,
                        partition_identity, offset_identity, F.lit(rule_name),
                    ),
                    256,
                ),
            ).withColumn("quarantined_at", F.current_timestamp())
            _merge_quarantine(
                spark,
                os.path.join(quarantine_root, rule_name.lower()),
                quarantine,
            )

        fail_rows = (
            metrics.filter(F.col("severity") == "FAIL")
            .agg(F.sum("failed_rows").alias("failed_rows"))
            .first()["failed_rows"] or 0
        )
        gate = spark.createDataFrame(
            [("full-history", int(fail_rows), bool(fail_rows == 0))],
            ["batch_id", "failed_fail_rows", "quality_passed"],
        ).withColumn("measured_at", F.current_timestamp())
        gate.write.format("delta").mode("overwrite").option(
            "overwriteSchema", "true"
        ).save("lake/gold/quality_gate")
        if fail_on_fail and fail_rows:
            raise RuntimeError(
                f"Quality gate failed: {fail_rows} rows violate FAIL-severity rules"
            )
    finally:
        spark.stop()


def main() -> None:
    parser = argparse.ArgumentParser(description="Run player/subscription data-quality rules")
    parser.add_argument("--player", default="lake/silver/player_events")
    parser.add_argument("--titles", default="lake/silver/titles_history")
    parser.add_argument("--profiles", default="lake/silver/profiles")
    parser.add_argument("--licences", default="lake/silver/licences")
    parser.add_argument("--city-state", default="data/raw/reference/city_state.csv")
    parser.add_argument("--subscriptions", default="lake/bronze/billing_subscr")
    parser.add_argument("--plans", default="lake/silver/plans")
    parser.add_argument("--output", default="lake/gold/dq_metrics")
    parser.add_argument("--quarantine-root", default="lake/silver/quarantine")
    parser.add_argument("--fail-on-fail", action="store_true")
    args = parser.parse_args()
    run_quality(
        args.player, args.titles, args.profiles, args.licences,
        args.city_state, args.subscriptions, args.plans, args.output,
        args.quarantine_root, args.fail_on_fail,
    )


if __name__ == "__main__":
    main()
