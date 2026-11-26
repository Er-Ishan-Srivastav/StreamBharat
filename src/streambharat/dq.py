"""Declarative data-quality checks for joined player, title, profile and licence data."""
from __future__ import annotations

from pyspark.sql import functions as F

RULES = [
    ("UNKNOWN_CONTENT", "FAIL"),
    ("KIDS_AGE_RESTRICTION", "FAIL"),
    ("LICENCE_EXPIRED_FOR_STATE", "FAIL"),
    ("POSITION_AFTER_DURATION", "WARN"),
    ("PRICE_PLAN_MISMATCH", "FAIL"),
    ("MISSING_PROFILE", "WARN"),
]


def classify_player_quality(player_df, titles_df, profiles_df, licences_df, city_state_df=None):
    """Return one DataFrame per rule after required enrichment joins."""
    enriched = (
        player_df
        .join(titles_df.select("content_id", "duration_sec", "age_rating"), "content_id", "left")
        .join(profiles_df.select("profile_id", "is_kids"), "profile_id", "left")
    )
    if city_state_df is not None:
        enriched = enriched.join(city_state_df.select("city", "state"), F.col("ip_city") == F.col("city"), "left")
    if licences_df is not None and city_state_df is not None:
        enriched = enriched.join(
            licences_df.select("content_id", "state", "licence_end"),
            ["content_id", "state"], "left"
        )
    rules = [
        ("UNKNOWN_CONTENT", F.col("content_id").isNotNull() & F.col("duration_sec").isNull(), "FAIL"),
        ("KIDS_AGE_RESTRICTION",
         F.coalesce(F.col("is_kids"), F.lit(False)) & F.col("age_rating").isin("16+", "18+"), "FAIL"),
        ("LICENCE_EXPIRED_FOR_STATE",
         F.col("licence_end").isNotNull() & (F.to_date("event_ts_corrected") > F.to_date("licence_end")), "FAIL"),
        ("POSITION_AFTER_DURATION",
         F.col("position_sec").cast("double") > F.col("duration_sec").cast("double"), "WARN"),
        ("MISSING_PROFILE", F.col("profile_id").isNull(), "WARN"),
    ]
    violations = []
    for rule_name, condition, severity in rules:
        if rule_name == "LICENCE_EXPIRED_FOR_STATE" and "licence_end" not in enriched.columns:
            continue
        violations.append(
            enriched.filter(condition)
            .withColumn("rule_name", F.lit(rule_name))
            .withColumn("severity", F.lit(severity))
        )
    return violations


def rule_metrics(violations, input_count: int, batch_id: str):
    return (
        violations.groupBy("rule_name", "severity")
        .agg(F.count(F.lit(1)).alias("failed_rows"))
        .withColumn("passed_rows", F.lit(input_count) - F.col("failed_rows"))
        .withColumn("batch_id", F.lit(batch_id))
        .withColumn("measured_at", F.current_timestamp())
    )
