"""Declarative data-quality checks for player and subscription data."""
from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


RULES = [
    ("UNKNOWN_CONTENT", "FAIL"),
    ("KIDS_AGE_RESTRICTION", "FAIL"),
    ("LICENCE_EXPIRED_FOR_STATE", "FAIL"),
    ("POSITION_AFTER_DURATION", "WARN"),
    ("PRICE_PLAN_MISMATCH", "FAIL"),
    ("MISSING_PROFILE", "WARN"),
]


def classify_player_quality(
    player_df: DataFrame,
    titles_df: DataFrame,
    profiles_df: DataFrame,
    licences_df: DataFrame,
    city_state_df: DataFrame | None = None,
) -> list[DataFrame]:
    """Return quarantinable violation frames after enriching the player rows."""
    title_columns = ["content_id", "duration_sec", "age_rating"]
    profile_columns = ["profile_id", "is_kids"]
    enriched = (
        player_df
        .join(titles_df.select(*title_columns).dropDuplicates(["content_id"]), "content_id", "left")
        .join(profiles_df.select(*profile_columns).dropDuplicates(["profile_id"]), "profile_id", "left")
    )
    if city_state_df is not None:
        city_map = city_state_df.select(
            F.col("city").alias("mapped_city"),
            F.col("state").alias("viewing_state"),
        ).dropDuplicates(["mapped_city"])
        enriched = enriched.join(city_map, F.col("ip_city") == F.col("mapped_city"), "left")
    if licences_df is not None and city_state_df is not None:
        licence_map = licences_df.select(
            "content_id",
            F.col("region").alias("licence_state"),
            F.to_date("licence_end").alias("licence_end"),
        )
        enriched = enriched.join(
            licence_map,
            (enriched.content_id == licence_map.content_id)
            & (enriched.viewing_state == licence_map.licence_state),
            "left",
        ).drop(licence_map.content_id)

    rules = [
        (
            "UNKNOWN_CONTENT",
            F.col("content_id").isNotNull() & F.col("duration_sec").isNull(),
            "FAIL",
        ),
        (
            "KIDS_AGE_RESTRICTION",
            F.coalesce(F.col("is_kids"), F.lit(False))
            & F.col("age_rating").isin("16+", "18+"),
            "FAIL",
        ),
        (
            "LICENCE_EXPIRED_FOR_STATE",
            F.col("licence_end").isNotNull()
            & (F.to_date("event_ts_corrected") > F.col("licence_end")),
            "FAIL",
        ),
        (
            "POSITION_AFTER_DURATION",
            (F.col("event_type") == "heartbeat")
            & (F.col("position_sec").cast("double") > F.col("duration_sec").cast("double")),
            "WARN",
        ),
        ("MISSING_PROFILE", F.col("profile_id").isNull() | F.col("is_kids").isNull(), "WARN"),
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


def classify_subscription_quality(
    subscription_bronze: DataFrame,
    plans_df: DataFrame,
) -> DataFrame:
    """Flag charged subscription rows whose price differs from the plan reference."""
    parsed = subscription_bronze.withColumn(
        "_payload",
        F.from_json(
            "raw_value",
            "MAP<STRING, STRING>",
        ),
    )
    subscription = parsed.select(
        F.element_at("_payload", F.lit("user_id")).alias("user_id"),
        F.element_at("_payload", F.lit("event_type")).alias("event_type"),
        F.element_at("_payload", F.lit("plan")).alias("plan"),
        F.element_at("_payload", F.lit("price")).cast("decimal(18,2)").alias("price"),
        F.to_timestamp(F.element_at("_payload", F.lit("event_ts"))).alias("event_ts"),
        "kafka_partition",
        "kafka_offset",
    )
    plans = plans_df.select(
        F.col("plan").alias("reference_plan"),
        F.col("price_inr_per_month").cast("decimal(18,2)").alias("reference_price"),
    ).dropDuplicates(["reference_plan"])
    compared = subscription.join(
        plans,
        subscription.plan == plans.reference_plan,
        "left",
    )
    charge_event = F.lower(F.col("event_type")).isin("subscribe", "renew", "upgrade")
    return (
        compared.filter(
            charge_event
            & F.col("price").isNotNull()
            & F.col("reference_price").isNotNull()
            & (F.col("price") != F.col("reference_price"))
        )
        .withColumn("rule_name", F.lit("PRICE_PLAN_MISMATCH"))
        .withColumn("severity", F.lit("FAIL"))
        .withColumn(
            "quarantine_id",
            F.sha2(
                F.concat_ws(
                    "|", "user_id", "event_type", F.col("event_ts").cast("string"),
                    F.col("price").cast("string"), "kafka_partition", "kafka_offset",
                ),
                256,
            ),
        )
    )


def build_rule_metrics(
    input_count: int | dict[str, int],
    violations: list[tuple[str, str, DataFrame]],
    batch_id: str,
) -> DataFrame:
    """Emit one metrics row per configured rule, including zero-failure rules."""
    if not violations:
        raise ValueError("at least one rule frame is required")
    rows = []
    spark = violations[0][2].sparkSession
    for rule_name, severity in RULES:
        frame = next(
            (df for name, level, df in violations if name == rule_name and level == severity),
            None,
        )
        if frame is None:
            failed = 0
        else:
            identity = "event_id" if "event_id" in frame.columns else None
            failed = (
                frame.select(identity).dropDuplicates().count()
                if identity else frame.count()
            )
        total = input_count.get(rule_name, 0) if isinstance(input_count, dict) else input_count
        rows.append((batch_id, rule_name, severity, max(total - failed, 0), failed))
    return spark.createDataFrame(
        rows,
        ["batch_id", "rule_name", "severity", "passed_rows", "failed_rows"],
    ).withColumn("measured_at", F.current_timestamp())
