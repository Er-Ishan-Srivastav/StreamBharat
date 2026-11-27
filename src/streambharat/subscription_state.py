"""Build subscription SCD2 history and publish current state to compacted Kafka.

Payment failure enters a seven-day grace state. If an active/renewal event
arrives in event time within that period, the failure is not counted as churn.
Otherwise a deterministic synthetic cancellation is inserted at grace expiry.
"""
from __future__ import annotations

import argparse
import os

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import MapType, StringType

from streambharat.dlq_utils import exclude_dlq_offsets
from streambharat.spark_session import create_spark

ACTIVE_TYPES = (
    "subscribe", "subscribed", "activate", "activated", "active",
    "renew", "renewal", "renewed", "downgrade", "downgraded",
    "payment_success", "payment_succeeded",
    "subscription_success", "subscription_activated", "resume", "resumed",
    "reactivate", "upgrade", "upgraded",
)
TRIAL_TYPES = ("trial", "trial_start", "trial_started", "start_trial")
PAUSED_TYPES = ("pause", "paused", "subscription_paused")
FAILED_TYPES = (
    "payment_failed", "payment_failure", "failed_payment",
    "charge_failed", "renewal_failed", "payment_declined",
)
CANCELLED_TYPES = (
    "cancel", "cancelled", "canceled", "subscription_cancelled",
    "subscription_canceled", "voluntary_cancel",
)


def parse_subscription_events(bronze: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Parse source events and return (valid canonical events, invalid rows)."""
    parsed = (
        bronze.withColumn(
            "_payload",
            F.from_json(
                F.col("raw_value"),
                MapType(StringType(), StringType()),
            ),
        )
        .withColumn("user_id", F.trim(F.col("_payload")["user_id"]))
        .withColumn("event_type_raw", F.trim(F.col("_payload")["event_type"]))
        .withColumn(
            "event_type",
            F.lower(F.regexp_replace("event_type_raw", r"[^A-Za-z0-9]+", "_")),
        )
        .withColumn("plan", F.trim(F.col("_payload")["plan"]))
        .withColumn("event_ts", F.to_timestamp(F.col("_payload")["event_ts"]))
        .withColumn(
            "status",
            F.when(F.col("event_type").isin(*ACTIVE_TYPES), F.lit("active"))
            .when(F.col("event_type").isin(*TRIAL_TYPES), F.lit("trial"))
            .when(F.col("event_type").isin(*PAUSED_TYPES), F.lit("paused"))
            .when(F.col("event_type").isin(*FAILED_TYPES), F.lit("payment_failed"))
            .when(F.col("event_type").isin(*CANCELLED_TYPES), F.lit("cancelled")),
        )
        .withColumn(
            "reason_code",
            F.when(F.col("_payload").isNull(), F.lit("MALFORMED_JSON"))
            .when(
                F.col("user_id").isNull() | (F.length("user_id") == 0),
                F.lit("MISSING_USER_ID"),
            )
            .when(F.col("event_ts").isNull(), F.lit("INVALID_EVENT_TIMESTAMP"))
            .when(F.col("status").isNull(), F.lit("UNKNOWN_SUBSCRIPTION_EVENT")),
        )
    )
    invalid = (
        parsed.filter(F.col("reason_code").isNotNull())
        .select(
            F.concat_ws(
                ":", "source_topic", "kafka_partition", "kafka_offset"
            ).alias("dlq_id"),
            "source_topic", "kafka_partition", "kafka_offset",
            "kafka_timestamp", "raw_value", "reason_code",
        )
        .withColumn("quarantined_at", F.current_timestamp())
    )

    valid = (
        parsed.filter(F.col("reason_code").isNull())
        .withColumn(
            "event_id",
            F.coalesce(
                F.col("_payload")["event_id"],
                F.sha2(
                    F.concat_ws(
                        "|",
                        "user_id",
                        "event_type",
                        F.col("event_ts").cast("string"),
                        F.coalesce("plan", F.lit("")),
                        F.coalesce(F.col("_payload")["price"], F.lit("")),
                    ),
                    256,
                ),
            ),
        )
        .withColumn("is_synthetic", F.lit(False))
        .select(
            "event_id", "user_id", "plan", "event_type", "status", "event_ts",
            "kafka_timestamp", "kafka_partition", "kafka_offset", "is_synthetic",
        )
        .dropDuplicates(["event_id"])
    )
    return valid, invalid


def build_subscription_history(events: DataFrame) -> DataFrame:
    """Return deterministic SCD2 states with synthetic expiry for failed payments."""
    ordering = [
        F.col("event_ts").asc(),
        F.col("kafka_timestamp").asc_nulls_last(),
        F.col("kafka_partition").asc_nulls_last(),
        F.col("kafka_offset").asc_nulls_last(),
        F.col("event_id").asc(),
    ]
    user_order = Window.partitionBy("user_id").orderBy(*ordering)
    future_events = user_order.rowsBetween(1, Window.unboundedFollowing)

    failed = (
        events.withColumn(
            "_next_active_ts",
            F.min(
                F.when(F.col("status") == "active", F.col("event_ts"))
            ).over(future_events),
        )
        .withColumn(
            "_grace_expiry",
            F.col("event_ts") + F.expr("INTERVAL 7 DAYS"),
        )
        .filter(
            (F.col("status") == "payment_failed")
            & (
                F.col("_next_active_ts").isNull()
                | (F.col("_next_active_ts") > F.col("_grace_expiry"))
            )
        )
    )

    synthetic = failed.select(
        F.sha2(
            F.concat_ws(
                "|",
                "user_id",
                F.col("event_ts").cast("string"),
                F.lit("payment_failure_expired"),
            ),
            256,
        ).alias("event_id"),
        "user_id",
        "plan",
        F.lit("payment_failure_expired").alias("event_type"),
        F.lit("cancelled").alias("status"),
        F.col("_grace_expiry").alias("event_ts"),
        F.col("kafka_timestamp"),
        F.col("kafka_partition"),
        F.col("kafka_offset"),
        F.lit(True).alias("is_synthetic"),
    )

    all_events = events.unionByName(synthetic)

    # Preserve the latest known plan when an event omits it.
    all_order = Window.partitionBy("user_id").orderBy(
        F.col("event_ts").asc(),
        F.col("is_synthetic").asc(),
        F.col("kafka_timestamp").asc_nulls_last(),
        F.col("kafka_partition").asc_nulls_last(),
        F.col("kafka_offset").asc_nulls_last(),
        F.col("event_id").asc(),
    )
    all_history = all_events.withColumn(
        "plan",
        F.last("plan", ignorenulls=True).over(
            all_order.rowsBetween(Window.unboundedPreceding, Window.currentRow)
        ),
    )
    transition = (
        all_history
        .withColumn("previous_status", F.lag("status").over(all_order))
        .withColumn("previous_plan", F.lag("plan").over(all_order))
        .filter(
            F.col("previous_status").isNull()
            | (F.col("status") != F.col("previous_status"))
            | (~F.col("plan").eqNullSafe(F.col("previous_plan")))
        )
    )
    state_order = Window.partitionBy("user_id").orderBy(
        F.col("event_ts").asc(),
        F.col("is_synthetic").asc(),
        F.col("kafka_timestamp").asc_nulls_last(),
        F.col("kafka_partition").asc_nulls_last(),
        F.col("kafka_offset").asc_nulls_last(),
        F.col("event_id").asc(),
    )
    history = (
        transition
        .withColumn("valid_from", F.col("event_ts"))
        .withColumn("valid_to", F.lead("event_ts").over(state_order))
        .withColumn("is_current", F.col("valid_to").isNull())
        .withColumn(
            "churn_type",
            F.when(
                (F.col("status") == "cancelled") & F.col("is_synthetic"),
                F.lit("INVOLUNTARY"),
            ).when(
                (F.col("status") == "cancelled") & ~F.col("is_synthetic"),
                F.lit("VOLUNTARY"),
            ),
        )
        .select(
            "user_id", "plan", "status", "event_id", "event_type",
            "valid_from", "valid_to", "is_current", "churn_type",
            "is_synthetic", "kafka_timestamp", "kafka_partition", "kafka_offset",
        )
    )
    return history


def run_subscription_state(
    bronze_path: str = "lake/bronze/billing_subscr",
    silver_path: str = "lake/silver/subscriptions",
    churn_path: str = "lake/gold/subscription_churn",
    invalid_path: str = "lake/bronze/dlq/subscriptions",
    bootstrap_servers: str | None = None,
    topic: str = "ott.ref.subscription_state",
) -> None:
    spark = create_spark(
        "StreamBharat-Subscription-State",
        extra_packages=(
            ["org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3"]
            if bootstrap_servers else None
        ),
    )
    try:
        bronze = spark.read.format("delta").load(bronze_path)
        bronze = exclude_dlq_offsets(bronze, "ott.billing.subscriptions")
        events, invalid = parse_subscription_events(bronze)
        if not invalid.isEmpty():
            if DeltaTable.isDeltaTable(spark, invalid_path):
                (
                    DeltaTable.forPath(spark, invalid_path).alias("target")
                    .merge(invalid.alias("source"), "target.dlq_id = source.dlq_id")
                    .whenNotMatchedInsertAll()
                    .execute()
                )
            else:
                invalid.write.format("delta").mode("append").save(invalid_path)

        history = build_subscription_history(events)
        history.write.format("delta").mode("overwrite").option(
            "overwriteSchema", "true"
        ).save(silver_path)

        churn = (
            history.filter(F.col("churn_type").isNotNull())
            .groupBy("churn_type")
            .agg(F.countDistinct("user_id").alias("users"))
            .withColumn("refreshed_at", F.current_timestamp())
        )
        churn.write.format("delta").mode("overwrite").save(churn_path)

        if bootstrap_servers:
            current = history.filter(F.col("is_current")).select(
                "user_id", "plan", "status", "valid_from", "churn_type"
            )
            kafka_rows = current.select(
                F.col("user_id").cast("string").alias("key"),
                F.to_json(
                    F.struct(
                        "user_id", "plan", "status", "valid_from", "churn_type",
                        F.current_timestamp().alias("published_at"),
                    )
                ).alias("value"),
            )
            if not kafka_rows.isEmpty():
                (
                    kafka_rows.write.format("kafka")
                    .option("kafka.bootstrap.servers", bootstrap_servers)
                    .option("topic", topic)
                    .save()
                )
    finally:
        spark.stop()


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Build subscription SCD2 history and publish compacted current state"
    )
    parser.add_argument("--bronze", default="lake/bronze/billing_subscr")
    parser.add_argument("--silver", default="lake/silver/subscriptions")
    parser.add_argument("--churn", default="lake/gold/subscription_churn")
    parser.add_argument("--invalid", default="lake/bronze/dlq/subscriptions")
    parser.add_argument(
        "--bootstrap-servers",
        default=os.getenv("KAFKA_BOOTSTRAP_SERVERS"),
    )
    parser.add_argument("--topic", default="ott.ref.subscription_state")
    args = parser.parse_args()
    run_subscription_state(
        args.bronze, args.silver, args.churn, args.invalid,
        args.bootstrap_servers, args.topic,
    )


if __name__ == "__main__":
    main()
