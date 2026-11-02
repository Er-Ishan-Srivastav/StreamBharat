"""Q20 — Churn propensity model using pre-renewal engagement signals.

Training examples are subscription-cycle snapshots. The supplied billing data
shows monthly renewal cycles; the label horizon is therefore 30 days plus a
7-day grace period. A successful renewal is retained; cancellation, unrecovered
payment failure, or no renewal by the end of the grace period is churn.
Playback features are computed strictly before each snapshot to prevent leakage.
"""

from __future__ import annotations

import argparse
import json
import os
from datetime import date

from .spark_session import create_spark


FEATURE_COLUMNS = [
    "watch_minutes_28d",
    "watch_minutes_prev_28d",
    "active_days_28d",
    "active_days_prev_28d",
    "watch_decline_pct",
    "payment_failures_90d",
    "prior_renewals",
    "tenure_days",
    "plan_price_inr",
    "is_premium",
]


def _load_subscription_events(spark, subscription_path: str):
    from pyspark.sql import functions as F

    events = (
        spark.read.json(subscription_path)
        .withColumn("event_ts", F.to_timestamp("event_ts"))
        .filter(F.col("user_id").isNotNull() & F.col("event_ts").isNotNull())
        .dropDuplicates(["user_id", "event_type", "event_ts"])
    )
    return events


def _build_unrecovered_failures(subscriptions):
    """Return failure events that do not have a successful renewal within 7 days."""
    from pyspark.sql import functions as F

    failed = subscriptions.filter(F.col("event_type") == "payment_failed").alias("f")
    renewals = subscriptions.filter(F.col("event_type") == "renew").alias("r")
    recovered = (
        failed.join(
            renewals,
            (F.col("f.user_id") == F.col("r.user_id")) &
            (F.col("r.event_ts") > F.col("f.event_ts")) &
            (F.col("r.event_ts") <= F.col("f.event_ts") + F.expr("INTERVAL 7 DAYS")),
            "left",
        )
        .groupBy(F.col("f.user_id").alias("user_id"), F.col("f.event_ts").alias("failure_ts"))
        .agg(F.count(F.col("r.event_ts")).alias("recovery_count"))
    )
    return (
        recovered.filter(F.col("recovery_count") == 0)
        .select(
            "user_id",
            F.col("failure_ts").alias("terminal_ts"),
            F.lit(0).alias("priority"),
            F.lit("involuntary_failure").alias("terminal_type"),
        )
    )


def _make_training_anchors(spark, subscriptions, as_of: str,
                           training_start: str = "2026-07-01"):
    """Build mature monthly-cycle snapshots; 30-day cycle + 7-day grace label."""
    from pyspark.sql import functions as F

    as_of_ts = F.to_timestamp(F.lit(f"{as_of} 23:59:59"))
    anchors = (
        subscriptions.filter(F.col("event_type").isin("subscribe", "renew"))
        .filter(F.col("event_ts") >= F.to_timestamp(F.lit(training_start)))
        .filter(F.col("event_ts") <= as_of_ts - F.expr("INTERVAL 37 DAYS"))
        .select("user_id", F.col("event_ts").alias("anchor_ts"), "plan", "price")
        .withColumn("anchor_id", F.sha2(F.concat_ws("||", "user_id", F.col("anchor_ts").cast("string")), 256))
        .withColumn("anchor_date", F.to_date("anchor_ts"))
        .withColumn(
            "plan_price_inr",
            F.when(F.col("price") > 0, F.col("price").cast("double"))
             .when(F.col("plan") == "premium", F.lit(299.0))
             .otherwise(F.lit(149.0)),
        )
    )

    cancels = (
        subscriptions.filter(F.col("event_type") == "cancel")
        .select("user_id", F.col("event_ts").alias("terminal_ts"),
                F.lit(1).alias("priority"), F.lit("voluntary_cancel").alias("terminal_type"))
    )
    failures = _build_unrecovered_failures(subscriptions)
    renewals = (
        subscriptions.filter(F.col("event_type") == "renew")
        .select("user_id", F.col("event_ts").alias("terminal_ts"),
                F.lit(2).alias("priority"), F.lit("renewed").alias("terminal_type"))
    )
    terminals = cancels.unionByName(failures).unionByName(renewals)

    matched = (
        anchors.alias("a")
        .join(
            terminals.alias("t"),
            (F.col("a.user_id") == F.col("t.user_id")) &
            (F.col("t.terminal_ts") > F.col("a.anchor_ts")) &
            (F.col("t.terminal_ts") <= F.col("a.anchor_ts") + F.expr("INTERVAL 37 DAYS")),
            "left",
        )
        .filter(F.col("t.terminal_ts").isNotNull())
        .groupBy(F.col("a.anchor_id").alias("anchor_id"))
        .agg(
            F.min(F.struct(
                F.col("t.terminal_ts").alias("terminal_ts"),
                F.col("t.priority").alias("priority"),
                F.col("t.terminal_type").alias("terminal_type"),
            )).alias("first_terminal")
        )
        .select(
            "anchor_id",
            F.col("first_terminal.terminal_type").alias("terminal_type"),
        )
    )
    labelled = (
        anchors.join(matched, "anchor_id", "left")
        .withColumn(
            "terminal_type",
            F.coalesce(F.col("terminal_type"), F.lit("no_renewal_within_grace")),
        )
        .withColumn("label", F.when(F.col("terminal_type") == "renewed", 0.0).otherwise(1.0))
    )
    return labelled


def _load_daily_watch(spark, playback_path: str):
    """Build user/day watch minutes from cleaned playback; exclude offline uploads."""
    from pyspark.sql import functions as F

    playback = spark.read.format("delta").load(playback_path)
    available = set(playback.columns)
    timestamp_col = next(
        (name for name in ("event_ts_corrected", "event_ts", "server_ts") if name in available),
        None,
    )
    if timestamp_col is None:
        raise ValueError(
            "Playback table needs event_ts_corrected, event_ts or server_ts. "
            "Use the Q2 Silver output when available; Bronze server_ts is the fallback."
        )
    selected = playback.filter(F.col("event_type").isin("heartbeat", "complete"))
    if "download_id" in available:
        selected = selected.filter(F.col("download_id").isNull() | (F.trim(F.col("download_id")) == ""))
    return (
        selected.withColumn("watch_ts", F.to_timestamp(F.col(timestamp_col)))
        .filter(F.col("watch_ts").isNotNull())
        .withColumn("watch_date", F.to_date("watch_ts"))
        .groupBy("user_id", "watch_date")
        .agg((F.count(F.lit(1)) * F.lit(5.0)).alias("watch_minutes"))
    )


def _build_features(spark, anchors, subscriptions, daily_watch, users_path: str):
    """Attach 28/56/90-day history features to each anchor without future data."""
    from pyspark.sql import functions as F

    a = anchors.alias("a")
    w = daily_watch.alias("w")
    activity_join = a.join(
        w,
        (F.col("a.user_id") == F.col("w.user_id")) &
        (F.col("w.watch_date") >= F.date_sub(F.col("a.anchor_date"), 56)) &
        (F.col("w.watch_date") < F.col("a.anchor_date")),
        "left",
    )
    days_before = F.datediff(F.col("a.anchor_date"), F.col("w.watch_date"))
    activity = activity_join.groupBy(F.col("a.anchor_id").alias("anchor_id")).agg(
        F.sum(F.when(days_before.between(1, 28), F.col("w.watch_minutes")).otherwise(0.0)).alias("watch_minutes_28d"),
        F.sum(F.when(days_before.between(29, 56), F.col("w.watch_minutes")).otherwise(0.0)).alias("watch_minutes_prev_28d"),
        F.countDistinct(F.when(days_before.between(1, 28), F.col("w.watch_date"))).alias("active_days_28d"),
        F.countDistinct(F.when(days_before.between(29, 56), F.col("w.watch_date"))).alias("active_days_prev_28d"),
    )

    fail = subscriptions.filter(F.col("event_type") == "payment_failed").alias("f")
    a2 = anchors.alias("a")
    fail_history = (
        a2.join(
            fail,
            (F.col("a.user_id") == F.col("f.user_id")) &
            (F.col("f.event_ts") < F.col("a.anchor_ts")) &
            (F.col("f.event_ts") >= F.col("a.anchor_ts") - F.expr("INTERVAL 90 DAYS")),
            "left",
        )
        .groupBy(F.col("a.anchor_id").alias("anchor_id"))
        .agg(F.count(F.col("f.event_ts")).alias("payment_failures_90d"))
    )

    prior_renewal = subscriptions.filter(F.col("event_type") == "renew").alias("r")
    renewal_history = (
        anchors.alias("a").join(
            prior_renewal,
            (F.col("a.user_id") == F.col("r.user_id")) & (F.col("r.event_ts") < F.col("a.anchor_ts")),
            "left",
        )
        .groupBy(F.col("a.anchor_id").alias("anchor_id"))
        .agg(F.count(F.col("r.event_ts")).alias("prior_renewals"))
    )

    users = (
        spark.read.json(users_path)
        .select("user_id", F.to_timestamp("signup_ts").alias("signup_ts"))
        .dropDuplicates(["user_id"])
    )
    base = anchors.join(activity, "anchor_id", "left").join(fail_history, "anchor_id", "left") \
        .join(renewal_history, "anchor_id", "left").join(users, "user_id", "left")
    base = (
        base.withColumn("watch_minutes_28d", F.coalesce("watch_minutes_28d", F.lit(0.0)))
        .withColumn("watch_minutes_prev_28d", F.coalesce("watch_minutes_prev_28d", F.lit(0.0)))
        .withColumn("active_days_28d", F.coalesce("active_days_28d", F.lit(0)))
        .withColumn("active_days_prev_28d", F.coalesce("active_days_prev_28d", F.lit(0)))
        .withColumn("payment_failures_90d", F.coalesce("payment_failures_90d", F.lit(0)))
        .withColumn("prior_renewals", F.coalesce("prior_renewals", F.lit(0)))
        .withColumn("tenure_days", F.greatest(F.datediff(F.to_date("anchor_ts"), F.to_date("signup_ts")), F.lit(0)))
        .withColumn("is_premium", F.when(F.col("plan") == "premium", 1.0).otherwise(0.0))
        .withColumn(
            "watch_decline_pct",
            F.when(F.col("watch_minutes_prev_28d") > 0,
                   (F.col("watch_minutes_prev_28d") - F.col("watch_minutes_28d")) /
                   F.col("watch_minutes_prev_28d"))
             .otherwise(F.lit(0.0)),
        )
    )
    return base


def _fit_and_score(training, scoring, model_path: str, output_path: str,
                   metrics_path: str, top_n: int):
    from pyspark.ml import Pipeline
    from pyspark.ml.classification import LogisticRegression
    from pyspark.ml.evaluation import BinaryClassificationEvaluator
    from pyspark.ml.feature import Imputer, VectorAssembler
    from pyspark.ml.functions import vector_to_array
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    # Chronological holdout: later subscription cycles are never used for fitting.
    prepared = training.withColumn("anchor_epoch", F.col("anchor_ts").cast("long"))
    cutoff_values = prepared.approxQuantile("anchor_epoch", [0.8], 0.001)
    if not cutoff_values:
        raise ValueError("No labelled training snapshots were produced.")
    cutoff = cutoff_values[0]
    train = prepared.filter(F.col("anchor_epoch") <= cutoff)
    test = prepared.filter(F.col("anchor_epoch") > cutoff)

    counts = {int(r["label"]): int(r["count"]) for r in train.groupBy("label").count().collect()}
    if len(counts) < 2:
        raise ValueError(
            f"Training period contains only one churn class: {counts}. "
            "Check the selected as-of date and subscription source."
        )
    n0, n1 = counts.get(0, 0), counts.get(1, 0)

    def weighted(df):
        return df.withColumn(
            "class_weight",
            F.when(F.col("label") == 1.0, F.lit(float(n0) / max(n1, 1))).otherwise(F.lit(1.0)),
        )

    imputed = [f"{name}_imputed" for name in FEATURE_COLUMNS]
    imputer = Imputer(strategy="median", inputCols=FEATURE_COLUMNS, outputCols=imputed)
    assembler = VectorAssembler(inputCols=imputed, outputCol="features", handleInvalid="keep")
    classifier = LogisticRegression(
        featuresCol="features", labelCol="label", weightCol="class_weight",
        maxIter=60, regParam=0.05, elasticNetParam=0.0,
    )
    pipeline = Pipeline(stages=[imputer, assembler, classifier])
    evaluation_model = pipeline.fit(weighted(train))

    evaluation_metrics = {}
    test_count = test.count()
    if test_count > 0:
        predictions = evaluation_model.transform(weighted(test))
        distinct_test_labels = predictions.select("label").distinct().count()
        if distinct_test_labels == 2:
            for metric in ("areaUnderROC", "areaUnderPR"):
                evaluator = BinaryClassificationEvaluator(
                    labelCol="label", rawPredictionCol="rawPrediction", metricName=metric
                )
                evaluation_metrics[metric] = float(evaluator.evaluate(predictions))
        tp = predictions.filter((F.col("label") == 1.0) & (F.col("prediction") == 1.0)).count()
        fp = predictions.filter((F.col("label") == 0.0) & (F.col("prediction") == 1.0)).count()
        fn = predictions.filter((F.col("label") == 1.0) & (F.col("prediction") == 0.0)).count()
        evaluation_metrics.update({
            "test_rows": test_count,
            "precision_at_0_5": float(tp / (tp + fp)) if tp + fp else 0.0,
            "recall_at_0_5": float(tp / (tp + fn)) if tp + fn else 0.0,
        })
    else:
        evaluation_metrics["test_rows"] = 0
        evaluation_metrics["note"] = "No rows after the chronological cutoff; AUC not calculated."

    # Refit the final deployment model on every labelled snapshot.
    all_counts = {int(r["label"]): int(r["count"]) for r in training.groupBy("label").count().collect()}
    all_n0, all_n1 = all_counts.get(0, 0), all_counts.get(1, 0)
    full_weighted = training.withColumn(
        "class_weight",
        F.when(F.col("label") == 1.0, F.lit(float(all_n0) / max(all_n1, 1))).otherwise(F.lit(1.0)),
    )
    final_model = pipeline.fit(full_weighted)
    final_model.write().overwrite().save(model_path)

    scored = (
        final_model.transform(scoring)
        .withColumn("churn_probability", vector_to_array("probability")[1])
        .withColumn("risk_value_inr", F.col("churn_probability") * F.col("plan_price_inr"))
    )
    score_window = Window.orderBy(F.col("risk_value_inr").desc(), F.col("churn_probability").desc())
    scored = (
        scored.withColumn("risk_rank", F.row_number().over(score_window))
        .filter(F.col("risk_rank") <= top_n)
        .select("user_id", "anchor_ts", "plan", "plan_price_inr", "churn_probability",
                "risk_value_inr", "risk_rank", "watch_minutes_28d", "watch_minutes_prev_28d",
                "watch_decline_pct", "payment_failures_90d", "active_days_28d")
    )
    (scored.write.format("delta").mode("overwrite").save(output_path))

    lr_model = final_model.stages[-1]
    coefficients = {
        feature: float(coef)
        for feature, coef in zip(FEATURE_COLUMNS, lr_model.coefficients.toArray().tolist())
    }
    metrics = {
        "model": "Spark MLlib LogisticRegression",
        "label_definition": "Monthly cycle (30 days) plus 7-day grace: successful renewal by day 37=0; cancellation, unrecovered payment failure, or no renewal by day 37=1",
        "subscription_cycle_days": 30,
        "renewal_grace_days": 7,
        "training_start_date": "2026-07-01",
        "payment_failure_recovery_window_days": 7,
        "feature_lookback_days": 56,
        "matured_training_snapshots": int(training.count()),
        "training_class_counts": all_counts,
        "chronological_split_epoch_seconds": cutoff,
        "evaluation": evaluation_metrics,
        "logistic_regression_coefficients": coefficients,
        "scored_accounts": int(scored.count()),
        "ranking": "churn_probability × monthly plan price (INR)",
    }
    os.makedirs(os.path.dirname(os.path.abspath(metrics_path)), exist_ok=True)
    with open(metrics_path, "w", encoding="utf-8") as handle:
        json.dump(metrics, handle, indent=2)


def run_q20(playback_path: str, subscription_path: str, users_path: str,
            model_path: str, output_path: str, metrics_path: str,
            as_of: str = "2026-10-31", top_n: int = 50000) -> None:
    from pyspark.sql import Window
    from pyspark.sql import functions as F
    from delta.tables import DeltaTable

    spark = create_spark("StreamBharat-Q20-Churn-Modelling")
    if not DeltaTable.isDeltaTable(spark, playback_path):
        raise FileNotFoundError(
            f"Playback Delta not found: {playback_path}. Run Q4 first, or pass the Q2 Silver Delta path."
        )
    for required in (subscription_path, users_path):
        if not os.path.exists(required):
            raise FileNotFoundError(f"Required source file not found: {required}")

    subscriptions = _load_subscription_events(spark, subscription_path)
    training_anchors = _make_training_anchors(spark, subscriptions, as_of)
    daily_watch = _load_daily_watch(spark, playback_path)
    training = _build_features(spark, training_anchors, subscriptions, daily_watch, users_path)

    as_of_ts = F.to_timestamp(F.lit(f"{as_of} 23:59:59"))
    latest_window = Window.partitionBy("user_id").orderBy(F.col("event_ts").desc())
    latest_state = (
        subscriptions.filter(F.col("event_ts") <= as_of_ts)
        .withColumn("_rn", F.row_number().over(latest_window))
        .filter(F.col("_rn") == 1)
        .filter(F.col("event_type").isin("subscribe", "renew", "upgrade", "downgrade", "resume"))
        .select("user_id", F.col("event_ts").alias("anchor_ts"), "plan", "price")
        .withColumn("anchor_id", F.sha2(F.concat_ws("||", "user_id", F.col("anchor_ts").cast("string")), 256))
        .withColumn("anchor_date", F.to_date("anchor_ts"))
        .withColumn(
            "plan_price_inr",
            F.when(F.col("price") > 0, F.col("price").cast("double"))
             .when(F.col("plan") == "premium", F.lit(299.0)).otherwise(F.lit(149.0)),
        )
    )
    scoring = _build_features(spark, latest_state, subscriptions, daily_watch, users_path)
    scoring = scoring.filter(F.col("plan").isin("basic", "premium"))

    _fit_and_score(training, scoring, model_path, output_path, metrics_path, top_n)
    spark.stop()


def main() -> None:
    parser = argparse.ArgumentParser(description="Q20: Train churn model and rank active accounts")
    parser.add_argument("--playback", default="data/delta/bronze/playback",
                        help="Prefer Q2 Silver output when available; Bronze uses server_ts fallback")
    parser.add_argument("--subscriptions", default="data/raw/billing/subscription_events.json")
    parser.add_argument("--users", default="data/raw/app/users.json")
    parser.add_argument("--model", default="artifacts/models/q20_churn_logreg")
    parser.add_argument("--output", default="data/delta/gold/churn_scores")
    parser.add_argument("--metrics", default="artifacts/reports/q20_metrics.json")
    parser.add_argument("--as-of", default="2026-10-31")
    parser.add_argument("--top-n", type=int, default=50000)
    args = parser.parse_args()
    run_q20(args.playback, args.subscriptions, args.users, args.model,
            args.output, args.metrics, args.as_of, args.top_n)


if __name__ == "__main__":
    main()
