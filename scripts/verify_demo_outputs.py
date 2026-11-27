#!/usr/bin/env python3
"""Fail-fast verification of the compact local demo's Delta and PostgreSQL outputs."""
from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

import psycopg2
from delta.tables import DeltaTable

from streambharat.spark_session import create_spark


DELTA_OUTPUTS = {
    "bronze_player_events": "lake/bronze/player_events",
    "bronze_subscriptions": "lake/bronze/billing_subscr",
    "bronze_settlements": "lake/bronze/billing_settle",
    "bronze_search": "lake/bronze/app_search",
    "bronze_ratings": "lake/bronze/app_ratings",
    "bronze_ad_breaks": "lake/bronze/ads_breaks",
    "bronze_ad_impressions": "lake/bronze/ads_impressions",
    "bronze_dlq": "lake/bronze/dlq",
    "bronze_offset_reconciliation": "lake/gold/bronze_offset_reconciliation",
    "silver_player_events": "lake/silver/player_events",
    "silver_subscriptions": "lake/silver/subscriptions",
    "gold_watch_time_daily": "lake/gold/watch_time_daily",
    "gold_active_users_daily": "lake/gold/active_users_daily",
    "gold_top_titles_daily": "lake/gold/top_titles_daily",
    "gold_viewing_sessions": "lake/gold/viewing_sessions",
    "gold_content_dropoff_daily": "lake/gold/content_dropoff_daily",
    "gold_ad_delivery": "lake/silver/ad_delivery",
    "gold_campaign_daily": "lake/gold/campaign_daily",
    "gold_ad_fill_rate_hourly": "lake/gold/ad_fill_rate_hourly",
    "gold_revenue_reconciliation": "lake/gold/revenue_reconciliation",
    "gold_royalties_quarterly": "lake/gold/royalties_quarterly",
    "gold_dlq_metrics_daily": "lake/gold/dlq_metrics_daily",
}

POSTGRES_OUTPUTS = (
    "watch_time_daily",
    "active_users_daily",
    "top_titles_daily",
    "subscriptions_scd2",
    "viewing_sessions",
    "content_dropoff_daily",
    "ad_delivery",
    "campaign_daily",
    "ad_fill_rate_hourly",
    "revenue_reconciliation",
    "royalties_quarterly",
    "dlq_metrics_daily",
)


def expected_source_rows(users: int) -> dict[str, int]:
    if users < 1 or users > 100:
        raise ValueError("users must be between 1 and 100")
    return {
        "bronze_player_events": users * 3 + 1,
        "bronze_subscriptions": users * 3,
        "bronze_settlements": users * 2,
        "bronze_search": users,
        "bronze_ratings": users,
        "bronze_ad_breaks": users,
        "bronze_ad_impressions": users,
        "bronze_dlq": 1,
    }


def write_report(path: str, payload: dict) -> None:
    report_path = Path(path)
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(
        json.dumps(payload, indent=2, sort_keys=True) + "\\n",
        encoding="utf-8",
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--users", type=int, default=5)
    parser.add_argument("--demo-date", default="2026-10-15")
    parser.add_argument(
        "--report-path",
        default="logs/synthetic_demo/latest_output_counts.json",
    )
    args = parser.parse_args()
    expected_rows = expected_source_rows(args.users)

    report = {
        "dataset_type": "synthetic_demo",
        "demo_date": args.demo_date,
        "users": args.users,
        "measured_at_utc": datetime.now(timezone.utc).isoformat(),
        "delta_counts": {},
        "postgres_counts": {},
        "status": "running",
        "failures": [],
    }
    failures = []
    spark = create_spark("StreamBharat-Demo-Verification")
    try:
        print("Delta output counts:")
        for name, path in DELTA_OUTPUTS.items():
            if not DeltaTable.isDeltaTable(spark, path):
                failures.append(f"Missing Delta table: {path}")
                report["delta_counts"][name] = None
                print(f"  FAIL  {name}: missing ({path})")
                continue
            count = spark.read.format("delta").load(path).count()
            report["delta_counts"][name] = count
            minimum = expected_rows.get(name, 1)
            passed = count >= minimum
            print(
                f"  {'PASS' if passed else 'FAIL'}  {name}: {count} "
                f"(minimum {minimum})"
            )
            if not passed:
                failures.append(
                    f"Delta table {path} has {count} rows; expected at least {minimum}"
                )
    finally:
        spark.stop()

    connection = psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "postgres"),
        port=int(os.getenv("POSTGRES_PORT", "5432")),
        dbname=os.getenv("POSTGRES_DB", "ottdw"),
        user=os.getenv("POSTGRES_USER", "ott"),
        password=os.getenv("POSTGRES_PASSWORD", "ott_local_dev"),
    )
    try:
        print("\\nPostgreSQL output counts:")
        with connection.cursor() as cursor:
            for table in POSTGRES_OUTPUTS:
                cursor.execute(f"SELECT COUNT(*) FROM ottdw.{table}")
                count = int(cursor.fetchone()[0])
                report["postgres_counts"][table] = count
                print(f"  {'PASS' if count > 0 else 'FAIL'}  ottdw.{table}: {count}")
                if count == 0:
                    failures.append(f"PostgreSQL table is empty: ottdw.{table}")
    finally:
        connection.close()

    report["failures"] = failures
    report["status"] = "failed" if failures else "passed"
    write_report(args.report_path, report)
    print(f"Machine-readable output counts: {args.report_path}")

    if failures:
        print("\\nDEMO VERIFICATION FAILED:")
        for failure in failures:
            print(f" - {failure}")
        sys.exit(1)
    print("\\nDEMO VERIFICATION PASSED: all required Delta and PostgreSQL outputs meet their minimum row counts.")


if __name__ == "__main__":
    main()
