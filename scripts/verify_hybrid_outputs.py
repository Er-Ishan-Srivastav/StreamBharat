#!/usr/bin/env python3
"""Verify hybrid source-to-Bronze flow and the batch Delta/PostgreSQL products."""
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


BRONZE_TABLES = {
    "player_events": ("lake/bronze/player_events", "playback_events"),
    "billing_subscr": ("lake/bronze/billing_subscr", "subscriptions"),
    "billing_settle": ("lake/bronze/billing_settle", "settlements"),
    "app_search": ("lake/bronze/app_search", "search"),
    "app_ratings": ("lake/bronze/app_ratings", "ratings"),
    "ads_breaks": ("lake/bronze/ads_breaks", "ad_breaks"),
    "ads_impressions": ("lake/bronze/ads_impressions", "ad_impressions"),
}

REQUIRED_DELTA_OUTPUTS = {
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
    "gold_cdn_egress_daily": "lake/gold/cdn_egress_daily",
    "bronze_offset_reconciliation": "lake/gold/bronze_offset_reconciliation",
}

REQUIRED_NONEMPTY_DELTA = {
    "silver_player_events",
    "silver_subscriptions",
    "gold_watch_time_daily",
    "gold_active_users_daily",
    "gold_top_titles_daily",
    "gold_viewing_sessions",
    "gold_content_dropoff_daily",
    "gold_ad_delivery",
    "gold_campaign_daily",
    "gold_ad_fill_rate_hourly",
    "gold_revenue_reconciliation",
    "gold_royalties_quarterly",
    "gold_cdn_egress_daily",
    "bronze_offset_reconciliation",
}

POSTGRES_TABLES = (
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
    "cdn_egress_daily",
)


def verify(
    data_root: Path,
    q1_root: Path,
    q3_bronze: Path,
    q3_silver: Path,
) -> dict:
    manifest_path = data_root / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    expected = manifest.get("row_counts", {})
    report = {
        "status": "running",
        "checked_at_utc": datetime.now(timezone.utc).isoformat(),
        "source_manifest": str(manifest_path),
        "generator_version": manifest.get("generator_version"),
        "source_checks": {},
        "delta_counts": {},
        "postgres_counts": {},
        "failures": [],
    }
    failures: list[str] = []

    spark = create_spark("StreamBharat-Hybrid-Acceptance")
    try:
        for alias, (path, manifest_key) in BRONZE_TABLES.items():
            if not DeltaTable.isDeltaTable(spark, path):
                failures.append(f"Missing Bronze Delta table: {path}")
                report["source_checks"][alias] = {
                    "path": path,
                    "expected_source_rows": expected.get(manifest_key),
                    "bronze_rows": None,
                    "passed": False,
                }
                continue
            actual = spark.read.format("delta").load(path).count()
            minimum = int(expected.get(manifest_key, 0))
            passed = actual >= minimum and minimum > 0
            report["source_checks"][alias] = {
                "path": path,
                "expected_source_rows": minimum,
                "bronze_rows": actual,
                "passed": passed,
            }
            if not passed:
                failures.append(
                    f"{alias}: Bronze contains {actual} rows; source manifest requires at least {minimum}"
                )

        for name, path in REQUIRED_DELTA_OUTPUTS.items():
            if not DeltaTable.isDeltaTable(spark, path):
                failures.append(f"Missing required Delta output: {path}")
                report["delta_counts"][name] = None
                continue
            count = spark.read.format("delta").load(path).count()
            report["delta_counts"][name] = count
            if name in REQUIRED_NONEMPTY_DELTA and count <= 0:
                failures.append(f"Required Delta output is empty: {name} ({path})")

        q2_root = Path(args.q2_root)
        for name, path in {
            "q1_dim_content": q1_root / "dim_content",
            "q1_content_language": q1_root / "content_language",
            "q1_content_genre": q1_root / "content_genre",
            "q2_playback_clean": q2_root / "q2_playback_clean",
            "q2_device_clock_offsets": q2_root / "q2_device_clock_offsets",
            "q2_daily_clock_impact": q2_root / "q2_daily_clock_impact",
            "q3_cdn_requests": q3_bronze,
            "q3_cdn_hourly": q3_silver,
        }.items():
            if not DeltaTable.isDeltaTable(spark, str(path)):
                failures.append(f"Missing domain output: {path}")
                report["delta_counts"][name] = None
                continue
            count = spark.read.format("delta").load(str(path)).count()
            report["delta_counts"][name] = count
            if count <= 0:
                failures.append(f"Domain output is empty: {name}")

    finally:
        spark.stop()

    connection = psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", os.getenv("PGHOST", "localhost")),
        port=int(os.getenv("POSTGRES_PORT", os.getenv("PGPORT", "5432"))),
        dbname=os.getenv("POSTGRES_DB", os.getenv("PGDATABASE", "ottdw")),
        user=os.getenv("POSTGRES_USER", os.getenv("PGUSER", "ott")),
        password=os.getenv("POSTGRES_PASSWORD", os.getenv("PGPASSWORD", "ott_local_dev")),
        connect_timeout=10,
    )
    try:
        with connection.cursor() as cursor:
            for table in POSTGRES_TABLES:
                cursor.execute(f"SELECT COUNT(*) FROM ottdw.{table}")
                count = int(cursor.fetchone()[0])
                report["postgres_counts"][table] = count
                if count <= 0:
                    failures.append(f"Required PostgreSQL serving table is empty: ottdw.{table}")
    finally:
        connection.close()

    report["failures"] = failures
    report["status"] = "failed" if failures else "passed"
    return report


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data-root", default="data/hybrid/raw")
    parser.add_argument("--q1-root", required=True)
    parser.add_argument("--q2-root", required=True)
    parser.add_argument("--q3-bronze", required=True)
    parser.add_argument("--q3-silver", required=True)
    parser.add_argument("--report", required=True)
    args = parser.parse_args()

    report = verify(
        Path(args.data_root),
        Path(args.q1_root),
        Path(args.q3_bronze),
        Path(args.q3_silver),
    )
    report_path = Path(args.report)
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    print(f"HYBRID END-TO-END ACCEPTANCE: {report['status'].upper()}")
    for name, details in report["source_checks"].items():
        print(
            f"  {name}: source={details.get('expected_source_rows')} "
            f"bronze={details.get('bronze_rows')}"
        )
    for name, count in report["delta_counts"].items():
        print(f"  {name}: {count}")
    for table, count in report["postgres_counts"].items():
        print(f"  ottdw.{table}: {count}")
    for failure in report["failures"]:
        print(f"  ERROR: {failure}", file=sys.stderr)
    print(f"  report: {report_path}")
    return 0 if report["status"] == "passed" else 1


if __name__ == "__main__":
    raise SystemExit(main())
