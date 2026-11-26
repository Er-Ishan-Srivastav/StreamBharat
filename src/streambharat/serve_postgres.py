"""Transactional Postgres serving for validated Delta Gold tables.

Each DataFrame is written to a staging table, followed by one transaction using
INSERT ... ON CONFLICT. A failed stage load never truncates the production table.
"""
from __future__ import annotations

import argparse
import os
import re
import uuid

import psycopg2
from pyspark.sql import DataFrame, functions as F
from delta.tables import DeltaTable

from streambharat.spark_session import create_spark

TABLES = {
    "watch_time_daily": {
        "path": "lake/gold/watch_time_daily",
        "columns": ["viewing_date","profile_id","user_id","content_id","watched_seconds","completion_rate"],
        "keys": ["viewing_date","profile_id","content_id"],
        "date_column": "viewing_date",
    },
    "active_users_daily": {
        "path": "lake/gold/active_users_daily",
        "columns": ["activity_date","metric_type","active_users"],
        "keys": ["activity_date","metric_type"],
        "date_column": "activity_date",
    },
    "top_titles_daily": {
        "path": "lake/gold/top_titles_daily",
        "columns": ["activity_date","language","rank_no","content_id","title","viewers","watched_seconds"],
        "keys": ["activity_date","language","rank_no"],
        "date_column": "activity_date",
    },
    "campaign_daily": {
        "path": "lake/gold/campaign_daily",
        "columns": ["activity_date","campaign_id","impressions","spend_inr","daily_budget_inr","pacing_ratio","unfilled_breaks","filled_missing_impression","orphan_impressions","frequency_cap_breaches","genre_mismatch_count","alert_status"],
        "keys": ["activity_date","campaign_id"],
        "date_column": "activity_date",
    },
    "revenue_reconciliation": {
        "path": "lake/gold/revenue_reconciliation",
        "columns": ["charge_id","user_id","charge_amount","settlement_id","settlement_status","charge_ts","settlement_ts"],
        "keys": ["charge_id"],
        "date_columns": ["charge_ts", "settlement_ts"],
    },
    "royalties_quarterly": {
        "path": "lake/gold/royalties_quarterly",
        "columns": ["quarter_start","partner_id","revenue_share_inr","guarantee_topup_inr","payout_inr"],
        "keys": ["quarter_start","partner_id"],
    },
    "dq_metrics": {
        "path": "lake/gold/dq_metrics",
        "columns": ["batch_id","rule_name","severity","passed_rows","failed_rows"],
        "keys": ["batch_id","rule_name"],
    },
    "ad_pacing_alerts": {
        "path": "lake/gold/pacing_alerts_live",
        "columns": ["alert_id","alert_type","campaign_id","user_id","event_ts","spend_inr","pacing_ratio","details"],
        "keys": ["alert_id"],
        "json_columns": ["details"],
    },
    "subscriptions_scd2": {
        "path": "lake/silver/subscriptions",
        "columns": ["user_id","valid_from","plan","status","event_id","event_type","valid_to","is_current","churn_type","is_synthetic","kafka_timestamp","kafka_partition","kafka_offset"],
        "keys": ["user_id","valid_from"],
    },
    "subscription_churn": {
        "path": "lake/gold/subscription_churn",
        "columns": ["churn_type","users","refreshed_at"],
        "keys": ["churn_type"],
    },
    "viewing_sessions": {
        "path": "lake/gold/viewing_sessions",
        "columns": ["session_id","viewing_date","user_id","profile_id","device_id","content_id","report_content_id","title","language","session_start_ts","session_end_ts","session_duration_seconds","event_count","watched_seconds","max_position_sec","duration_sec","completion_rate","completed","dropoff_seconds"],
        "keys": ["session_id"],
        "date_column": "viewing_date",
    },
    "content_dropoff_daily": {
        "path": "lake/gold/content_dropoff_daily",
        "columns": ["viewing_date","report_content_id","language","sessions","viewers","watched_seconds","completed_sessions","average_completion_rate","average_dropoff_seconds"],
        "keys": ["viewing_date","report_content_id","language"],
        "date_column": "viewing_date",
    },
    "ad_delivery": {
        "path": "lake/silver/ad_delivery",
        "columns": ["delivery_id","break_id","impression_id","campaign_id","user_id","profile_id","content_id","break_filled","break_ts","ad_break_ts","duration_sec","completed","cpm_inr","delivery_status","genre_eligible","activity_date","spend_inr","break_partition","break_offset","impression_partition","impression_offset"],
        "keys": ["delivery_id"],
        "date_column": "activity_date",
    },
    "ad_fill_rate_hourly": {
        "path": "lake/gold/ad_fill_rate_hourly",
        "columns": ["hour_start","total_breaks","filled_breaks","unfilled_breaks","fill_rate"],
        "keys": ["hour_start"],
        "date_column": "hour_start",
    },
    "stream_metrics": {
        "path": "lake/gold/stream_metrics",
        "columns": ["metric_ts","query_name","topic","input_rows_per_second","processed_rows_per_second","batch_duration_ms","event_time_max","consumer_lag","freshness_seconds"],
        "keys": ["metric_ts","query_name","topic"],
        "date_column": "metric_ts",
    },
    "cdn_egress_daily": {
        "path": "lake/gold/cdn_egress_daily",
        "columns": ["activity_date","cdn","region","request_count","total_bytes","egress_gb","cost_inr","missing_rate_requests","http_4xx","http_5xx","unique_content"],
        "keys": ["activity_date","cdn","region"],
        "date_column": "activity_date",
    },
    "published_metrics_reconciliation": {
        "path": "lake/gold/published_metrics_reconciliation",
        "columns": ["month","metric","actual_metric","published_value","actual_value","variance_absolute","variance_pct","tolerance_pct","reconciliation_status","investigation_hint","published_ts","compared_at"],
        "keys": ["month","metric"],
        "date_column": "month",
    },
}


def upsert_dataframe(
    df: DataFrame,
    table_name: str,
    config: dict,
    jdbc_url: str,
    jdbc_properties: dict,
    pg_config: dict,
    date_arg: str | None = None,
) -> None:
    """Stage a Spark frame, then atomically replace one date or upsert all rows."""
    if not re.fullmatch(r"[a-z_]+", table_name):
        raise ValueError("Unsafe table name")
    stage = f"stg_{table_name}_{uuid.uuid4().hex[:10]}"
    selected = df.select(*config["columns"])
    (
        selected.write.format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", f"ottdw.{stage}")
        .option("user", jdbc_properties["user"])
        .option("password", jdbc_properties["password"])
        .option("driver", "org.postgresql.Driver")
        .option("batchsize", "5000")
        .mode("error")
        .save()
    )

    columns = config["columns"]
    keys = config["keys"]
    updates = [column for column in columns if column not in keys]
    insert_columns = ", ".join(columns)
    key_columns = ", ".join(keys)
    update_sql = ", ".join(
        f"{column}=EXCLUDED.{column}" for column in updates
    )
    select_columns = ", ".join(
        f"{column}::jsonb AS {column}"
        if column in config.get("json_columns", [])
        else column
        for column in columns
    )
    conflict_action = (
        f"DO UPDATE SET {update_sql}" if update_sql else "DO NOTHING"
    )
    insert_sql = (
        f"INSERT INTO ottdw.{table_name} ({insert_columns}) "
        f"SELECT {select_columns} FROM ottdw.{stage} "
        f"ON CONFLICT ({key_columns}) {conflict_action}"
    )

    date_columns = config.get("date_columns") or (
        [config["date_column"]] if config.get("date_column") else []
    )
    if date_arg and date_columns:
        if any(not re.fullmatch(r"[a-z_]+", column) for column in date_columns):
            raise ValueError("Unsafe date column")
        date_predicates = " OR ".join(
            f"DATE({column}) = %s" for column in date_columns
        )
        delete_sql = (
            f"DELETE FROM ottdw.{table_name} WHERE ({date_predicates})"
        )
        delete_params = tuple(date_arg for _ in date_columns)
    else:
        delete_sql = None
        delete_params = ()

    conn = psycopg2.connect(**pg_config)
    try:
        with conn:
            with conn.cursor() as cursor:
                if delete_sql:
                    cursor.execute(delete_sql, delete_params)
                cursor.execute(insert_sql)
                cursor.execute(f"DROP TABLE ottdw.{stage}")
    except Exception:
        # Stage creation is committed by Spark JDBC outside the transaction.
        # Clean it up without masking the original transaction exception.
        try:
            with conn:
                with conn.cursor() as cursor:
                    cursor.execute(f"DROP TABLE IF EXISTS ottdw.{stage}")
        except Exception:
            pass
        raise
    finally:
        conn.close()


def run_serving(date_arg: str | None = None):
    spark = create_spark(
        "StreamBharat-Postgres-Serving",
        extra_packages=["org.postgresql:postgresql:42.7.4"],
    )
    postgres_host = os.getenv("POSTGRES_HOST", "localhost")
    postgres_port = int(os.getenv("POSTGRES_PORT", "5432"))
    postgres_db = os.getenv("POSTGRES_DB", "ottdw")
    jdbc_url = os.getenv(
        "POSTGRES_JDBC_URL",
        f"jdbc:postgresql://{postgres_host}:{postgres_port}/{postgres_db}",
    )
    jdbc_properties = {
        "user": os.getenv("POSTGRES_USER", "ott"),
        "password": os.getenv("POSTGRES_PASSWORD", "ott_local_dev"),
    }
    pg_config = {
        "host": postgres_host,
        "port": postgres_port,
        "dbname": postgres_db,
        "user": jdbc_properties["user"],
        "password": jdbc_properties["password"],
    }
    try:
        for table, config in TABLES.items():
            path = config["path"]
            if not DeltaTable.isDeltaTable(spark, path):
                print(f"Skipping {table}; Delta path is not available: {path}")
                continue
            df = spark.read.format("delta").load(path)
            if date_arg:
                date_columns = config.get("date_columns") or (
                    [config["date_column"]] if config.get("date_column") else []
                )
                available_columns = [column for column in date_columns if column in df.columns]
                if available_columns:
                    date_condition = F.lit(False)
                    for date_column in available_columns:
                        date_condition = date_condition | (
                            F.to_date(F.col(date_column)) == F.to_date(F.lit(date_arg))
                        )
                    df = df.filter(date_condition)
            upsert_dataframe(
                df, table, config, jdbc_url, jdbc_properties, pg_config, date_arg
            )
    finally:
        spark.stop()


def main():
    p=argparse.ArgumentParser()
    p.add_argument("--date")
    a=p.parse_args()
    run_serving(a.date)

if __name__=="__main__":
    main()
