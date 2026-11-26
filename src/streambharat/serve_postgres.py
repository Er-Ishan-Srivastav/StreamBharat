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
from pyspark.sql import DataFrame

from .spark_session import create_spark

TABLES = {
    "watch_time_daily": {
        "path": "lake/gold/watch_time_daily",
        "columns": ["viewing_date","profile_id","user_id","content_id","watched_seconds","completion_rate"],
        "keys": ["viewing_date","profile_id","content_id"],
    },
    "active_users_daily": {
        "path": "lake/gold/active_users_daily",
        "columns": ["activity_date","metric_type","active_users"],
        "keys": ["activity_date","metric_type"],
    },
    "top_titles_daily": {
        "path": "lake/gold/top_titles_daily",
        "columns": ["activity_date","language","rank_no","content_id","title","viewers","watched_seconds"],
        "keys": ["activity_date","language","rank_no"],
    },
    "campaign_daily": {
        "path": "lake/gold/campaign_daily",
        "columns": ["activity_date","campaign_id","impressions","spend_inr","daily_budget_inr","pacing_ratio","alert_status"],
        "keys": ["activity_date","campaign_id"],
    },
    "revenue_reconciliation": {
        "path": "lake/gold/revenue_reconciliation",
        "columns": ["charge_id","user_id","charge_amount","settlement_id","settlement_status","charge_ts","settlement_ts"],
        "keys": ["charge_id"],
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
}


def upsert_dataframe(df: DataFrame, table_name: str, config: dict, jdbc_url: str,
                     jdbc_properties: dict, pg_config: dict):
    if not re.fullmatch(r"[a-z_]+", table_name):
        raise ValueError("Unsafe table name")
    stage = f"stg_{table_name}_{uuid.uuid4().hex[:10]}"
    selected=df.select(*config["columns"])
    (selected.write.format("jdbc").option("url",jdbc_url)
     .option("dbtable",f"ottdw.{stage}").option("user",jdbc_properties["user"])
     .option("password",jdbc_properties["password"]).option("driver","org.postgresql.Driver")
     .option("batchsize","5000").mode("error").save())

    keys=set(config["keys"])
    cols=config["columns"]
    updates=[c for c in cols if c not in keys]
    insert_cols=", ".join(cols)
    key_cols=", ".join(config["keys"])
    update_sql=", ".join(f"{c}=EXCLUDED.{c}" for c in updates)
    sql=(
        f"INSERT INTO ottdw.{table_name} ({insert_cols}) "
        f"SELECT {insert_cols} FROM ottdw.{stage} "
        f"ON CONFLICT ({key_cols}) DO UPDATE SET {update_sql}; "
        f"DROP TABLE ottdw.{stage};"
    )
    conn=psycopg2.connect(**pg_config)
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(sql)
    except Exception:
        with conn:
            with conn.cursor() as cur:
                cur.execute(f"DROP TABLE IF EXISTS ottdw.{stage}")
        raise
    finally:
        conn.close()


def run_serving(date_arg: str | None = None):
    spark=create_spark("StreamBharat-Postgres-Serving")
    jdbc_url=os.getenv("POSTGRES_JDBC_URL","jdbc:postgresql://localhost:5432/ottdw")
    jdbc_properties={"user":os.getenv("POSTGRES_USER","ott"),
                     "password":os.environ["POSTGRES_PASSWORD"]}
    pg_config={"host":os.getenv("POSTGRES_HOST","localhost"),
               "port":int(os.getenv("POSTGRES_PORT","5432")),
               "dbname":os.getenv("POSTGRES_DB","ottdw"),
               "user":jdbc_properties["user"],"password":jdbc_properties["password"]}
    try:
        for table,config in TABLES.items():
            path=config["path"]
            df=spark.read.format("delta").load(path)
            if date_arg and "viewing_date" in df.columns:
                df=df.filter(f"viewing_date = DATE '{date_arg}'")
            upsert_dataframe(df,table,config,jdbc_url,jdbc_properties,pg_config)
    finally:
        spark.stop()


def main():
    p=argparse.ArgumentParser()
    p.add_argument("--date")
    a=p.parse_args()
    run_serving(a.date)

if __name__=="__main__":
    main()
