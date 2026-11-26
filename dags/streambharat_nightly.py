"""Operational Kafka ETL orchestration for the current implemented pipeline slice.

The DAG can be triggered with dag_run.conf start_date/end_date to replay a range.
Scheduled runs default to the logical date for both endpoints.
"""
from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

PROJECT_ROOT = os.getenv("STREAMBHARAT_PROJECT_ROOT", "/opt/streambharat")
PYTHON = os.getenv("STREAMBHARAT_PYTHON", "python")
BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
DATA_ROOT = os.getenv("STREAMBHARAT_DATA_ROOT", "data/raw")

default_args = {
    "owner": "streambharat-data-engineering",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(hours=6),
}

with DAG(
    dag_id="streambharat_nightly",
    description=(
        "Replay source feeds, ingest Bronze/DLQ, reconcile, transform player Silver/Gold "
        "and serve available Gold tables to Postgres"
    ),
    start_date=datetime(2026, 10, 1),
    schedule="0 2 * * *",
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["streambharat", "kafka", "pyspark", "delta", "postgres"],
    doc_md="""
    ## Run modes

    - Scheduled run: processes the Airflow logical date (ds) only.
    - Manual backfill: trigger with JSON conf:
      {"start_date":"2026-10-01","end_date":"2026-10-31"}.

    The first run requires the course dataset under data/raw/. Bronze and DLQ
    use availableNow triggers and preserve their checkpoints. The current DAG
    operationalises the implemented source/player/engagement slice; subscription
    SCD2, revenue/royalties and the full dashboard layer remain separate work.
    """,
) as dag:

    preflight = BashOperator(
        task_id="validate_source_dataset",
        cwd=PROJECT_ROOT,
        bash_command=(
            f"cd {PROJECT_ROOT} && {PYTHON} scripts/preflight_dataset.py "
            f"--data-root {DATA_ROOT}"
        ),
    )

    create_topics = BashOperator(
        task_id="create_kafka_topics",
        cwd=PROJECT_ROOT,
        bash_command=(
            f"cd {PROJECT_ROOT} && {PYTHON} -m streambharat.create_topics "
            f"--bootstrap-servers {BOOTSTRAP} --replication-factor 1"
        ),
    )

    replay_sources = BashOperator(
        task_id="replay_seven_source_topics",
        cwd=PROJECT_ROOT,
        bash_command=(
            f"cd {PROJECT_ROOT} && {PYTHON} -m streambharat.replay_kafka "
            f"--data-root {DATA_ROOT} "
            "--start-date '{{ dag_run.conf.get(\"start_date\", ds) }}' "
            "--end-date '{{ dag_run.conf.get(\"end_date\", ds) }}' "
            f"--bootstrap-servers {BOOTSTRAP} --speed 0"
        ),
    )

    bronze_ingestion = BashOperator(
        task_id="bronze_ingestion_seven_topics",
        cwd=PROJECT_ROOT,
        bash_command=(
            f"cd {PROJECT_ROOT} && bash scripts/run_bronze_ingest.sh "
            "--starting-offsets earliest --available-now"
        ),
    )

    source_dlq = BashOperator(
        task_id="validate_source_events_and_write_dlq",
        cwd=PROJECT_ROOT,
        bash_command=(
            f"cd {PROJECT_ROOT} && bash scripts/run_dlq_ingest.sh "
            "--starting-offsets earliest --available-now"
        ),
    )

    offset_reconciliation = BashOperator(
        task_id="reconcile_bronze_offsets",
        cwd=PROJECT_ROOT,
        bash_command=(
            f"cd {PROJECT_ROOT} && {PYTHON} -m streambharat.bronze_reconcile "
            f"--bootstrap-servers {BOOTSTRAP}"
        ),
    )

    silver_player = BashOperator(
        task_id="silver_player_schema_and_clock_correction",
        cwd=PROJECT_ROOT,
        bash_command=(
            f"cd {PROJECT_ROOT} && {PYTHON} -m streambharat.silver_player "
            "--bronze lake/bronze/player_events "
            "--silver lake/silver/player_events "
            "--offsets lake/silver/device_clock_offsets"
        ),
    )

    gold_engagement = BashOperator(
        task_id="gold_watch_time_dau_mau_top_titles",
        cwd=PROJECT_ROOT,
        bash_command=(
            f"cd {PROJECT_ROOT} && {PYTHON} -m streambharat.gold_analytics "
            "--player lake/silver/player_events "
            "--catalog data/raw/content/catalog_2026-10-01.json "
            "--gold-root lake/gold"
        ),
    )

    serve_postgres = BashOperator(
        task_id="upsert_gold_tables_to_postgres",
        cwd=PROJECT_ROOT,
        bash_command=(
            f"cd {PROJECT_ROOT} && {PYTHON} -m streambharat.serve_postgres"
        ),
    )

    preflight >> create_topics >> replay_sources >> bronze_ingestion
    bronze_ingestion >> [source_dlq, silver_player]
    [source_dlq, silver_player] >> offset_reconciliation
    silver_player >> gold_engagement >> serve_postgres
    offset_reconciliation >> gold_engagement
