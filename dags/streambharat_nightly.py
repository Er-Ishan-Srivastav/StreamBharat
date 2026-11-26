"""Date-aware orchestration for the StreamBharat Kafka ETL platform."""
from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

PROJECT_ROOT = os.getenv("STREAMBHARAT_PROJECT_ROOT", "/opt/streambharat")
PYTHON = os.getenv("STREAMBHARAT_PYTHON", "python")
BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
DATA_ROOT = os.getenv("STREAMBHARAT_DATA_ROOT", "/opt/streambharat/data/raw")
LOGICAL_DATE = '{{ dag_run.conf.get("date", ds) }}'
REPLAY_START = '{{ dag_run.conf.get("start_date", ds) }}'
REPLAY_END = '{{ dag_run.conf.get("end_date", ds) }}'

default_args = {
    "owner": "streambharat-data-engineering",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(hours=6),
    "sla": timedelta(hours=8),
}

with DAG(
    dag_id="streambharat_nightly",
    description=(
        "Master data, Kafka replay/Bronze/DLQ, player history, quality, "
        "engagement, ad delivery, finance and Postgres serving"
    ),
    start_date=datetime(2026, 10, 1),
    schedule="0 2 * * *",
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["streambharat", "kafka", "pyspark", "delta", "postgres"],
    doc_md="""
    ## Run configuration

    A scheduled run replays the source file day matching the Airflow logical date.
    A single-day rerun can be triggered with:

    JSON configuration: {"date": "2026-10-15", "start_date": "2026-10-15", "end_date": "2026-10-15"}

    A bounded source replay can use start_date/end_date. Downstream Gold rebuilds
    remain scoped to date (default: logical date). Trigger the separate historical
    backfill command for July–September; completed file days are skipped via the
    Delta manifest. The local development stack and dataset must be available first.

    The DAG fails on a FAIL-severity DQ gate. Warnings and quarantined rows remain
    visible in the Gold DQ outputs. SLA misses are surfaced by Airflow's task/SLA
    monitoring; configure an SMTP backend for email delivery outside local testing.
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

    master_data = BashOperator(
        task_id="load_master_and_reference_data",
        cwd=PROJECT_ROOT,
        bash_command=(
            f"cd {PROJECT_ROOT} && {PYTHON} -m streambharat.batch_master "
            f"--data-root {DATA_ROOT} --output-root lake/silver"
        ),
    )

    replay_sources = BashOperator(
        task_id="replay_seven_source_topics",
        cwd=PROJECT_ROOT,
        bash_command=(
            f"cd {PROJECT_ROOT} && {PYTHON} -m streambharat.replay_kafka "
            f"--data-root {DATA_ROOT} "
            f"--start-date '{REPLAY_START}' --end-date '{REPLAY_END}' "
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

    player_backfill = BashOperator(
        task_id="backfill_july_to_september_player_history",
        cwd=PROJECT_ROOT,
        bash_command=(
            f"cd {PROJECT_ROOT} && {PYTHON} -m streambharat.backfill_player_history "
            f"--data-root {DATA_ROOT} --start-date 2026-07-01 --end-date 2026-09-30"
        ),
    )

    subscription_state = BashOperator(
        task_id="build_subscription_scd2_and_compacted_state",
        cwd=PROJECT_ROOT,
        bash_command=(
            f"cd {PROJECT_ROOT} && {PYTHON} -m streambharat.subscription_state "
            f"--bootstrap-servers {BOOTSTRAP}"
        ),
    )

    engagement_gold = BashOperator(
        task_id="gold_watch_time_dau_mau_top_titles",
        cwd=PROJECT_ROOT,
        bash_command=(
            f"cd {PROJECT_ROOT} && {PYTHON} -m streambharat.gold_analytics "
            "--player lake/silver/player_events "
            "--catalog data/raw/content/catalog_2026-10-01.json "
            f"--gold-root lake/gold --date '{LOGICAL_DATE}'"
        ),
    )

    sessions_gold = BashOperator(
        task_id="build_viewing_sessions_and_episode_dropoff",
        cwd=PROJECT_ROOT,
        bash_command=(
            f"cd {PROJECT_ROOT} && {PYTHON} -m streambharat.session_analytics "
            "--player lake/silver/player_events "
            f"--date '{LOGICAL_DATE}'"
        ),
    )

    cdn_cost_gold = BashOperator(
        task_id="build_cdn_egress_and_regional_cost",
        cwd=PROJECT_ROOT,
        bash_command=(
            f"cd {PROJECT_ROOT} && {PYTHON} -m streambharat.cdn_analytics "
            "--requests lake/silver/cdn_requests "
            "--rates lake/silver/cdn_rates "
            "--output lake/gold/cdn_egress_daily"
        ),
    )

    ad_delivery_gold = BashOperator(
        task_id="match_ad_breaks_and_impressions",
        cwd=PROJECT_ROOT,
        bash_command=(
            f"cd {PROJECT_ROOT} && {PYTHON} -m streambharat.ad_matching "
            f"--date '{LOGICAL_DATE}'"
        ),
    )

    finance_gold = BashOperator(
        task_id="reconcile_revenue_and_calculate_partner_royalties",
        cwd=PROJECT_ROOT,
        bash_command=(
            f"cd {PROJECT_ROOT} && {PYTHON} -m streambharat.finance_royalties "
            "--subscription lake/bronze/billing_subscr "
            "--settlement lake/bronze/billing_settle "
            "--revenue-output lake/gold/revenue_reconciliation "
            "--watch-time lake/gold/watch_time_daily "
            "--royalty-output lake/gold/royalties_quarterly "
            "--partners data/raw/reference/content_partners.csv "
            "--catalog-dir data/raw/content"
        ),
    )

    quality_gate = BashOperator(
        task_id="run_dq_rules_quarantine_and_fail_gate",
        cwd=PROJECT_ROOT,
        bash_command=(
            f"cd {PROJECT_ROOT} && {PYTHON} -m streambharat.quality_job "
            "--player lake/silver/player_events "
            "--titles lake/silver/titles_history "
            "--profiles lake/silver/profiles "
            "--licences lake/silver/licences "
            f"--city-state {DATA_ROOT}/reference/city_state.csv "
            "--subscriptions lake/bronze/billing_subscr "
            "--plans lake/silver/plans "
            "--output lake/gold/dq_metrics "
            "--fail-on-fail"
        ),
    )

    serve_postgres = BashOperator(
        task_id="upsert_gold_and_silver_serving_tables_to_postgres",
        cwd=PROJECT_ROOT,
        bash_command=(
            f"cd {PROJECT_ROOT} && {PYTHON} -m streambharat.serve_postgres "
            f"--date '{LOGICAL_DATE}'"
        ),
    )

    preflight >> [create_topics, master_data]
    create_topics >> replay_sources >> bronze_ingestion
    bronze_ingestion >> [
        source_dlq, offset_reconciliation, silver_player, subscription_state
    ]
    [silver_player, master_data] >> player_backfill
    player_backfill >> [
        engagement_gold, sessions_gold, quality_gate
    ]
    master_data >> [ad_delivery_gold, cdn_cost_gold, quality_gate]
    bronze_ingestion >> ad_delivery_gold
    [engagement_gold, ad_delivery_gold, subscription_state] >> finance_gold
    [source_dlq, offset_reconciliation, quality_gate, sessions_gold, cdn_cost_gold, finance_gold] >> serve_postgres
