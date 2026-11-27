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
# Render DagRun configuration into process environment, never into shell source.
# Environment values are passed to BashOperator's subprocess without re-parsing
# their contents as shell syntax.
LOGICAL_DATE = '{{ dag_run.conf.get("date", ds) }}'
FULL_REFRESH = '{{ dag_run.conf.get("full_refresh", "false") | string | lower }}'
REPLAY_START = '{{ dag_run.conf.get("start_date", ds) }}'
REPLAY_END = '{{ dag_run.conf.get("end_date", ds) }}'
DATE_RUN_ENV = {
    "STREAMBHARAT_LOGICAL_DATE": LOGICAL_DATE,
    "STREAMBHARAT_FULL_REFRESH": FULL_REFRESH,
}
REPLAY_RUN_ENV = {
    "STREAMBHARAT_REPLAY_START": REPLAY_START,
    "STREAMBHARAT_REPLAY_END": REPLAY_END,
}
RUN_CONFIG_ENV = {**DATE_RUN_ENV, **REPLAY_RUN_ENV}

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
    For a daily rerun, pass {"date":"2026-10-15","start_date":"2026-10-15","end_date":"2026-10-15"}.

    For the initial historical load, pass full_refresh=true and a source replay
    range, for example {"full_refresh":true,"start_date":"2026-10-01","end_date":"2026-10-31"}.
    This rebuilds all engagement, session and ad-delivery Gold dates and loads
    all available Delta rows into Postgres. The July–September player backfill
    is also executed and tracked by its Delta file manifest.

    With full_refresh omitted/false, date-partitioned Gold and Postgres serving
    replace only the logical date. The subscription history, finance totals,
    quality checks and CDN monthly metrics are rebuilt from their available inputs.

    The DAG fails on FAIL-severity DQ findings. SLA misses are visible in Airflow;
    email notification requires an SMTP backend which is not configured by default.
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

    validate_run_configuration = BashOperator(
        task_id="validate_run_configuration",
        cwd=PROJECT_ROOT,
        bash_command=f"cd {PROJECT_ROOT} && {PYTHON} scripts/validate_run_config.py",
        env=RUN_CONFIG_ENV,
        append_env=True,
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
            f"cd {PROJECT_ROOT} && {PYTHON} producers/replay.py "
            f"--data {DATA_ROOT} "
            '--from "$STREAMBHARAT_REPLAY_START" --to "$STREAMBHARAT_REPLAY_END" '
            f"--bootstrap {BOOTSTRAP} --speed 0 --seed 42"
        ),
        env=REPLAY_RUN_ENV,
        append_env=True,
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
            f"cd {PROJECT_ROOT} && "
            'if [ "$STREAMBHARAT_FULL_REFRESH" = "true" ]; then '
            f"{PYTHON} -m streambharat.gold_analytics --player lake/silver/player_events --catalog data/raw/content/catalog_2026-10-01.json --gold-root lake/gold ; "
            f'else {PYTHON} -m streambharat.gold_analytics --player lake/silver/player_events --catalog data/raw/content/catalog_2026-10-01.json --gold-root lake/gold --date "$STREAMBHARAT_LOGICAL_DATE"; fi'
        ),
        env=DATE_RUN_ENV,
        append_env=True,
    )
    sessions_gold = BashOperator(
        task_id="build_viewing_sessions_and_episode_dropoff",
        cwd=PROJECT_ROOT,
        bash_command=(
            f"cd {PROJECT_ROOT} && "
            'if [ "$STREAMBHARAT_FULL_REFRESH" = "true" ]; then '
            f"{PYTHON} -m streambharat.session_analytics --player lake/silver/player_events ; "
            f'else {PYTHON} -m streambharat.session_analytics --player lake/silver/player_events --date "$STREAMBHARAT_LOGICAL_DATE"; fi'
        ),
        env=DATE_RUN_ENV,
        append_env=True,
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
            f"cd {PROJECT_ROOT} && "
            'if [ "$STREAMBHARAT_FULL_REFRESH" = "true" ]; then '
            f"{PYTHON} -m streambharat.ad_matching ; "
            f'else {PYTHON} -m streambharat.ad_matching --date "$STREAMBHARAT_LOGICAL_DATE"; fi'
        ),
        env=DATE_RUN_ENV,
        append_env=True,
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

    dlq_metrics = BashOperator(
        task_id="aggregate_dlq_records_by_source_and_reason",
        cwd=PROJECT_ROOT,
        bash_command=(
            f"cd {PROJECT_ROOT} && {PYTHON} -m streambharat.dlq_metrics "
            "--source lake/bronze/dlq "
            "--output lake/gold/dlq_metrics_daily"
        ),
    )

    published_metrics_reconciliation = BashOperator(
        task_id="reconcile_gold_metrics_to_published_course_board",
        cwd=PROJECT_ROOT,
        bash_command=(
            f"cd {PROJECT_ROOT} && {PYTHON} -m streambharat.published_metrics_reconciliation "
            "--published data/raw/reference/published_metrics.csv "
            "--aliases config/quality/published_metric_aliases.csv "
            "--gold-root lake/gold "
            "--output lake/gold/published_metrics_reconciliation "
            "--tolerance-pct 0.5"
        ),
    )

    serve_postgres = BashOperator(
        task_id="upsert_gold_and_silver_serving_tables_to_postgres",
        cwd=PROJECT_ROOT,
        bash_command=(
            f"cd {PROJECT_ROOT} && "
            'if [ "$STREAMBHARAT_FULL_REFRESH" = "true" ]; then '
            f"{PYTHON} -m streambharat.serve_postgres ; "
            f'else {PYTHON} -m streambharat.serve_postgres --date "$STREAMBHARAT_LOGICAL_DATE"; fi'
        ),
        env=DATE_RUN_ENV,
        append_env=True,
    )
    validate_run_configuration >> preflight
    preflight >> [create_topics, master_data]
    create_topics >> replay_sources >> bronze_ingestion
    bronze_ingestion >> [source_dlq, offset_reconciliation]
    source_dlq >> [silver_player, subscription_state, ad_delivery_gold, quality_gate]
    source_dlq >> dlq_metrics
    [silver_player, master_data] >> player_backfill
    player_backfill >> [
        engagement_gold, sessions_gold, quality_gate
    ]
    master_data >> [ad_delivery_gold, cdn_cost_gold, quality_gate]
    bronze_ingestion >> ad_delivery_gold
    [engagement_gold, ad_delivery_gold, subscription_state] >> finance_gold
    [
        engagement_gold, sessions_gold, ad_delivery_gold,
        finance_gold, cdn_cost_gold,
    ] >> published_metrics_reconciliation
    [
        source_dlq, dlq_metrics, offset_reconciliation, quality_gate, sessions_gold,
        cdn_cost_gold, finance_gold, published_metrics_reconciliation,
    ] >> serve_postgres
