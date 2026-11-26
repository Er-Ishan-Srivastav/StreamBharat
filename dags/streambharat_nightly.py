"""Nightly orchestration for StreamBharat batch and serving workloads.

This DAG calls repository CLI modules inside the Airflow worker/container.
Set STREAMBHARAT_PROJECT_ROOT and ensure the Airflow container shares the repo,
dataset, Delta lake and Python environment.
"""
from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import ShortCircuitOperator

PROJECT_ROOT = os.getenv("STREAMBHARAT_PROJECT_ROOT", "/opt/streambharat")
PYTHON = os.getenv("STREAMBHARAT_PYTHON", "python")

def _quality_gate() -> bool:
    """Fail closed when the quality/reconciliation report signals blockers."""
    from pathlib import Path
    import json

    report = Path(PROJECT_ROOT) / "artifacts/reports/quality_gate.json"
    if not report.exists():
        raise FileNotFoundError(f"Quality gate report missing: {report}")
    result = json.loads(report.read_text(encoding="utf-8"))
    if not result.get("passed", False):
        raise ValueError(f"Quality/reconciliation gate failed: {result}")
    return True

default_args = {
    "owner": "streambharat-data-engineering",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(minutes=90),
}

with DAG(
    dag_id="streambharat_nightly",
    description="StreamBharat master data, backfill, quality, gold analytics and Postgres serving",
    start_date=datetime(2026, 10, 1),
    schedule="0 2 * * *",
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["streambharat", "pyspark", "delta", "warehouse"],
) as dag:

    master_data = BashOperator(
        task_id="master_data",
        cwd=PROJECT_ROOT,
        bash_command=(
            f"cd {PROJECT_ROOT} && {PYTHON} -m streambharat.q1_catalog "
            f"--as-of '{{{{ ds }}}}'"
        ),
    )

    player_backfill_check = BashOperator(
        task_id="player_backfill_check",
        cwd=PROJECT_ROOT,
        bash_command=(
            f"cd {PROJECT_ROOT} && {PYTHON} -m streambharat.q4_incremental_ingestion "
            f"--date '{{{{ ds }}}}'"
        ),
    )

    quality = BashOperator(
        task_id="quality_and_reconciliation",
        cwd=PROJECT_ROOT,
        bash_command=(
            f"cd {PROJECT_ROOT} && {PYTHON} -m streambharat.q19_quality "
            f"--date '{{{{ ds }}}}'"
        ),
    )

    quality_gate = ShortCircuitOperator(
        task_id="quality_gate",
        python_callable=_quality_gate,
    )

    gold_engagement = BashOperator(
        task_id="gold_watch_time_and_active_users",
        cwd=PROJECT_ROOT,
        bash_command=(
            f"cd {PROJECT_ROOT} && {PYTHON} -m streambharat.q5_sessions "
            f"--date '{{{{ ds }}}}'"
        ),
    )

    gold_revenue = BashOperator(
        task_id="gold_revenue_and_royalties",
        cwd=PROJECT_ROOT,
        bash_command=(
            f"cd {PROJECT_ROOT} && {PYTHON} -m streambharat.q6_subscriptions "
            f"--date '{{{{ ds }}}}'"
        ),
    )

    serve_postgres = BashOperator(
        task_id="serve_postgres",
        cwd=PROJECT_ROOT,
        bash_command=(
            f"cd {PROJECT_ROOT} && {PYTHON} -m streambharat.serve_postgres "
            f"--date '{{{{ ds }}}}'"
        ),
    )

    master_data >> player_backfill_check >> quality >> quality_gate
    quality_gate >> [gold_engagement, gold_revenue] >> serve_postgres
