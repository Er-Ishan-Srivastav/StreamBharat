#!/usr/bin/env python3
"""Execute every committed Metabase SQL question against the local warehouse."""
from __future__ import annotations

import os
import sys
from pathlib import Path

import psycopg2

sys.path.insert(0, str(Path(__file__).resolve().parent))
from bootstrap_metabase import parse_dashboard_spec  # noqa: E402


def verify_queries() -> int:
    connection = psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", os.getenv("PGHOST", "localhost")),
        port=int(os.getenv("POSTGRES_PORT", os.getenv("PGPORT", "5432"))),
        dbname=os.getenv("POSTGRES_DB", os.getenv("PGDATABASE", "ottdw")),
        user=os.getenv("POSTGRES_USER", os.getenv("PGUSER", "ott")),
        password=os.getenv("POSTGRES_PASSWORD", os.getenv("PGPASSWORD", "ott_local_dev")),
        connect_timeout=10,
    )
    dashboards = parse_dashboard_spec(
        Path("config/dashboard/streambharat-metabase-questions.md")
    )
    checked = 0
    failures = []
    try:
        with connection.cursor() as cursor:
            for dashboard in dashboards:
                for question in dashboard["questions"]:
                    try:
                        cursor.execute(question["sql"])
                        if cursor.description:
                            cursor.fetchall()
                        checked += 1
                        print(f"PASS  {dashboard['name']} / {question['name']}")
                    except Exception as exc:
                        connection.rollback()
                        failures.append(
                            f"{dashboard['name']} / {question['name']}: "
                            f"{type(exc).__name__}: {str(exc).splitlines()[0]}"
                        )
                        print(f"FAIL  {failures[-1]}", file=sys.stderr)
        if failures:
            raise RuntimeError(
                f"{len(failures)} of {checked + len(failures)} dashboard SQL queries failed"
            )
    finally:
        connection.close()

    print(f"All {checked} committed Metabase SQL questions executed successfully.")
    return checked


if __name__ == "__main__":
    try:
        verify_queries()
    except Exception as exc:
        print(f"Warehouse SQL verification failed: {exc}", file=sys.stderr)
        raise SystemExit(1)
