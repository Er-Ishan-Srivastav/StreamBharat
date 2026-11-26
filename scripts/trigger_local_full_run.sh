#!/usr/bin/env bash
set -euo pipefail

START_DATE="${1:-2026-10-01}"
END_DATE="${2:-2026-10-31}"
FULL_REFRESH="${FULL_REFRESH:-true}"

date -d "$START_DATE" >/dev/null
date -d "$END_DATE" >/dev/null
if [[ "$END_DATE" < "$START_DATE" ]]; then
  echo "END_DATE must be on or after START_DATE" >&2
  exit 2
fi

CONF="$(python -c 'import json,os,sys; print(json.dumps({
    "full_refresh": os.environ["FULL_REFRESH"].lower() == "true",
    "start_date": os.environ["START_DATE"],
    "end_date": os.environ["END_DATE"],
}))' )"

export FULL_REFRESH START_DATE END_DATE
echo "Triggering StreamBharat full_refresh=$FULL_REFRESH for $START_DATE through $END_DATE"
docker compose -f docker-compose.airflow.yml exec -T airflow-scheduler \
  airflow dags trigger streambharat_nightly --conf "$CONF"
