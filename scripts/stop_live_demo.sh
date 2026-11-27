#!/usr/bin/env bash
set -euo pipefail

LOG_ROOT="${LIVE_DEMO_LOG_ROOT:-logs/live_demo}"
stopped=0

for name in producer q17 q18; do
  pid_file="$LOG_ROOT/$name.pid"
  if [[ -f "$pid_file" ]]; then
    pid="$(cat "$pid_file")"
    if [[ "$pid" =~ ^[0-9]+$ ]] && kill -0 "$pid" 2>/dev/null; then
      echo "Stopping $name (PID $pid)..."
      kill -TERM "$pid" 2>/dev/null || true
      stopped=$((stopped + 1))
    fi
    rm -f "$pid_file"
  fi
done

sleep 2
if (( stopped == 0 )); then
  echo "No live demo processes were running."
  exit 0
fi

echo "Refreshing PostgreSQL serving tables from the completed Delta outputs..."
python -m streambharat.serve_postgres
python scripts/bootstrap_metabase.py

echo
echo "Live demo stopped and available outputs served to PostgreSQL."
echo "Inspect Kafka topics ott.metrics.trending and ott.alerts.ad_pacing."
