#!/usr/bin/env bash
set -euo pipefail

LOG_ROOT="${LIVE_DEMO_LOG_ROOT:-logs/live_demo}"
pids=()
names=()

for name in producer q17 q18; do
  pid_file="$LOG_ROOT/$name.pid"
  if [[ -f "$pid_file" ]]; then
    pid="$(cat "$pid_file")"
    if [[ "$pid" =~ ^[0-9]+$ ]] && kill -0 "$pid" 2>/dev/null; then
      echo "Stopping $name (PID $pid)..."
      kill -TERM "$pid" 2>/dev/null || true
      pids+=("$pid")
      names+=("$name")
    fi
  fi
done

if (( ${#pids[@]} == 0 )); then
  for name in producer q17 q18; do
    rm -f "$LOG_ROOT/$name.pid"
  done
  echo "No live demo processes were running."
  exit 0
fi

# Give Spark a chance to stop the query and commit its final checkpoint/batch.
for index in "${!pids[@]}"; do
  pid="${pids[$index]}"
  name="${names[$index]}"
  for attempt in $(seq 1 30); do
    if ! kill -0 "$pid" 2>/dev/null; then
      break
    fi
    sleep 1
  done
  if kill -0 "$pid" 2>/dev/null; then
    echo "WARN: $name did not stop within 30 seconds; sending SIGKILL." >&2
    kill -KILL "$pid" 2>/dev/null || true
  fi
done

for name in producer q17 q18; do
  rm -f "$LOG_ROOT/$name.pid"
done

echo "Refreshing PostgreSQL serving tables from the completed Delta outputs..."
python3 -m streambharat.serve_postgres
python3 scripts/bootstrap_metabase.py

echo
echo "Live demo stopped and available outputs served to PostgreSQL."
echo "Inspect Kafka topics ott.metrics.trending and ott.alerts.ad_pacing."
