#!/usr/bin/env bash
set -euo pipefail

# Run inside the workspace container after run_synthetic_demo.sh has completed.
DEMO_DATE="${1:-2026-10-15}"
DURATION_SECONDS="${2:-120}"
STREAMS="${3:-both}"
DEMO_DATA_ROOT="${DEMO_DATA_ROOT:-data/demo/raw}"
CATALOG_PATH="$DEMO_DATA_ROOT/content/catalog_${DEMO_DATE}.json"
CAMPAIGNS_PATH="$DEMO_DATA_ROOT/ads/ad_campaigns.csv"
LOG_ROOT="${LIVE_DEMO_LOG_ROOT:-logs/live_demo}"

case "$STREAMS" in
  plays|ads|both) ;;
  *) echo "Usage: $0 [YYYY-MM-DD] [seconds] [plays|ads|both]" >&2; exit 2 ;;
esac
if [[ ! -f "$CATALOG_PATH" || ! -f "$CAMPAIGNS_PATH" ]]; then
  echo "Synthetic demo inputs not found. Run scripts/run_synthetic_demo.sh first." >&2
  exit 1
fi
if ! [[ "$DURATION_SECONDS" =~ ^[0-9]+$ ]] || (( DURATION_SECONDS < 15 )); then
  echo "Duration must be an integer of at least 15 seconds." >&2
  exit 2
fi

mkdir -p "$LOG_ROOT"
for name in q17 q18 producer; do
  pid_file="$LOG_ROOT/$name.pid"
  if [[ -f "$pid_file" ]] && kill -0 "$(cat "$pid_file")" 2>/dev/null; then
    echo "A live demo $name process is already running (PID $(cat "$pid_file"))." >&2
    exit 1
  fi
  rm -f "$pid_file"
done

export PYTHONPATH="${PYTHONPATH:-/workspace/src}"
export KAFKA_BOOTSTRAP="${KAFKA_BOOTSTRAP:-kafka:29092}"
export KAFKA_BOOTSTRAP_SERVERS="${KAFKA_BOOTSTRAP_SERVERS:-$KAFKA_BOOTSTRAP}"
export SPARK_MASTER="${SPARK_MASTER:-spark://spark-master:7077}"

if [[ "$STREAMS" == "plays" || "$STREAMS" == "both" ]]; then
  echo "Starting Q17 real-time trending consumer..."
  nohup bash scripts/run_kafka_stream.sh plays \
    --catalog "$CATALOG_PATH" \
    --as-of "$DEMO_DATE" \
    --bronze lake/silver/player_events \
    --baseline lake/gold/trending_baseline \
    --output lake/gold/trending \
    --checkpoint lake/_checkpoints/kafka_q17_demo \
    --starting-offsets latest \
    --min-viewers 1 --top-n 5 \
    >"$LOG_ROOT/q17.log" 2>&1 &
  echo "$!" > "$LOG_ROOT/q17.pid"
fi

if [[ "$STREAMS" == "ads" || "$STREAMS" == "both" ]]; then
  MATCH_START="$(python3 -c 'from datetime import datetime; from zoneinfo import ZoneInfo; print(datetime.now(ZoneInfo("Asia/Kolkata")).replace(tzinfo=None).isoformat(sep=" "))')"
  echo "Starting Q18 live ad-pacing consumer..."
  nohup bash scripts/run_kafka_stream.sh ads \
    --campaigns "$CAMPAIGNS_PATH" \
    --match-start "$MATCH_START" \
    --match-hours 24 \
    --checkpoint lake/_checkpoints/kafka_q18_demo \
    --starting-offsets latest \
    >"$LOG_ROOT/q18.log" 2>&1 &
  echo "$!" > "$LOG_ROOT/q18.pid"
fi

echo "Allowing Spark streaming consumers to initialize..."
sleep 10

echo "Starting live producer for $DURATION_SECONDS seconds..."
nohup python producers/live.py \
  --stream "$STREAMS" --seconds "$DURATION_SECONDS" --seed 9 \
  --catalog "$CATALOG_PATH" --campaigns "$CAMPAIGNS_PATH" \
  >"$LOG_ROOT/producer.log" 2>&1 &
echo "$!" > "$LOG_ROOT/producer.pid"

echo
echo "Live Q17/Q18 demo started. Logs: $LOG_ROOT"
echo "Q17 output topic: ott.metrics.trending"
echo "Q18 output topic: ott.alerts.ad_pacing"
echo "Run 'bash scripts/stop_live_demo.sh' when finished."
