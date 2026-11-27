#!/usr/bin/env bash
set -euo pipefail

# Run this from the workspace container after start_local_stack.sh succeeds.
# Synthetic inputs live under data/demo so the supplied course dataset is not overwritten.
export PYTHONPATH="${PYTHONPATH:-/workspace/src}"
export KAFKA_BOOTSTRAP_SERVERS="${KAFKA_BOOTSTRAP_SERVERS:-kafka:29092}"
export KAFKA_BOOTSTRAP="${KAFKA_BOOTSTRAP:-$KAFKA_BOOTSTRAP_SERVERS}"
export SPARK_MASTER="${SPARK_MASTER:-spark://spark-master:7077}"
export SPARK_SESSION_TIMEZONE="${SPARK_SESSION_TIMEZONE:-UTC}"
export SPARK_SHUFFLE_PARTITIONS="${SPARK_SHUFFLE_PARTITIONS:-4}"

DEMO_DATE="${1:-2026-10-15}"
USERS="${2:-5}"
DEMO_DATA_ROOT="${DEMO_DATA_ROOT:-data/demo/raw}"
CATALOG_PATH="$DEMO_DATA_ROOT/content/catalog_${DEMO_DATE}.json"
LOG_DIR="${DEMO_LOG_DIR:-logs/synthetic_demo}"
RUN_ID="$(date -u +%Y%m%dT%H%M%SZ)"
LOG_FILE="$LOG_DIR/$RUN_ID.log"
CURRENT_STEP="initialization"

mkdir -p "$LOG_DIR"
exec > >(tee -a "$LOG_FILE") 2>&1

on_error() {
  local status="$?"
  trap - ERR
  {
    printf 'failed_at_utc=%s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
    printf 'failed_step=%s\n' "$CURRENT_STEP"
    printf 'exit_code=%s\n' "$status"
    printf 'log_file=%s\n' "$LOG_FILE"
  } > "$LOG_DIR/latest_failure.txt"
  echo
  echo "SYNTHETIC DEMO FAILED during: $CURRENT_STEP (exit $status)"
  echo "Diagnostic log: $LOG_FILE"
  exit "$status"
}
trap on_error ERR

if ! [[ "$DEMO_DATE" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ ]]; then
  echo "Demo date must use YYYY-MM-DD format." >&2
  exit 2
fi
if ! [[ "$USERS" =~ ^[0-9]+$ ]] || (( USERS < 1 || USERS > 100 )); then
  echo "User count must be an integer between 1 and 100." >&2
  exit 2
fi

echo "StreamBharat synthetic demo"
echo "  run_id: $RUN_ID"
echo "  date: $DEMO_DATE"
echo "  users: $USERS"
echo "  source_data: $DEMO_DATA_ROOT"
echo "  log: $LOG_FILE"

CURRENT_STEP="generate isolated synthetic source data"
echo "[1/14] Generate isolated synthetic OTT source feeds..."
python scripts/generate_demo_data.py --data-root "$DEMO_DATA_ROOT" --date "$DEMO_DATE" --users "$USERS"

CURRENT_STEP="replay seven synthetic source feeds to Kafka"
echo "[2/14] Replay the seven source feeds to Kafka..."
python producers/replay.py --data "$DEMO_DATA_ROOT" --from "$DEMO_DATE" --to "$DEMO_DATE" \
  --speed 0 --seed 42 --no-chaos --bootstrap "$KAFKA_BOOTSTRAP"

CURRENT_STEP="ingest seven Kafka topics into Bronze Delta"
echo "[3/14] Ingest Kafka source topics into Bronze Delta..."
bash scripts/run_bronze_ingest.sh --starting-offsets earliest --available-now

CURRENT_STEP="validate source events and write DLQ records"
echo "[4/14] Validate source events and write DLQ records..."
bash scripts/run_dlq_ingest.sh --starting-offsets earliest --available-now

CURRENT_STEP="reconcile retained Kafka offsets against Bronze"
echo "[5/14] Reconcile retained Kafka offsets against Bronze..."
python -m streambharat.bronze_reconcile

CURRENT_STEP="build Silver player events and subscription SCD2"
echo "[6/14] Build Silver player events and subscription SCD2..."
python -m streambharat.silver_player \
  --bronze lake/bronze/player_events \
  --silver lake/silver/player_events \
  --offsets lake/silver/device_clock_offsets
python -m streambharat.subscription_state --bootstrap-servers "$KAFKA_BOOTSTRAP"

CURRENT_STEP="build Gold engagement and viewing-session analytics"
echo "[7/14] Build Gold engagement and viewing-session analytics..."
python -m streambharat.gold_analytics \
  --player lake/silver/player_events \
  --catalog "$CATALOG_PATH" \
  --gold-root lake/gold
python -m streambharat.session_analytics \
  --player lake/silver/player_events \
  --catalog "$CATALOG_PATH"

CURRENT_STEP="build ad delivery, campaign pacing and fill-rate analytics"
echo "[8/14] Build ad delivery, campaign pacing and fill-rate analytics..."
python -m streambharat.ad_matching \
  --campaigns "$DEMO_DATA_ROOT/ads/ad_campaigns.csv" \
  --catalog "$CATALOG_PATH"

CURRENT_STEP="reconcile subscription charges and calculate partner royalties"
echo "[9/14] Reconcile subscription charges and calculate partner royalties..."
python -m streambharat.finance_royalties \
  --subscription lake/bronze/billing_subscr \
  --settlement lake/bronze/billing_settle \
  --partners "$DEMO_DATA_ROOT/reference/content_partners.csv" \
  --catalog-dir "$DEMO_DATA_ROOT/content"

CURRENT_STEP="aggregate DLQ records by source and reason"
echo "[10/14] Aggregate DLQ records by source and reason..."
python -m streambharat.dlq_metrics

CURRENT_STEP="publish Silver and Gold Delta tables to PostgreSQL"
echo "[11/14] Publish Silver/Gold Delta tables to PostgreSQL..."
python -m streambharat.serve_postgres

CURRENT_STEP="verify Delta and PostgreSQL output contracts"
echo "[12/14] Verify all seven source feeds and serving outputs..."
python scripts/verify_demo_outputs.py --users "$USERS" --demo-date "$DEMO_DATE" --report-path "$LOG_DIR/output_counts_$RUN_ID.json"
python scripts/verify_warehouse_sql.py

CURRENT_STEP="provision Metabase warehouse questions and dashboards"
echo "[13/14] Provision Metabase questions and dashboards..."
python scripts/bootstrap_metabase.py

CURRENT_STEP="verify Metabase database, dashboards and saved questions"
echo "[14/14] Verify Metabase database, dashboards and saved questions..."
python scripts/verify_metabase.py

trap - ERR
rm -f "$LOG_DIR/latest_failure.txt"
printf 'run_id=%s\ncompleted_at_utc=%s\ndemo_date=%s\nusers=%s\nlog_file=%s\n' \
  "$RUN_ID" "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$DEMO_DATE" "$USERS" "$LOG_FILE" \
  > "$LOG_DIR/latest_success.txt"

echo
echo "SYNTHETIC DEMO PIPELINE FINISHED AND VERIFIED."
echo "Inspect Delta paths under lake/bronze, lake/silver and lake/gold."
echo "Inspect PostgreSQL schema ottdw and open http://localhost:3000."
echo "Synthetic inputs were written to $DEMO_DATA_ROOT."
echo "Run logs: $LOG_FILE"
echo "The supplied course dataset under data/raw has NOT been modified or processed."
