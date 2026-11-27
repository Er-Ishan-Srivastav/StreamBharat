#!/usr/bin/env bash
set -Eeuo pipefail

# Run inside the StreamBharat workspace container after start_local_stack.sh.
# This runs the new hybrid source files through the repository's real pipeline.
# It does not recreate the missing course dataset or overwrite data/raw.

export PYTHONPATH="${PYTHONPATH:-src}"
export KAFKA_BOOTSTRAP_SERVERS="${KAFKA_BOOTSTRAP_SERVERS:-kafka:29092}"
export KAFKA_BOOTSTRAP="${KAFKA_BOOTSTRAP:-$KAFKA_BOOTSTRAP_SERVERS}"
export SPARK_MASTER="${SPARK_MASTER:-spark://spark-master:7077}"
export SPARK_SESSION_TIMEZONE="${SPARK_SESSION_TIMEZONE:-UTC}"
export SPARK_SHUFFLE_PARTITIONS="${SPARK_SHUFFLE_PARTITIONS:-4}"

DATA_ROOT="data/hybrid/raw"
REPLAY_FROM=""
REPLAY_TO=""
REPLAY_SPEED="0"
SEED="42"
CHAOS="false"

while (($#)); do
  case "$1" in
    --data) DATA_ROOT="$2"; shift 2 ;;
    --from) REPLAY_FROM="$2"; shift 2 ;;
    --to) REPLAY_TO="$2"; shift 2 ;;
    --speed) REPLAY_SPEED="$2"; shift 2 ;;
    --seed) SEED="$2"; shift 2 ;;
    --chaos) CHAOS="true"; shift ;;
    -h|--help)
      cat <<'USAGE'
Usage: bash scripts/run_hybrid_pipeline.sh [options]

Options:
  --data PATH       Hybrid source root (default: data/hybrid/raw)
  --from YYYY-MM-DD First server-arrival day to replay (default: manifest start)
  --to YYYY-MM-DD   Last server-arrival day to replay (default: manifest end)
  --speed NUMBER    Replay speed multiplier; 0 means instant (default: 0)
  --seed NUMBER     Replay chaos seed (default: 42)
  --chaos           Enable replay-time duplicates, late and poison records
  -h, --help        Show this help

The generator's own offline uploads and event-clock offsets remain present
even when replay-time chaos is disabled. Run this only against the intended
local Kafka/Delta/PostgreSQL environment.
USAGE
      exit 0
      ;;
    *)
      echo "Unknown argument: $1" >&2
      exit 2
      ;;
  esac
done

if [[ ! -f "$DATA_ROOT/manifest.json" ]]; then
  echo "Hybrid dataset manifest not found: $DATA_ROOT/manifest.json" >&2
  echo "Generate the dataset and run its preflight first. See docs/hybrid-data-lab.md." >&2
  exit 2
fi

RUN_ID="$(date -u +%Y%m%dT%H%M%SZ)"
LOG_DIR="logs/hybrid_runs/$RUN_ID"
LOG_FILE="$LOG_DIR/pipeline.log"
CURRENT_STEP="initialization"
mkdir -p "$LOG_DIR"
exec > >(tee -a "$LOG_FILE") 2>&1

on_error() {
  local status="$?"
  trap - ERR
  echo
  echo "HYBRID PIPELINE FAILED"
  echo "  step: $CURRENT_STEP"
  echo "  exit_code: $status"
  echo "  log: $LOG_FILE"
  exit "$status"
}
trap on_error ERR

mapfile -t MANIFEST_VALUES < <(
  python3 - "$DATA_ROOT" <<'PY'
import json
import sys
from pathlib import Path

root = Path(sys.argv[1])
manifest = json.loads((root / "manifest.json").read_text(encoding="utf-8"))
period = manifest.get("playback_arrival_period") or {}
snapshots = sorted((root / "content").glob("catalog_*.json"))
if not snapshots:
    raise SystemExit("No content/catalog_*.json snapshot exists")
start = period.get("start_date")
end = period.get("end_date")
if not start or not end:
    raise SystemExit("Manifest does not contain a playback arrival period")
print(start)
print(end)
print(snapshots[0])
PY
)

MANIFEST_FROM="${MANIFEST_VALUES[0]}"
MANIFEST_TO="${MANIFEST_VALUES[1]}"
CATALOG_PATH="${MANIFEST_VALUES[2]}"
REPLAY_FROM="${REPLAY_FROM:-$MANIFEST_FROM}"
REPLAY_TO="${REPLAY_TO:-$MANIFEST_TO}"

if [[ "$REPLAY_FROM" > "$MANIFEST_FROM" || "$REPLAY_TO" < "$MANIFEST_TO" ]]; then
  echo "Replay date range must include the full manifest arrival period." >&2
  echo "Manifest: $MANIFEST_FROM through $MANIFEST_TO; requested: $REPLAY_FROM through $REPLAY_TO" >&2
  exit 2
fi

echo "StreamBharat Hybrid Pipeline"
echo "  run_id: $RUN_ID"
echo "  data_root: $DATA_ROOT"
echo "  catalogue: $CATALOG_PATH"
echo "  arrival_range: $REPLAY_FROM through $REPLAY_TO"
echo "  replay_speed: $REPLAY_SPEED"
echo "  replay_chaos: $CHAOS"
echo "  log: $LOG_FILE"

run_step() {
  CURRENT_STEP="$1"
  shift
  echo
  echo "[$CURRENT_STEP]"
  "$@"
}

run_step "validate hybrid source contracts, joins, row counts and checksums" \
  python3 scripts/preflight_hybrid_dataset.py \
    --data-root "$DATA_ROOT" \
    --report "$LOG_DIR/source_preflight.json"

run_step "clean playback source and estimate reliable device clock offsets (Q2)" \
  python3 -m streambharat.q2_playback_clean \
    --input "$DATA_ROOT/player" \
    --output "$LOG_DIR/q2_playback_clean" \
    --offset-output "$LOG_DIR/q2_device_clock_offsets" \
    --impact-output "$LOG_DIR/q2_daily_clock_impact" \
    --min-online-events 5

run_step "build catalogue dimensions and bridges (Q1)" \
  python3 -m streambharat.q1_catalog \
    --catalog "$CATALOG_PATH" \
    --genre-mapping "$DATA_ROOT/reference/genre_mapping.csv" \
    --output "$LOG_DIR/q1_catalog_delta"

if python3 - "$DATA_ROOT/manifest.json" <<'PY'
import json
import sys
from pathlib import Path

manifest = json.loads(Path(sys.argv[1]).read_text(encoding="utf-8"))
sys.exit(0 if "wikimedia_pageviews" in manifest.get("input_snapshots", {}) else 1)
PY
then
  run_step "join public Wikipedia interest to content IDs" \
    python3 scripts/build_public_interest_features.py \
      --public-catalog data/external/wikidata/catalog.json \
      --pageviews data/external/wikimedia/pageviews.jsonl \
      --hybrid-catalog "$CATALOG_PATH" \
      --output "$LOG_DIR/content_interest_features.csv"
else
  echo
  echo "[public-interest sidecar] Not found; skipping optional feature join."
  echo "Fetch it with scripts/fetch_wikidata_catalog.py and scripts/fetch_wikimedia_pageviews.py."
fi

REPLAY_ARGS=(
  --data "$DATA_ROOT"
  --from "$REPLAY_FROM"
  --to "$REPLAY_TO"
  --speed "$REPLAY_SPEED"
  --seed "$SEED"
  --bootstrap "$KAFKA_BOOTSTRAP"
)
if [[ "$CHAOS" != "true" ]]; then
  REPLAY_ARGS+=(--no-chaos)
fi
run_step "replay the seven hybrid event feeds to Kafka" \
  python3 producers/replay.py "${REPLAY_ARGS[@]}"

run_step "ingest Kafka topics into append-only Bronze Delta" \
  bash scripts/run_bronze_ingest.sh --starting-offsets earliest --available-now

for topic in player_events billing_subscr billing_settle app_search app_ratings ads_breaks ads_impressions; do
  run_step "validate and route $topic records to the DLQ" \
    bash scripts/run_dlq_ingest.sh --topics "$topic" --starting-offsets earliest --available-now
done

run_step "reconcile Kafka partition offsets with Bronze" \
  python3 -m streambharat.bronze_reconcile

run_step "build Silver playback and subscription state" \
  python3 -m streambharat.silver_player \
    --bronze lake/bronze/player_events \
    --silver lake/silver/player_events \
    --offsets lake/silver/device_clock_offsets

run_step "build subscription SCD Type 2 history and current state" \
  python3 -m streambharat.subscription_state --bootstrap-servers "$KAFKA_BOOTSTRAP"

run_step "build Gold engagement and viewing-session facts" \
  python3 -m streambharat.gold_analytics \
    --player lake/silver/player_events \
    --catalog "$CATALOG_PATH" \
    --gold-root lake/gold

run_step "build viewing sessions and content drop-off" \
  python3 -m streambharat.session_analytics \
    --player lake/silver/player_events \
    --catalog "$CATALOG_PATH"

run_step "reconcile ad breaks, impressions and campaign delivery" \
  python3 -m streambharat.ad_matching \
    --campaigns "$DATA_ROOT/ads/ad_campaigns.csv" \
    --catalog "$CATALOG_PATH"

run_step "reconcile subscription charges and calculate partner royalties" \
  python3 -m streambharat.finance_royalties \
    --subscription lake/bronze/billing_subscr \
    --settlement lake/bronze/billing_settle \
    --partners "$DATA_ROOT/reference/content_partners.csv" \
    --catalog-dir "$DATA_ROOT/content"

if [[ -d lake/bronze/dlq/_delta_log ]]; then
  run_step "aggregate DLQ metrics by source and reason" \
    python3 -m streambharat.dlq_metrics
else
  echo
  echo "[DLQ metrics] No invalid events were routed to the DLQ in this run."
  echo "Re-run with --chaos to exercise replay-time poison, duplicate and late-event handling."
fi

run_step "parse both CDN providers and calculate hourly egress" \
  python3 -m streambharat.q3_cdn \
    --akash "$DATA_ROOT/cdn/akashcdn_*.log" \
    --nimbus "$DATA_ROOT/cdn/nimbusedge_*.tsv" \
    --edge-map "$DATA_ROOT/reference/cdn_edges.csv" \
    --bronze "$LOG_DIR/q3_cdn_bronze" \
    --silver "$LOG_DIR/q3_cdn_hourly"

run_step "aggregate CDN egress, status quality and regional cost (Q3)" \
  python3 -m streambharat.cdn_analytics \
    --requests "$LOG_DIR/q3_cdn_bronze" \
    --rates "$DATA_ROOT/reference/cdn_rates.csv" \
    --rates-format csv \
    --output lake/gold/cdn_egress_daily

run_step "publish available Silver and Gold Delta outputs to PostgreSQL" \
  python3 -m streambharat.serve_postgres

run_step "verify source-to-Bronze counts and Delta/PostgreSQL outputs" \
  python3 scripts/verify_hybrid_outputs.py \
    --data-root "$DATA_ROOT" \
    --q1-root "$LOG_DIR/q1_catalog_delta" \
    --q2-root "$LOG_DIR" \
    --q3-bronze "$LOG_DIR/q3_cdn_bronze" \
    --q3-silver "$LOG_DIR/q3_cdn_hourly" \
    --report "$LOG_DIR/end-to-end-acceptance.json"

run_step "execute every committed Metabase SQL question against PostgreSQL" \
  python3 scripts/verify_warehouse_sql.py

run_step "provision Metabase questions and dashboards" \
  python3 scripts/bootstrap_metabase.py

run_step "verify Metabase database, questions and dashboards" \
  python3 scripts/verify_metabase.py

CURRENT_STEP="write hybrid run evidence"
python3 - "$DATA_ROOT" "$LOG_DIR" "$RUN_ID" "$REPLAY_FROM" "$REPLAY_TO" "$SEED" "$CHAOS" <<'PY'
import hashlib
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

data_root, log_dir, run_id, replay_from, replay_to, seed, chaos = sys.argv[1:]
root = Path(data_root)
manifest_bytes = (root / "manifest.json").read_bytes()
manifest = json.loads(manifest_bytes)
report = {
    "run_id": run_id,
    "status": "pipeline_commands_completed",
    "completed_at_utc": datetime.now(timezone.utc).isoformat(),
    "dataset_type": manifest.get("dataset_type"),
    "generator_version": manifest.get("generator_version"),
    "source_manifest_sha256": hashlib.sha256(manifest_bytes).hexdigest(),
    "source_root": str(root),
    "replay_arrival_range": {"from": replay_from, "to": replay_to},
    "replay_seed": int(seed),
    "replay_chaos_enabled": chaos == "true",
    "source_row_counts": manifest.get("row_counts", {}),
    "provenance": manifest.get("provenance", {}),
    "end_to_end_acceptance_report": str(Path(log_dir) / "end-to-end-acceptance.json"),
    "log_file": str(Path(log_dir) / "pipeline.log"),
    "acceptance_boundary": (
        "All invoked commands exited successfully. This is local pipeline evidence, "
        "not a production deployment or proof of original coursework dataset equivalence."
    ),
}
output = Path(log_dir) / "run-summary.json"
output.parent.mkdir(parents=True, exist_ok=True)
output.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
print(f"Run evidence written: {output}")
PY

trap - ERR
echo
echo "HYBRID PIPELINE COMMANDS COMPLETED."
echo "Run summary: $LOG_DIR/run-summary.json"
echo "Full log:    $LOG_FILE"
echo "Q1, Q2 and Q3 intermediate Delta outputs are isolated under $LOG_DIR."
