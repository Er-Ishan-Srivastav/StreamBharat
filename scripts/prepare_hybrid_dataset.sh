#!/usr/bin/env bash
set -Eeuo pipefail

# Run inside the StreamBharat workspace container. The generated files are
# written to the bind-mounted repository data/ directory and remain git-ignored.

OUTPUT_ROOT="data/hybrid/raw"
START_DATE="$(date -u +%Y-%m-01)"
DAYS="7"
USERS="500"
CATALOG_SIZE="200"
MIN_YEAR="2018"
CATALOG_LIMIT="300"
PAGEVIEW_DAYS="30"
PAGEVIEW_ARTICLES="20"
SEED="42"
REUSE_CATALOG="false"
SKIP_PAGEVIEWS="false"
OVERWRITE="false"

while (($#)); do
  case "$1" in
    --output) OUTPUT_ROOT="$2"; shift 2 ;;
    --start-date) START_DATE="$2"; shift 2 ;;
    --days) DAYS="$2"; shift 2 ;;
    --users) USERS="$2"; shift 2 ;;
    --catalog-size) CATALOG_SIZE="$2"; shift 2 ;;
    --min-year) MIN_YEAR="$2"; shift 2 ;;
    --catalog-limit) CATALOG_LIMIT="$2"; shift 2 ;;
    --pageview-days) PAGEVIEW_DAYS="$2"; shift 2 ;;
    --pageview-articles) PAGEVIEW_ARTICLES="$2"; shift 2 ;;
    --seed) SEED="$2"; shift 2 ;;
    --reuse-catalog) REUSE_CATALOG="true"; shift ;;
    --skip-pageviews) SKIP_PAGEVIEWS="true"; shift ;;
    --overwrite) OVERWRITE="true"; shift ;;
    -h|--help)
      cat <<'USAGE'
Usage: bash scripts/prepare_hybrid_dataset.sh [options]

Options:
  --output PATH          Generated source root (default data/hybrid/raw)
  --start-date YYYY-MM-DD Business start date (default first day of current UTC month)
  --days N               Business period length (default 7)
  --users N              Synthetic user count (default 500)
  --catalog-size N       Number of public catalogue titles to use (default 200)
  --min-year YYYY        Earliest film release year in Wikidata (default 2018)
  --catalog-limit N      Maximum catalogue records fetched (default 300)
  --pageview-days N      External pageview lookback (default 30)
  --pageview-articles N  Number of articles for pageviews (default 20)
  --seed N               Deterministic scenario seed (default 42)
  --reuse-catalog        Reuse data/external/wikidata/catalog.json
  --skip-pageviews       Skip pageview acquisition and feature join
  --overwrite            Replace an existing non-empty hybrid output directory
  -h, --help             Show this help

The Wikidata catalogue is required. The Wikimedia pageview sidecar is optional.
All external and synthetic data are stored under git-ignored data/ paths.
USAGE
      exit 0
      ;;
    *)
      echo "Unknown argument: $1" >&2
      exit 2
      ;;
  esac
done

if [[ ! "$START_DATE" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ ]]; then
  echo "--start-date must use YYYY-MM-DD format." >&2
  exit 2
fi

mkdir -p data/external/wikidata data/external/wikimedia
CATALOG_FILE="data/external/wikidata/catalog.json"
PAGEVIEWS_FILE="data/external/wikimedia/pageviews.jsonl"

if [[ "$REUSE_CATALOG" == "true" ]]; then
  if [[ ! -f "$CATALOG_FILE" ]]; then
    echo "Requested cached catalogue does not exist: $CATALOG_FILE" >&2
    exit 2
  fi
  echo "Reusing cached Wikidata catalogue snapshot: $CATALOG_FILE"
else
  echo "[1/5] Fetching the current Wikidata CC0 film catalogue..."
  python3 scripts/fetch_wikidata_catalog.py \
    --min-year "$MIN_YEAR" \
    --limit "$CATALOG_LIMIT" \
    --output "$CATALOG_FILE"
fi

if [[ "$SKIP_PAGEVIEWS" != "true" ]]; then
  echo "[2/5] Fetching the optional Wikimedia pageview sidecar..."
  python3 scripts/fetch_wikimedia_pageviews.py \
    --catalog "$CATALOG_FILE" \
    --days "$PAGEVIEW_DAYS" \
    --articles "$PAGEVIEW_ARTICLES" \
    --output "$PAGEVIEWS_FILE"
else
  echo "[2/5] Skipping Wikimedia pageviews as requested."
fi

echo "[3/5] Generating the seeded hybrid OTT source dataset..."
GENERATOR_ARGS=(
  --catalog "$CATALOG_FILE"
  --output "$OUTPUT_ROOT"
  --start-date "$START_DATE"
  --days "$DAYS"
  --users "$USERS"
  --catalog-size "$CATALOG_SIZE"
  --seed "$SEED"
)
if [[ "$OVERWRITE" == "true" ]]; then
  GENERATOR_ARGS+=(--overwrite)
fi
python3 scripts/generate_hybrid_dataset.py "${GENERATOR_ARGS[@]}"

if [[ "$SKIP_PAGEVIEWS" != "true" ]]; then
  SNAPSHOT_DATE="${START_DATE:0:7}-01"
  CATALOG_SNAPSHOT="$OUTPUT_ROOT/content/catalog_$SNAPSHOT_DATE.json"
  echo "[4/5] Joining public article interest to generated content IDs..."
  python3 scripts/build_public_interest_features.py \
    --public-catalog "$CATALOG_FILE" \
    --pageviews "$PAGEVIEWS_FILE" \
    --hybrid-catalog "$CATALOG_SNAPSHOT" \
    --output data/external/wikimedia/content_interest_features.csv
else
  echo "[4/5] Skipping public-interest feature join."
fi

echo "[5/5] Running strict hybrid source preflight..."
python3 scripts/preflight_hybrid_dataset.py \
  --data-root "$OUTPUT_ROOT" \
  --report logs/hybrid_preflight/latest.json

echo
echo "HYBRID DATA PREPARATION COMPLETE"
echo "  source catalogue: $CATALOG_FILE"
echo "  generated sources: $OUTPUT_ROOT"
echo "  preflight report: logs/hybrid_preflight/latest.json"
if [[ "$SKIP_PAGEVIEWS" != "true" ]]; then
  echo "  public-interest features: data/external/wikimedia/content_interest_features.csv"
  echo "  feature manifest: data/external/wikimedia/content_interest_features.manifest.json"
fi
echo "Next: bash scripts/run_hybrid_pipeline.sh"
