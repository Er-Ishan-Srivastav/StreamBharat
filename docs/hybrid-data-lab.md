# StreamBharat Hybrid Data Lab

**Purpose:** give a new contributor a reproducible, inspectable data-engineering workload without depending on the missing course archive.

This is a hybrid dataset, not a claim to have recovered the original coursework data:

- **Public source metadata:** real film descriptions from Wikidata, whose structured data is released under CC0.
- **Public external-interest signal:** daily English Wikipedia article pageviews from Wikimedia Analytics, also CC0.
- **Generated platform activity:** fictional users, profiles, playback events, subscriptions, gateway settlements, searches, ratings, advertising delivery, CDN requests, licences and business reference data.

The source facts and generated scenario facts remain distinguishable. Public interest is not OTT engagement. Synthetic playback is not observed customer behaviour.

## 1. Why these sources

### Wikidata — content metadata

Wikidata provides structured film metadata and permits reuse of its structured data under CC0. The project fetches a bounded, recent-release slice through the public SPARQL endpoint. The fetched snapshot is cached locally so later generation runs do not depend on the API being available.

Source and policy:
- https://www.wikidata.org/wiki/Wikidata:Database_download
- https://www.wikidata.org/wiki/Wikidata:Licensing
- https://query.wikidata.org/

### Wikimedia Analytics — external attention signal

The pageviews script fetches daily English Wikipedia article pageviews for titles linked to the catalogue. It uses the agent=user series to exclude identified bots and automated traffic. These records are a public-attention signal, not a viewing count for StreamBharat.

Source and API reference:
- https://doc.wikimedia.org/generated-data-platform/aqs/analytics-api/reference/page-views.html
- https://doc.wikimedia.org/generated-data-platform/aqs/analytics-api/examples/page-metrics.html
- https://doc.wikimedia.org/generated-data-platform/aqs/analytics-api/documentation/access-policy.html

The pageview API needs a descriptive User-Agent. The client sends requests sequentially and writes a dated snapshot with its retrieval metadata.

## 2. Data flow

~~~mermaid
flowchart TD
    WD["Wikidata SPARQL<br/>CC0 film metadata"] --> SNAP["External catalogue snapshot<br/>data/external/wikidata"]
    SNAP --> FACTORY["Hybrid dataset factory<br/>fixed seed + date range + scale"]
    FACTORY --> RAW["Source-shaped OTT feeds<br/>data/hybrid/raw"]
    WV["Wikimedia pageviews<br/>CC0 public-interest signal"] --> SIDE["External signal sidecar<br/>data/external/wikimedia"]
    RAW --> REPLAY["Kafka replay producer"]
    REPLAY --> KAFKA["Kafka topic contracts"]
    KAFKA --> BRONZE["Append-only Bronze + DLQ"]
    BRONZE --> SILVER["Silver conformance"]
    SILVER --> GOLD["Gold analytics"]
    GOLD --> WAREHOUSE["PostgreSQL serving"]
    WAREHOUSE --> BI["Metabase dashboards"]
~~~

The seven-feed Kafka replay path consumes the generated player, subscription, settlement, search, rating, ad-break and ad-impression feeds. The factory also produces catalogue, licence, CDN and reference assets for the batch analytics exercises. The Wikimedia sidecar is currently a separate public-data input; it is not silently inserted into OTT playback metrics.

## 3. Reproduce the inputs

Use Python 3.10 or 3.11, as required by the repository's Spark environment. These data preparation scripts use only the Python standard library.

From the repository root:

~~~bash
# 1) Fetch a local CC0 catalogue snapshot.
python3 scripts/fetch_wikidata_catalog.py --min-year 2018 --limit 300

# 2) Fetch 30 days of public article-interest signals for up to 20 titles.
python3 scripts/fetch_wikimedia_pageviews.py --days 30 --articles 20

# 3) Generate a seven-day, 500-user OTT activity dataset.
python3 scripts/generate_hybrid_dataset.py \
  --output data/hybrid/raw \
  --start-date 2026-11-01 \
  --days 7 \
  --users 500 \
  --catalog-size 200 \
  --seed 42
~~~

# 4) Join the public pageview signal to the generated content IDs.
python3 scripts/build_public_interest_features.py \
  --public-catalog data/external/wikidata/catalog.json \
  --pageviews data/external/wikimedia/pageviews.jsonl \
  --hybrid-catalog data/hybrid/raw/content/catalog_2026-11-01.json \
  --output data/external/wikimedia/content_interest_features.csv

# 5) Validate the generated source tree.
python3 scripts/preflight_hybrid_dataset.py \
  --data-root data/hybrid/raw \
  --report logs/hybrid_preflight/latest.json
~~~

The Wikidata and Wikimedia requests need internet access. After the catalogue and pageview snapshots have been fetched, generation is offline and deterministic for the same catalogue snapshot, date range, scale and seed.

To run another scale or experiment, select a new output directory rather than overwriting an existing run. The generator refuses to replace a non-empty directory unless --overwrite is supplied.

Example profiles:

| Profile | Users | Days | Catalogue titles | Purpose |
|---|---:|---:|---:|---|
| Quick iteration | 50 | 2 | 50 | Inspect generated rows and test data contracts |
| Integration | 500 | 7 | 200 | Exercise the seven-feed Kafka path |
| Load experiment | 2,500 | 30 | 300 | Observe throughput, lag, Spark partitions and serving load |

These are suggested workload settings, not claims about the size of the original course sample. Scale the run only after checking free disk and Docker memory.

## 4. Files produced

The generator writes to the requested output directory (default data/hybrid/raw/):

| Path | Format | Meaning |
|---|---|---|
| content/catalog_YYYY-MM-01.json | Nested JSON array | Monthly snapshot of public film metadata plus explicitly synthetic platform attributes |
| content/licences.csv | CSV | Synthetic regional availability periods; not legal licensing evidence |
| player/playback_YYYY-MM-DD.json | JSONL | Playback events partitioned by server-arrival date |
| app/users.json, app/profiles.json | JSONL | Fictional account and profile dimensions |
| app/ratings.json | JSONL | Generated profile-to-content ratings |
| billing/subscription_events.json | JSONL | Generated lifecycle events |
| billing/gateway_settlements.csv | CSV | Generated settlement outcomes |
| search/search_logs.json | JSONL | Generated search and click events |
| ads/ad_campaigns.csv | CSV | Generated campaign definitions |
| ads/ad_breaks.csv, ads/ad_impressions.csv | CSV | Generated ad opportunities and delivered impressions |
| cdn/akashcdn_YYYY-MM.log | Access log | Generated AkashCDN-style requests |
| cdn/nimbusedge_YYYY-MM.tsv | TSV | Generated NimbusEdge-style requests |
| reference/*.csv | CSV | Generated partners, CDN edges/rates, city/state, genre mapping, plans and an empty published-metric register |
| manifest.json | JSON | Configuration, provenance, period, row counts and SHA-256 for every source file |

The public catalogue snapshot is stored separately under data/external/wikidata/. The public-interest sidecar is stored under data/external/wikimedia/.

## 5. What is real, derived and generated

| Data / field | Provenance | What it can support |
|---|---|---|
| Film title, release date, genre, original language, runtime where available | public_external | Catalogue dimensions and content metadata |
| Wikidata QID, Wikipedia article title and source URL | public_external | Source traceability and joining the external pageview sidecar |
| StreamBharat content ID, partner, tier, age rating | synthetic_derived | Fictional OTT business scenario |
| Account/profile IDs and attributes | synthetic_generated | Join, cohort and profile analytics |
| Playback and session events | synthetic_generated | Event-time processing, watch time and completion logic |
| Subscription and settlement events | synthetic_generated | Lifecycle, reconciliation and MRR exercises |
| Search and ratings | synthetic_generated | Funnel and recommendation experiments |
| Ad and CDN requests | synthetic_generated | Fill, pacing, egress and quality exercises |
| Wikipedia pageviews | public_external | External-interest trend comparison only |

Public content metadata is not proof of licensing rights to stream that content. partner_id, tier, age rating and licence rows are scenario fields generated for engineering exercises. Cast data is left empty when it is not returned by the public source query; the factory does not invent performers or series episodes.

## 6. Data-engineering behaviours represented

The generated source set contains deliberate, inspectable behaviour:

- client_ts and server_ts differ by a stable per-device clock offset and network arrival delay.
- A small share of playback sessions are offline downloads uploaded one to three days later. Playback files are named by server-arrival date, not client event date.
- Playback emits a start, five-minute heartbeat events and a final stop/complete event.
- Subscription attempts include paid starts and renewals; a small share of renewals fail and most failed attempts have a later recovery.
- Settlement rows are separate from subscription events so reconciliation is an actual join, not a precomputed answer.
- Free-tier ad breaks can be unfilled; only filled breaks produce an impression row.
- Two CDN providers use different log formats and provider-specific edge identifiers.
- Every source file is local and immutable for a run once generated; the manifest hashes it.

This is a controlled simulation. Its event distributions are assumptions, not statistically validated estimates of a real OTT platform.

## 7. Inspect and validate a run

~~~bash
python3 -m json.tool data/hybrid/raw/manifest.json
~~~

Verify checksums from the repository root:

~~~python
import hashlib
import json
from pathlib import Path

root = Path("data/hybrid/raw")
manifest = json.loads((root / "manifest.json").read_text(encoding="utf-8"))
failed = []
for relative, expected in manifest["files_sha256"].items():
    actual = hashlib.sha256((root / relative).read_bytes()).hexdigest()
    if actual != expected:
        failed.append(relative)
print("checksum failures:", failed)
if failed:
    raise SystemExit(1)
print("all source file checksums match")
~~~

For the Kafka replay producer, use the custom source root explicitly:

~~~bash
python3 producers/replay.py \
  --data data/hybrid/raw \
  --from 2026-11-01 \
  --to 2026-11-10 \
  --speed 0 \
  --seed 42 \
  --no-chaos \
  --bootstrap kafka:29092
~~~

Set the replay date range to cover the playback arrival period shown in manifest.json, not just the business-event period. Delayed offline uploads can arrive after the last client event day. Run the replay only against a fresh or deliberately reset Kafka/checkpoint/Delta/PostgreSQL environment; persistent offsets and checkpoints make naive reruns unsafe.

The original 20-user scripts/generate_demo_data.py and scripts/run_synthetic_demo.sh remain unchanged as the compact integration acceptance fixture. They are useful for quick smoke tests; they are not the full data factory.

## 8. Run the complete local hybrid pipeline

Start the services from the repository root on the host:

~~~bash
bash scripts/start_local_stack.sh
~~~

Then use the same Docker workspace for source preparation and pipeline execution. This ensures the generator, PySpark, Delta and Kafka clients use the versions installed with the project:

~~~bash
docker compose -f docker-compose.kafka.yml exec workspace \
  bash scripts/prepare_hybrid_dataset.sh

docker compose -f docker-compose.kafka.yml exec workspace \
  bash scripts/run_hybrid_pipeline.sh
~~~

The runner reads the playback **arrival period** from `manifest.json`, runs the separate hybrid preflight, executes Q1 catalogue flattening, replays all seven event feeds, ingests Bronze, routes validation failures, builds Silver and Gold outputs, performs ad and revenue reconciliation, processes both CDN log formats, publishes available Delta products to PostgreSQL, and verifies the serving SQL and Metabase setup.

It writes a run-specific log and machine-readable evidence under `logs/hybrid_runs/<run-id>/`. The end-to-end acceptance report checks source-to-Bronze counts and required Delta/PostgreSQL outputs. A successful run is evidence for that local run and commit; it is not a production deployment or proof of equivalence to the missing coursework archive.

To exercise replay-time duplicates, late deliveries and poison records, add `--chaos`:

~~~bash
bash scripts/run_hybrid_pipeline.sh --chaos
~~~

Without `--chaos`, the generator's own device-clock offsets and offline uploads are still present. Replay-time chaos is deliberately separate so contributors can compare a clean transport run with an imperfect transport run. Use a clean, isolated Kafka/Delta/checkpoint environment for a first acceptance run. Reusing persistent Kafka topics and checkpoints is stateful; do not mistake a second replay for a fresh run.

Q1 and Q3 intermediate outputs are isolated under the run's evidence directory. Q3 reads all generated monthly AkashCDN and NimbusEdge log files using provider-specific parsing and the generated edge reference.

## 9. Current limits — do not overstate compatibility

- The original course dataset is unavailable and has not been recreated.
- The factory currently uses a public film catalogue; it does not invent TV series, seasons, episode relationships or cast lists.
- The generated source directory follows the documented source file families, but it does not claim exact original row counts or complete Q1–Q20 acceptance.
- reference/published_metrics.csv is deliberately empty. There are no authentic board-published values to reconcile against.
- The generated licence, partner, plan and campaign data are scenario data, not real commercial agreements.
- The pageview sidecar is not yet included in the Kafka topics, Bronze/Silver/Gold tables or Metabase dashboards.
- The existing Kafka demo acceptance evidence remains specific to its recorded small synthetic fixture. It does not automatically validate this new factory output.

The remaining engineering gates are to run this pipeline from a clean environment, inspect its acceptance report, close any source-to-Bronze or transformation gaps, and then perform explicit Q1–Q20 gap analysis. The public-interest feature file is currently an external analytical sidecar; it is not inserted into OTT playback metrics or Kafka topics. Keep each gate separately evidenced.
