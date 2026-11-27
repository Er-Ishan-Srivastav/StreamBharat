# StreamBharat — Hybrid Dataset Decision and Delivery Plan

**Status:** Hybrid source tooling implemented on the feature branch; runtime acceptance is still required.  
**Branch:** feature/hybrid-dataset-factory  
**Decision:** The original course dataset is unavailable and will not be a project dependency. Use public, open content metadata plus a reproducible synthetic OTT activity factory.

## 1. Project objective

Make StreamBharat independently reproducible by a new contributor. The repository must teach the actual data-engineering lifecycle: acquire data, preserve provenance, define source contracts, generate or ingest events, validate, replay, land Bronze, conform Silver, compute Gold, reconcile, serve and explain the outputs.

The missing course archive is no longer a blocker. It is also not recreated or claimed to be present. The existing Q1–Q20 modules and compact Kafka demo remain part of the repository, but their implementation status must be kept separate from end-to-end acceptance on a newly generated hybrid dataset.

## 2. Provenance contract

Every source dataset must have one explicit top-level provenance class:

| Class | Meaning | Examples |
|---|---|---|
| course_original | Original file distributed by the course | Not currently available; optional only if later recovered |
| public_external | Fetched from an identified public source, with retrieval date and terms | Wikidata film metadata; Wikimedia article pageviews |
| synthetic_derived | Scenario attributes mapped or assigned to a public source record | StreamBharat content IDs, tier, partner, age rating, duration fallback |
| synthetic_generated | Data created by the StreamBharat factory | Users, profiles, playback, subscriptions, settlements, search, ratings, ads, CDN |

Preserve provenance at source/dataset level and, where practical, in records. Do not describe synthetic events as real customer activity. Do not treat public article pageviews as OTT viewing. Do not infer streaming rights from the existence of a public content record.

## 3. Public data choices

### 3.1 Wikidata — content catalogue

Use the Wikidata Query Service to fetch a bounded snapshot of film metadata. The fields used by the current adapter are the Wikidata identifier, title, release date, genres, original languages, runtime when available, and English Wikipedia article title.

Wikidata's structured data is released under CC0. See:
- https://www.wikidata.org/wiki/Wikidata:Database_download
- https://www.wikidata.org/wiki/Wikidata:Licensing
- https://query.wikidata.org/

The snapshot is cached locally under data/external/wikidata/catalog.json. A generation run then uses the same snapshot rather than re-querying a mutable endpoint.

### 3.2 Wikimedia Analytics — public-interest signal

For a subset of titles with English Wikipedia article links, fetch daily pageviews using the Wikimedia Analytics API. The API documents page-level pageview series and provides data from July 2015 onward. Its access policy identifies the API data as CC0 1.0, requires an informative User-Agent and asks clients to wait for each request to finish before sending the next request.

See:
- https://doc.wikimedia.org/generated-data-platform/aqs/analytics-api/reference/page-views.html
- https://doc.wikimedia.org/generated-data-platform/aqs/analytics-api/examples/page-metrics.html
- https://doc.wikimedia.org/generated-data-platform/aqs/analytics-api/documentation/access-policy.html

The snapshot is stored under data/external/wikimedia/pageviews.jsonl, with a manifest beside it. This is an external demand/attention proxy only. It is not wired into the Kafka topic model or BI metrics yet.

### 3.3 Sources deliberately not selected

IMDb bulk datasets are refreshed daily, but IMDb states that use is personal and non-commercial and restricts republishing, resale and repurposing into an online/offline movie database. That is not an appropriate dependency for a public teaching repository intended to be freely replicated. See https://www.imdb.com/interfaces/ and https://origin-help.imdb.com/article/imdb/general-information/can-i-use-imdb-data-in-my-software/G5JTRESSHJBBHTGX.

Movie ratings data is not used as a stand-in for OTT playback or subscriptions. Public catalogue metadata and public-interest signals are used only for the domains they actually measure.

## 4. Synthetic source factory

The new scripts are separate from scripts/generate_demo_data.py:

- scripts/fetch_wikidata_catalog.py retrieves and normalizes the public catalogue.
- scripts/fetch_wikimedia_pageviews.py retrieves an optional public-interest snapshot.
- scripts/generate_hybrid_dataset.py generates the OTT source files from a cached catalogue.

The generator uses a fixed seed, date range, user count and catalogue size. It refuses to overwrite a non-empty output directory unless explicitly told to do so. It writes a manifest with generator version, inputs, date range, configuration, row counts, provenance, known limitations and SHA-256 checksums.

### Source domains emitted

| Domain | Output |
|---|---|
| Content | Monthly catalogue snapshots and synthetic regional licence rows |
| Identity | Users and profiles |
| Playback | JSONL files partitioned by server-arrival date |
| Billing | Subscription events and separate gateway settlements |
| Discovery | Search logs and ratings |
| Advertising | Campaigns, ad breaks and delivered impressions |
| CDN | AkashCDN-style access logs and NimbusEdge TSV logs |
| Reference | Partners, CDN rates and edges, city/state, genre mapping, plans, empty published-metric register |

The public film catalogue is not expanded into invented television seasons, episodes or cast lists. If cast is not supplied by the public query, it remains empty.

### Controlled data behaviours

The generator introduces per-device clock offsets, event/server arrival delays, delayed offline uploads, five-minute playback heartbeats, renewal failures/recoveries, unmatched or unfilled advertising opportunities, and provider-specific CDN log formats and response codes. The manifest records the assumptions and the known limitations.

This gives learners concrete data-engineering problems to inspect and test. It does not claim the generated probability distributions are a measured model of a real streaming business.

## 5. Local data separation

Keep these roots separate:

~~~text
data/
  external/
    wikidata/       # public catalogue snapshot
    wikimedia/      # public-interest pageview snapshot
  hybrid/
    raw/            # generated OTT source files + manifest
  demo/
    raw/            # existing compact Kafka demo fixture
  raw/              # optional original coursework archive, if later found
  delta/            # local transformation outputs
  checkpoints/      # local stream state
~~~

All data roots are git-ignored. Never commit external snapshots, raw datasets, generated source data, Kafka state, Delta files, logs or credentials.

Do not weaken scripts/preflight_dataset.py to make the generated hybrid source set appear to be the course archive. The generator has its own explicit provenance and limitations.

## 6. Current implementation status

Implemented on this branch:
- Wikidata catalogue snapshot adapter.
- Wikimedia pageview sidecar adapter.
- Seeded synthetic OTT source generator.
- Generated-data manifest with row counts, provenance and SHA-256 file hashes.
- Unit tests for the synthetic source contracts, deterministic output hashes and overwrite protection.
- Hybrid Data Lab runbook and local data-layout guidance.

Not yet claimed:
- A live run against the Wikidata and Wikimedia services from a clean environment.
- A passing test result for this new code on the repository's CI.
- End-to-end Kafka/Delta/PostgreSQL/Metabase acceptance using a dataset produced by this factory.
- Full Q1–Q20 execution on the generated data.
- Use of the Wikimedia pageview sidecar in Bronze/Silver/Gold or dashboards.
- Production deployment or operational SLOs.

The dated 27 November 2026 20-user synthetic demo remains the only previously recorded end-to-end synthetic acceptance. Do not transfer its counts or pass status to this new factory.

## 7. Acceptance gates

### Gate A — source reproducibility
- Wikidata snapshot fetch succeeds and records retrieval metadata and licence.
- Pageview fetch succeeds for linked pages, writes a retrieval manifest and respects sequential request guidance.
- A generation run completes offline from the cached catalogue.
- Same catalogue snapshot, parameters and seed produce identical source-file hashes.

### Gate B — source integrity
- Every generated JSONL, JSON, CSV and provider log is parseable.
- Playback event IDs are unique unless a specific duplicate defect is deliberately enabled.
- All account, profile, content, campaign, partner and edge relationships are testable.
- Server-arrival date partitions agree with server timestamps.
- File row counts and SHA-256 values match the manifest.
- Synthetic and public provenance markers remain distinct.

### Gate C — pipeline acceptance
- Replay the generated source tree into a fresh, isolated Kafka environment.
- Reconcile source row counts to Kafka offsets and Bronze records.
- Validate each source contract and account for invalid/quarantined records.
- Build Silver and Gold outputs and verify row counts, schemas and business invariants.
- Validate PostgreSQL serving tables and all committed SQL questions.
- Capture a machine-readable run report, commit SHA, configuration and manifest hash.

### Gate D — learning and usability
- Document each data contract and the reason each transformation exists.
- Show before/after examples for deduplication, timestamp correction, late events, malformed rows and payment reconciliation.
- Explain Bronze/Silver/Gold, event time vs processing/arrival time, keys, idempotency, checkpoints and DLQ using actual outputs.
- Label dashboard and notebook data modes: public external, synthetic or original course data.
- Add the public-interest signal to an analytical comparison only after its grain and meaning are documented.

## 8. Next engineering sequence

1. Run the scripts and tests in a clean Python 3.10/3.11 environment; correct any issues found.
2. Add a dedicated hybrid-source preflight that validates every source file and key relationship without weakening the course preflight.
3. Add a safe hybrid replay runner that covers the manifest's playback arrival period and verifies the source-to-Bronze totals.
4. Run the existing Spark transformation path using the hybrid source, documenting every source or question that is not compatible.
5. Produce a stage-by-stage evidence report and rerun the local stack from a clean state.
6. Add public pageviews as a separate Gold/BI metric with a clear external-interest label.
7. Review Q1–Q20 individually and close only those items with verified output and tests.

The aim is a credible, teachable data-engineering lab: each output has a source, contract, transformation, reason, test and evidence trail. Presence of code alone is not acceptance.
