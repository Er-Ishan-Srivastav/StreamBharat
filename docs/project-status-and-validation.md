# StreamBharat — project history, implementation status and validation record

**Record updated:** 27 November 2026  
**Repository baseline:** main at merge commit 46ae729 (PR #2)  
**Validation profile:** local Docker Compose stack + 20-user synthetic dataset  
**Overall status:** synthetic local vertical slice completed; full course-data and production-readiness work remain open.

## 1. Executive assessment

StreamBharat has reached a verified local synthetic end-to-end milestone. The local Kafka, Spark/Delta, PostgreSQL and Metabase path was brought up; a deterministic 20-user synthetic source set was replayed; Bronze, DLQ, offset reconciliation, Silver, Gold, PostgreSQL serving, warehouse SQL and Metabase resources were checked.

This is **not evidence that the complete course dataset or every Q1–Q20 question has been executed end-to-end**. The acceptance run used isolated synthetic inputs under **data/demo/raw/** for one demonstration date. The original course dataset under **data/raw/** was not processed in this run.

Use these terms precisely:

- **Implemented in the repository** means the source modules, scripts, DAGs, schemas or SQL are present.
- **Locally validated** means the relevant code path was actually executed and its output was inspected.
- **CI validated** means the automated GitHub Actions workflow completed successfully for the referenced commit.
- **Not yet validated** means code or configuration may exist, but the corresponding runtime/data/performance evidence has not been collected.

## 2. Project scope and tracks

The repository brings together two related bodies of work:

1. The original StreamBharat OTT PySpark workbook, containing Q1–Q20 question-specific modules and a finite batch orchestrator.
2. A Kafka-based ETL extension with source topics, Bronze ingestion, DLQ handling, Silver/Gold transformations, PostgreSQL serving and Metabase dashboards.

The business scenario is a fictional OTT platform. The synthetic demo data is test data, not real customer or platform activity. The repository's course-data instructions describe a larger supplied dataset, but that dataset was not part of this acceptance run.

## 3. What is present in the repository

The current repository contains the following implementation areas:

| Area | Repository implementation | Acceptance status in this record |
|---|---|---|
| Workbook analytics | Q1–Q20 modules and finite batch pipeline | Modules are present; the full Q1–Q20 flow was not run against the course dataset |
| Local infrastructure | Kafka in KRaft mode, Spark master/worker and workspace, PostgreSQL 16, Metabase; optional Airflow Compose | Core stack startup and infrastructure smoke checks passed locally |
| Kafka contracts | Seven source topics, live topics, DLQ, metrics/alert and compacted subscription-state topics | Topic contracts checked by the local smoke test; the seven source feeds were exercised |
| Bronze and DLQ | Append-only Bronze ingestion, validation, DLQ classification/routing and retained-offset reconciliation | Exercised on the synthetic source feeds |
| Silver | Player schema-v1/v2 parsing, device-clock handling, deduplication and subscription SCD2 | Player and subscription outputs verified on the synthetic run |
| Gold | Engagement, active users, top titles, viewing sessions, drop-off, ad delivery/campaign metrics and finance/royalties | The listed batch outputs were generated and checked on the synthetic run |
| Warehouse and BI | PostgreSQL staging/upserts, dashboard SQL, Metabase connection, saved questions and dashboards | PostgreSQL rows, all 20 SQL questions, 3 dashboards and 20 saved questions verified |
| Continuous streaming | Q9/Q10 session/ad streams and Q17/Q18 live trending/ad pacing | Not exercised in this acceptance run |
| Airflow | Full-refresh/date-sliced orchestration DAG and optional Compose stack | Airflow was kept stopped; no Airflow DAG run is claimed |
| Cloud deployment | AWS migration plan | Deferred; no AWS deployment is claimed |

## 4. Recorded local acceptance run

### Run identity and boundaries

| Item | Recorded value |
|---|---|
| Local execution date | 27 November 2026 |
| Synthetic business date | 15 October 2026 |
| Synthetic users | 20 |
| Synthetic run ID from source-generation log | 20261127T091200Z |
| Synthetic source root | data/demo/raw/ |
| Original course data | data/raw/ not processed |
| Airflow | Not started during this run |
| Runtime shape | Single local development stack; not a high-availability deployment |

The infrastructure smoke module reported **5/5 checks passed**: Kafka/topic contracts, Spark cluster/session, Spark Kafka connector access, Delta round-trip and PostgreSQL JDBC round-trip. The full course dataset preflight was deferred because the course source data was not present for this run.

### Kafka and Bronze source counts

| Source feed | Bronze rows |
|---|---:|
| Player events | 61 |
| Subscription events | 60 |
| Billing settlements | 40 |
| App search | 20 |
| App ratings | 20 |
| Ad breaks | 20 |
| Ad impressions | 20 |
| **Total source Bronze rows** | **241** |

The source generator intentionally included two invalid records. They were routed to the DLQ:

| Source topic | Partition / offset | Reason | Records |
|---|---:|---|---:|
| ott.app.ratings | 0 / 0 | INVALID_RATING | 1 |
| ott.player.events | 0 / 9 | MISSING_POSITION | 1 |
| **Total DLQ records** | | | **2** |

The Silver player output contained **60 rows** after excluding the invalid player record. Silver subscriptions contained **60 rows**.

### Kafka-to-Bronze reconciliation

The reconciliation report contained **22 topic-partition rows** across the seven source topics. Every reported partition had **reconciled = true** and **count_delta = 0**. This establishes agreement between Bronze and the broker's retained offset ranges at the time of the check; it does not prove offsets that may have expired before the check.

### Delta outputs generated

| Delta path | Rows |
|---|---:|
| lake/gold/watch_time_daily | 20 |
| lake/gold/active_users_daily | 2 |
| lake/gold/top_titles_daily | 1 |
| lake/gold/viewing_sessions | 20 |
| lake/gold/content_dropoff_daily | 1 |
| lake/silver/ad_delivery | 20 |
| lake/gold/campaign_daily | 1 |
| lake/gold/ad_fill_rate_hourly | 1 |
| lake/gold/revenue_reconciliation | 40 |
| lake/gold/royalties_quarterly | 1 |
| lake/gold/dlq_metrics_daily | 2 |

The final demo verifier reported that all **22 required Delta output contracts** met their minimum row-count checks. That verifier includes Bronze, DLQ, offset reconciliation, Silver and Gold paths; the table above lists the business outputs for easier reading.

### PostgreSQL serving counts

| PostgreSQL table (schema ottdw) | Rows |
|---|---:|
| watch_time_daily | 20 |
| active_users_daily | 2 |
| top_titles_daily | 1 |
| subscriptions_scd2 | 60 |
| viewing_sessions | 20 |
| content_dropoff_daily | 1 |
| ad_delivery | 20 |
| campaign_daily | 1 |
| ad_fill_rate_hourly | 1 |
| revenue_reconciliation | 40 |
| royalties_quarterly | 1 |
| dlq_metrics_daily | 2 |

All **12 required PostgreSQL output tables** were non-empty and passed the demo output verifier.

### SQL, Metabase and automated tests

| Check | Result |
|---|---|
| Shell syntax validation | Passed |
| Kafka Docker Compose configuration | Passed |
| Airflow Docker Compose configuration | Passed |
| Python compileall in workspace | Passed |
| Local pytest suite | **77 passed** |
| Committed warehouse SQL questions | **20/20 executed successfully** |
| Metabase dashboards | **3 verified** |
| Metabase saved questions | **20 verified** |
| Metabase warehouse connection | Verified; database ID 1 |
| GitHub Actions Python tests, run #849 | Passed |
| GitHub Actions Kafka ETL validation, run #444 | Passed |

Useful evidence links:

- [GitHub Actions](https://github.com/Er-Ishan-Srivastav/StreamBharat/actions)
- [Python tests — run 849](https://github.com/Er-Ishan-Srivastav/StreamBharat/actions/runs/45994205696)
- [Kafka ETL validation — run 444](https://github.com/Er-Ishan-Srivastav/StreamBharat/actions/runs/45994205667)
- [PR #2 — runtime compatibility fixes](https://github.com/Er-Ishan-Srivastav/StreamBharat/pull/2)
- [Merged main commit 46ae729](https://github.com/Er-Ishan-Srivastav/StreamBharat/commit/46ae729)

## 5. Issues found and fixes applied during acceptance

The local run exposed several environment and orchestration issues. The Python executable, Spark listener property and PostgreSQL identifier fixes were merged into main through PR #2. The sequential DLQ runner hardening and manual acceptance workflow Python 3 corrections were merged through PR #3.

1. **Python executable mismatch:** the workspace image provides python3, while several shell entry points called python. The affected local scripts now use python3.
2. **Spark listener property conflict:** StreamingMetricsListener previously assigned to PySpark's read-only spark property. The reference is now stored as _spark_session. A separate asynchronous callback/shutdown warning was still observed during stream teardown; this is a known limitation and is not claimed as fully resolved.
3. **PostgreSQL table-name validation:** the original validation rejected the digit in subscriptions_scd2. The accepted pattern now permits digits after the first lowercase letter while restricting the identifier to lowercase letters, digits and underscores.
4. **Shared DLQ Delta target:** launching all seven topic validations simultaneously caused a concurrent Delta table-initialization conflict on the first empty DLQ path. The local recovery ran the topic validations sequentially. The synthetic runner now preserves that sequential execution on fresh runs.
5. **Synthetic catalogue path:** manual Gold/session/ad commands must use the generated catalogue at data/demo/raw/content/catalog_2026-10-15.json, not the default course-data path under data/raw/.

The original synthetic runner invocation stopped at Bronze because of the python/python3 mismatch. After the runtime fixes, the remaining stages were completed manually and verified stage-by-stage. The corrected one-command runner has **not yet been independently re-run from start to finish on a fresh isolated set of Kafka topics, checkpoints and volumes**. Do not represent the original manual recovery as a clean one-shot runner acceptance.

## 6. Explicitly not completed or not evidenced by this run

The following work remains before describing the entire StreamBharat coursework/platform as fully validated:

- **Original course dataset:** place the supplied source files under data/raw/, run preflight, then execute the full historical workflow. No original-data metrics or workbook reconciliation results are claimed here.
- **All Q1–Q20 outputs:** module files exist, but this synthetic vertical slice is not evidence that every question has been run and reconciled on the supplied data.
- **Airflow:** the DAG is present, but Airflow was intentionally kept stopped to conserve local memory. No DAG task-level success is claimed.
- **Q9/Q10 continuous streams:** viewing_sessions_stream, ad_delivery_stream and orphan_impressions_stream were not produced in this run.
- **Q17/Q18 live streams:** live trending and live ad-pacing pipelines were not run. The optional lake/gold/trending and lake/gold/pacing_alerts_live paths were absent during serving.
- **Broader DQ/CDN/board reconciliation:** lake/gold/dq_metrics, lake/gold/cdn_egress_daily and lake/gold/published_metrics_reconciliation were absent. DLQ validation and daily DLQ aggregation did run; those are not substitutes for all workbook data-quality and CDN/board reconciliation tasks.
- **Streaming metrics:** lake/gold/stream_metrics was not established as a successful populated output in this acceptance. The asynchronous listener shutdown warning remains noted above.
- **Resilience tests:** broker outage/recovery, long-lived checkpoint restart, replay after failure, alert retry and sustained late-event drills were not executed as a complete acceptance suite.
- **Performance:** no throughput, end-to-end latency, lag baseline under sustained load or Spark job-duration benchmark is claimed.
- **AWS:** no cloud resources or production deployment were created.

The serving job intentionally skips optional Delta paths that are not present. Those skip messages are expected for this synthetic run; they do not mean those optional analytics were generated.

## 7. Safe rerun guidance

The local acceptance stack contains persistent Kafka offsets, streaming checkpoints, Delta tables and PostgreSQL state. Do not simply replay the same synthetic date into the existing persistent stack and assume it is a clean repeat: replay can create new Kafka offsets and the existing checkpoints/lake tables remain stateful.

For a new acceptance, use a deliberately isolated environment with its own Docker Compose project/volumes, Kafka topics, lake root and checkpoint root, or follow a documented reset procedure after preserving anything needed. Do not use docker compose down -v or docker system prune --volumes as a casual troubleshooting step; those commands can delete persistent project data.

The synthetic inputs are isolated under data/demo/raw/. The original course input location data/raw/ remains reserved for the supplied dataset.

## 8. Recommended next milestones

1. **Dataset onboarding:** obtain the course dataset, place it under data/raw/, run preflight and record file counts/schema checks.
2. **Full workbook execution:** run the batch pipeline and validate each Q1–Q20 output schema, keys, counts and business rules.
3. **Financial/data reconciliation:** compare settlements, royalties and published metrics with the supplied references; record measured variances and exceptions.
4. **Streaming acceptance:** execute Q9/Q10/Q17/Q18 with dedicated checkpoints; test restart, late events, duplicate replay and broker interruption.
5. **Airflow acceptance:** run the full-refresh DAG and a date-sliced rerun; inspect task logs and idempotency.
6. **Performance and capacity:** record job durations, throughput, batch latency, consumer lag and resource usage with documented dataset size.
7. **Cloud deployment decision:** only after local correctness, security, network, IAM, storage/catalog and cost requirements are documented.

## 9. Completion statement

As of 27 November 2026, StreamBharat has a **locally verified synthetic Kafka → Bronze/DLQ → Silver/Gold → PostgreSQL → Metabase vertical slice**, passing 77 local tests, 20 warehouse SQL executions, 3-dashboard/20-question Metabase verification and both GitHub Actions workflows referenced above.

The project repository is merged and stable on main. The full course-data workload, complete Q1–Q20 acceptance, optional continuous streaming jobs, Airflow orchestration, measured performance and AWS deployment remain separate milestones. This is the accurate project status; it should not be described as production-ready or as a completed full-course-data run.


### CI follow-up after the recorded acceptance

After PR #3 was merged, the Kafka ETL workflow passed on main (run #448). The subsequent Python tests push run on the same main baseline (run #853) failed while Spark's Ivy resolver attempted to obtain Delta Lake 3.2.1 from Maven Central; Spark then could not load DeltaCatalog. The Python test workflow passed in the PR context (run #852), and the local test suite had passed 77 tests. This discrepancy is recorded as a CI dependency-bootstrapping issue, not as a product-level test assertion failure. A follow-up change is being prepared to install the pinned Delta JARs directly into the PySpark test runtime and make that path explicit.
