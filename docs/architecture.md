# StreamBharat — Solution Architecture

## 1. Architecture objectives

StreamBharat is a workbook-aligned lakehouse implementation for a fictional OTT platform. Its architecture is designed to:

- preserve raw source data and ingestion lineage;
- convert heterogeneous JSON, CSV, JSONL and provider logs into conformed datasets;
- support finite batch processing and file-based event-time streaming;
- publish reusable Gold facts and business aggregates;
- support recommendation and churn modelling with time-aware features;
- validate data quality, safety rules and financial reconciliations.

The assignment's core implementation uses PySpark, Spark Structured Streaming, Delta Lake and Spark MLlib. Kafka, Airflow, PostgreSQL and a dashboard-serving platform are not assumed by the Workbook 2 specification.

## 2. System context

The platform scenario includes playback, subscription, catalogue, advertising, search, CDN and user/profile domains. The supplied learning dataset contains approximately 1.62 million playback events, 2,500 accounts and 4,736 profiles; it is not a production-scale 3-crore-user dataset.

~~~mermaid
flowchart TB
    subgraph Inputs["Source systems / supplied course files"]
        PLAY["Player events<br/>JSONL"]
        BILL["Subscription events<br/>JSON + gateway settlements CSV"]
        CAT["Catalogue snapshots<br/>Nested JSON + licences CSV"]
        AD["Ad breaks and impressions<br/>CSV"]
        SEARCH["Search logs<br/>JSON"]
        CDN["AkashCDN logs + NimbusEdge TSV"]
        ID["Users, profiles and ratings<br/>JSON"]
    end
    subgraph Platform["StreamBharat processing platform"]
        ING["Ingestion and schema enforcement"]
        LAKE["Delta Lake zones"]
        TRANS["Spark transformations"]
        QA["Quality and reconciliation gates"]
        ML["Spark MLlib"]
        STREAM["Structured Streaming"]
    end
    PLAY --> ING
    BILL --> ING
    CAT --> ING
    AD --> ING
    SEARCH --> ING
    CDN --> ING
    ID --> ING
    ING --> LAKE
    LAKE --> TRANS
    TRANS --> QA
    TRANS --> ML
    ING --> STREAM
    STREAM --> QA
    ML --> QA
    QA --> OUT["Validated business outputs<br/>and model scores"]
    TRANS --> GOLD["Gold facts and aggregates"]
    GOLD --> OUT
~~~

## 3. Lakehouse / medallion architecture

~~~mermaid
flowchart LR
    subgraph RAW["RAW — immutable source"]
      R1["Playback JSONL"]
      R2["Billing + settlements"]
      R3["Catalogue + licence files"]
      R4["Ads + search + CDN"]
      R5["Users + profiles + ratings"]
    end
    subgraph BRONZE["BRONZE — source-aligned"]
      B1["Playback Bronze<br/>Q4"]
      B2["CDN request records<br/>Q3"]
      B3["Live event ledgers<br/>Q17 / Q18"]
    end
    subgraph SILVER["SILVER — conformed"]
      S1["playback_clean<br/>Q2"]
      S2["cdn_usage_hourly<br/>Q3"]
      S3["search_clean<br/>Q7"]
      S4["Catalogue dimensions<br/>Q1"]
    end
    subgraph GOLD["GOLD — business products"]
      G1["Viewing sessions / watch time<br/>Q5"]
      G2["Subscription periods / MRR<br/>Q6 / Q11"]
      G3["Active users / Top 10<br/>Q9 / Q10"]
      G4["Ads / royalties / CDN cost<br/>Q13–Q15"]
      G5["Recommendations / churn<br/>Q16 / Q20"]
      G6["Quality and safety reports<br/>Q19"]
    end
    R1 --> B1 --> S1
    R4 --> B2 --> S2
    R3 --> S4
    R2 --> G2
    R1 --> G1
    S1 --> G1
    S1 --> G3
    S1 --> G5
    S4 --> G1
    S4 --> G3
    S4 --> G4
    S4 --> G5
    R4 --> S3
    S1 --> G4
    G1 --> G3
    G1 --> G5
    G2 --> G4
    G1 --> G6
    G2 --> G6
    G4 --> G6
    G5 --> G6
~~~

### Zone responsibilities

| Zone | Design rule | Representative artifacts |
|---|---|---|
| Raw | Immutable; preserve delivered source files | data/raw/** |
| Bronze | Preserve source-shaped records, event keys and lineage | data/delta/bronze/playback, cdn_requests |
| Silver | Correct types, timestamps, duplicate keys and standardised dimensions | playback_clean, cdn_usage_hourly, search_clean |
| Gold | Publish business-grain tables for analytics and downstream models | Sessions, daily watch time, subscriptions, MRR, royalties, recommendations, churn scores |
| Quality | Record exceptions, assertions, reconciliations and gate results | data/delta/quality/** |

Exact paths and table contracts are listed in [docs/delivery-plan.md](delivery-plan.md). Course raw files should never be modified in place.

## 4. Batch processing and dependency graph

The finite orchestrator is **python -m streambharat.pipeline**. It executes Q1–Q16, Q19 and Q20 in dependency order. Q17 and Q18 are excluded from the finite sequence because they are long-running streaming jobs.

~~~mermaid
flowchart TD
    Q1["Q1 Catalogue model"]
    Q2["Q2 Playback cleansing"]
    Q3["Q3 CDN parsing"]
    Q4["Q4 Incremental Bronze"]
    Q5["Q5 Sessions / watch time"]
    Q6["Q6 Subscription lifecycle"]
    Q7["Q7 Search analytics"]
    Q8["Q8 SCD2 / licences"]
    Q9["Q9 Active users"]
    Q10["Q10 Top 10"]
    Q11["Q11 Cohorts / MRR"]
    Q12["Q12 Account sharing"]
    Q13["Q13 Advertising"]
    Q14["Q14 Skew / CDN cost"]
    Q15["Q15 Royalties"]
    Q16["Q16 ALS recommendations"]
    Q19["Q19 Quality gates"]
    Q20["Q20 Churn + integration"]
    Q1 --> Q8
    Q1 --> Q10
    Q1 --> Q13
    Q1 --> Q16
    Q2 --> Q5
    Q2 --> Q9
    Q2 --> Q10
    Q2 --> Q12
    Q2 --> Q14
    Q2 --> Q16
    Q2 --> Q20
    Q4 --> Q2
    Q5 --> Q9
    Q5 --> Q10
    Q5 --> Q12
    Q5 --> Q15
    Q5 --> Q16
    Q5 --> Q20
    Q6 --> Q11
    Q6 --> Q15
    Q6 --> Q20
    Q7 --> Q19
    Q8 --> Q15
    Q8 --> Q16
    Q8 --> Q19
    Q11 --> Q15
    Q13 --> Q15
    Q14 --> Q19
    Q15 --> Q19
    Q16 --> Q19
    Q9 --> Q19
    Q19 --> Q20
~~~

This dependency graph is a logical data-dependency view; the current batch orchestrator uses a fixed stage order. A selected stage assumes that its upstream output already exists. Use --dry-run to inspect the planned list, not as proof that transformations execute.

## 5. Streaming architecture

Q17 and Q18 are driven by the supplied ott_stream_simulator.py. It writes JSON files to local folders; the Spark file source discovers those files. Neither job requires Kafka for this workbook.

~~~mermaid
flowchart LR
    SIM["ott_stream_simulator.py"]
    SIM -->|--stream plays| PLAYDIR["data/stream/plays/"]
    SIM -->|--stream ads| ADDIR["data/stream/ads/"]
    PLAYDIR --> Q17["Q17 Trending stream"]
    Q17 --> BUF["Bounded Delta event buffer"]
    BUF --> WIN["10-minute windows<br/>1-minute slide"]
    BASE["Prior 7-day<br/>same-hour baseline"] --> WIN
    WIN --> TREND["Top 20 by language<br/>growth + viewer threshold"]
    ADDIR --> Q18["Q18 Ad pacing stream"]
    Q18 --> LEDGER["Delta impression ledger<br/>key: impression_id"]
    LEDGER --> CAP["Per-user / campaign / day<br/>frequency-cap state"]
    LEDGER --> PACE["Campaign spend pacing"]
    CAP --> ALERT["Capped users + alerts"]
    PACE --> ALERT
~~~

### Q17 — trending

- Event-time windows: 10 minutes, sliding every minute.
- Late-event allowance: configurable, default 15 minutes.
- Exact distinct profile counts are calculated over a bounded Delta event buffer in foreachBatch.
- Current windows are compared with a same-hour, prior-seven-day baseline.
- Minimum viewer threshold and top-N per language are configurable.
- Checkpoint: data/checkpoints/q17_trending.

### Q18 — live ad pacing

- Consumes simulator ad-impression files.
- Persists unique events in a Delta ledger keyed by impression_id.
- Recomputes per-user/campaign/day frequency and spend from unique ledger rows to make micro-batch retries idempotent.
- Publishes capped-user and pacing-alert outputs.
- Uses its own checkpoint, separate from Q17.

## 6. Machine-learning architecture

~~~mermaid
flowchart TB
    WATCH["Q5 cleaned watch-time facts"]
    CATALOG["Q1 catalogue + Q8 licence/SCD2"]
    USERS["Profiles + kids flags"]
    RATING["Ratings"]
    SUB["Q6 subscription history"]
    PLAY["Q2 corrected playback"]
    WATCH --> ALSINPUT["Implicit feedback matrix"]
    RATING --> ALSINPUT
    CATALOG --> ALSFILTER["Candidate eligibility filter"]
    USERS --> ALSFILTER
    ALSINPUT --> ALS["Spark MLlib ALS"]
    ALS --> ALSFILTER
    ALSFILTER --> REC["Recommendations + Precision@10"]
    SUB --> LABEL["Monthly-cycle labels<br/>30-day cycle + 7-day grace"]
    PLAY --> FEAT["Pre-anchor engagement features"]
    WATCH --> FEAT
    LABEL --> CHURN["Chronological train/test split"]
    FEAT --> CHURN
    CHURN --> LR["Spark MLlib Logistic Regression"]
    LR --> SCORE["Churn probability × plan value"]
    SCORE --> PRIORITY["Prioritised retention scores"]
~~~

Q16 uses implicit engagement signals and evaluates recommendations against held-out recent titles, with licence and kids-safety filtering. Q20 uses features observed before the prediction anchor, a chronological evaluation split and a separate final refit for scoring. Model metrics must come from actual Spark execution; do not substitute illustrative values.

## 7. Reliability, state and data governance

| Concern | Design control | Evidence to capture |
|---|---|---|
| File replay | Stable Structured Streaming checkpoints | Second Q4 run with no new files |
| Duplicate playback | Deduplication and Delta upsert on event_id | Duplicate-key count |
| Duplicate live ad events | Q18 Delta ledger keyed by impression_id | Replayed-batch row-count comparison |
| Schema drift | Explicit input schemas and controlled additive-field migration | Before/after Delta schemas on pinned runtime |
| Event-time lateness | Q17 bounded event buffer and late-event horizon | Test with delayed simulator events |
| Timestamp quality | Q2 device-offset estimate and offline-event handling | Client/server offset distribution and impact |
| Financial correctness | Currency rounding and settlement/royalty reconciliation | Settlement and partner reconciliation reports |
| Kids safety | Age rating joined to profile safety flags | Exception counts and failing examples |
| Model leakage | Feature timestamps strictly before anchor | Feature-window audit and chronological split |

## 8. Runtime topology and local execution

~~~mermaid
flowchart TB
    DEV["Developer workstation / lab VM"]
    DATA["Course dataset<br/>data/raw/"]
    PY["Python environment<br/>PySpark 3.5.3"]
    JVM["Java 17 + Spark driver"]
    DELTA["Delta Lake tables<br/>data/delta/"]
    CKPT["Streaming checkpoints<br/>data/checkpoints/"]
    ART["Reports / models<br/>artifacts/"]
    DEV --> DATA
    DEV --> PY
    PY --> JVM
    JVM --> DELTA
    JVM --> CKPT
    JVM --> ART
~~~

The documented baseline is Python 3.10/3.11, Java 17, PySpark 3.5.3 and Delta Lake 3.2.0. The session factory defaults to local Spark execution for the course/lab setup. A distributed cluster, cloud object store, scheduler or dashboard layer would be an extension, not a current workbook dependency.

## 9. Repository modules and question ownership

Each question is implemented in a separate src/streambharat/qN_*.py module. The team allocation is recorded in [docs/questions-and-ownership.md](questions-and-ownership.md); per-question algorithms, assumptions and outputs are documented in [docs/team-workstreams.md](team-workstreams.md).

Primary orchestration and shared modules:

- pipeline.py: finite batch stage order and CLI controls.
- spark_session.py: Spark + Delta session setup.
- quality_rules.py: deterministic business-rule functions.
- q17_trending.py and q18_live_ad_pacing.py: separate streaming entry points.

## 10. Validation boundary and release gates

The deterministic Python tests, Python compilation and batch dry-run validate only their respective scopes. They do not establish that Spark/Delta jobs have completed against all course files.

Before presenting the project as end-to-end validated:

1. Run each batch module in the pinned Spark/Delta environment.
2. Inspect schemas, row counts, nulls, duplicate keys and quarantine volumes.
3. Verify Q4 checkpoint replay and additive cast_device migration.
4. Run Q17/Q18 with delayed events and replayed files; verify checkpoint recovery.
5. Reconcile subscription MRR, gateway settlement, ad credits and partner royalties.
6. Validate Q16 precision@10 and Q20 ROC-AUC/PR-AUC from chronological holdouts.
7. Run the full integration sequence and capture logs, output schemas and sample records.
8. Have each module owner explain assumptions, edge cases and trade-offs.

**Current status:** all 20 question modules and the finite orchestrator are present. Full Spark/Delta integration, data-level reconciliation and measured ML performance remain validation work. The architecture intentionally distinguishes code coverage from runtime evidence.
