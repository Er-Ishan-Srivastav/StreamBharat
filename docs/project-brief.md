# Project brief — StreamBharat OTT Streaming Analytics Platform

## Business context

StreamBharat is a fictional OTT video platform with 3 crore registered users, a free advertising-supported tier and two paid plans. It distributes films, web series in eight Indian languages and live cricket.

The project responds to five connected business needs:

1. **Content intelligence:** understand what viewers actually watch and how discovery performs.
2. **Finance and partner settlement:** derive subscription revenue and calculate accurate content-partner royalties.
3. **Advertising accountability:** measure delivery, frequency caps, campaign pacing and revenue credits.
4. **Platform operations:** understand CDN consumption, regional delivery costs and Spark data skew.
5. **Growth and retention:** measure engagement, recommend relevant titles and identify subscribers at risk of churn.

## Scope and source of truth

This repository implements **Project 4 / Workbook 2: StreamBharat — OTT Streaming Analytics Platform**. The student-facing assignment is included at `source-materials/Project-4-StreamBharat-PySpark-Workbook.pdf`.

The related Kafka + PySpark real-time ETL workbook is a separate assignment and is not the specification for this repo. The supplied simulator produces files for Spark Structured Streaming; Kafka is not a required component in this project brief.

## Data and scale

The supplied course sample contains 2,500 user accounts and 4,736 profiles. Subscription history spans May 2025–October 2026; playback, search and ads span July–October 2026; CDN access logs cover September 2026. Playback input contains approximately 1.62 million events. The player emits a heartbeat every five minutes in this sample (not every 30 seconds), so watch-time logic must use the dataset's five-minute convention.

## Technical objectives

- Ingest nested JSON and raw logs with explicit, controlled schemas.
- Preserve source data and create reproducible Bronze, Silver and Gold datasets.
- Process incremental file arrivals safely with streaming checkpoints.
- Correct playback timestamps and account for offline uploads.
- Build reliable viewing sessions, subscription periods, metrics and finance outputs.
- Support event-time streaming analytics for trending and live ad pacing.
- Train MLlib recommendation and churn models with leakage-aware evaluation.
- Add automated data-quality checks, kids-safety controls and regression tests.

## Expected overall outputs

The workbook's question-level outputs include flattened catalogue dimensions and bridges, cleaned playback, incremental Bronze ingestion, viewing sessions and watch-time facts, subscription periods, search analytics, SCD2 catalogue/licence reports, active-user metrics, regional Top 10, cohort/MRR reports, account-sharing flags, ad-delivery/pacing reports, CDN usage/cost outputs, partner royalty statements, recommendations, live trending, live ad caps/pacing alerts, quality reports/tests, and churn-risk scores with an integrated pipeline.
