# StreamBharat — Kafka ETL extension architecture

This document describes the Kafka-based extension alongside the original PySpark workbook track. Kafka ingestion, Bronze/DLQ processing, Silver/Gold batch transformations and PostgreSQL/Metabase serving have been exercised with the local synthetic dataset. The continuous Q9/Q10 streams, Q17/Q18 live consumers and Airflow DAG remain separate runtime acceptance items. The original file-based Q17/Q18 workbook streams are not the same thing as the topic-based live interfaces.

## Architecture contract

\`\`\`mermaid
flowchart LR
  subgraph Inputs["Source producers"]
    P[Player SDK]
    B[Billing]
    S[Payment gateway]
    Q[Search]
    R[Ratings]
    A[Ad servers]
    L[Live player and ads]
  end
  subgraph Kafka["Kafka"]
    T1[(ott.player.events)]
    T2[(ott.billing.subscr)]
    T3[(ott.billing.settle)]
    T4[(ott.app.search)]
    T5[(ott.app.ratings)]
    T6[(ott.ads.breaks)]
    T7[(ott.ads.impressions)]
    T8[(ott.live.plays)]
    T9[(ott.live.ads)]
    T10[(ott.dlq)]
    T11[(ott.metrics.trending)]
    T12[(ott.alerts.ad_pacing)]
    T13[(ott.ref.subscription_state)]
  end
  subgraph Spark["PySpark / Structured Streaming"]
    BR[Append-only Bronze]
    DQ[DLQ classification]
    SI[Silver conformance]
    GO[Gold analytics]
    RT[Trending and ad pacing]
    MT[Stream metrics]
    SS[Q9 session windows]
    AJ[Q10 ad break / impression join]
  end
  subgraph Serve["Serve and orchestrate"]
    PG[(Postgres ottdw)]
    MB[Metabase]
    AF[Airflow]
  end
  P --> T1
  B --> T2
  S --> T3
  Q --> T4
  R --> T5
  A --> T6
  A --> T7
  L --> T8
  L --> T9
  T1 --> BR
  T1 --> SS
  T6 --> AJ
  T7 --> AJ
  T2 --> BR
  T3 --> BR
  T4 --> BR
  T5 --> BR
  T6 --> BR
  T7 --> BR
  T8 --> RT
  T9 --> RT
  BR --> DQ --> T10
  BR --> SI --> GO
  SI --> T13
  SS --> GO
  AJ --> GO
  RT --> T11
  RT --> T12
  RT --> MT
  GO --> PG --> MB
  AF --> Spark
  MT --> PG
\`\`\`

## Kafka contracts from the workbook

Seven replay/backfill topics:
- \`ott.player.events\` — 6 partitions, \`user_id\` key.
- \`ott.billing.subscr\` — 3, \`user_id\`.
- \`ott.billing.settle\` — 3, \`user_id\`.
- \`ott.app.search\` — 3, \`profile_id\`.
- \`ott.app.ratings\` — 1, \`profile_id\`.
- \`ott.ads.breaks\` — 3, \`user_id\`.
- \`ott.ads.impressions\` — 3, \`user_id\`.

Live feeds: \`ott.live.plays\` and \`ott.live.ads\`. Output/reference topics: \`ott.dlq\`, \`ott.metrics.trending\`, \`ott.alerts.ad_pacing\`, \`ott.ref.subscription_state\`.

## Medallion layers

Bronze preserves the original value as a string plus topic, partition, offset, Kafka timestamp, key, headers, ingestion timestamp and ingestion date. It is append-only, partitioned by ingestion date, and has one checkpoint per source topic.

Silver parses explicit/version-aware schemas, classifies malformed and semantically invalid rows, corrects player clocks, normalises seconds/milliseconds, deduplicates business keys, versions subscriptions (SCD2), builds viewing sessions and reconciles ad breaks/impressions. The independent Q9 stream uses a 30-minute session window and 12-minute watermark; Q10 uses watermarked stream-stream joins for delivered/unfilled breaks and orphan impressions.

Gold publishes daily watch time, DAU/MAU, top titles, campaign performance, financial reconciliation, partner royalties, DQ metrics, trending and pacing alerts.

## Serving and orchestration

Postgres \`ottdw\` is the BI warehouse. Spark writes to staging tables; psycopg2 runs transactional INSERT ... ON CONFLICT upserts. Metabase queries Postgres views and tables.

Airflow runs the finite nightly sequence: master data → player backfill/reconciliation → quality gate → watch time and active users → revenue and royalties → Postgres serving. Continuous streams run independently; Airflow checks their health and lag rather than blocking on long-running stream operators.

## AWS migration boundary

Keep transformations and paths configuration-driven. Later:
- Kafka → Amazon MSK.
- Delta lake → S3, using a compatible Delta/Spark runtime and catalog.
- Spark → EMR Serverless / EMR on EC2 after runtime validation.
- Airflow → MWAA or managed Airflow.
- Postgres → RDS PostgreSQL / Aurora PostgreSQL.
- Dashboard → private Metabase hosting or selected BI platform.
- Credentials → IAM roles and Secrets Manager.

Do not embed AWS IDs, credentials or endpoints in transformations. Local Compose remains the developer profile; AWS networking, IAM, storage catalogue, deployment and cost sizing are a separate phase.


## Runtime profile and validation boundary

The local development profile is a single Kafka broker in KRaft mode, a Spark 3.5.3 standalone master/worker pair, PostgreSQL 16, Metabase and an optional Airflow stack. It is intended for development and academic integration work, not high availability.

The 27 November 2026 synthetic acceptance exercised seven source topics through Bronze/DLQ, offset reconciliation, Silver player/subscription processing, selected Gold transformations, PostgreSQL serving and Metabase. The verified evidence is detailed in [project-status-and-validation.md](project-status-and-validation.md).

The following are not established by that synthetic acceptance: continuous Q9/Q10 session/ad streams, Q17/Q18 topic consumers, full Airflow orchestration, all original Q1–Q20 workbook outputs, full course-data reconciliation, resilience drills or production throughput. Keep these items as explicit follow-up work rather than implying that the architecture diagram alone proves they ran.
