# StreamBharat — target big-data platform

Architecture for the Kafka ETL workbook, extending existing PySpark/Spark SQL without claiming the existing file-based streaming jobs already satisfy the Kafka design.

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
  end
  subgraph Spark["PySpark / Structured Streaming"]
    BR[Append-only Bronze]
    DQ[DLQ classification]
    SI[Silver conformance]
    GO[Gold analytics]
    RT[Trending and ad pacing]
    MT[Stream metrics]
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
  T2 --> BR
  T3 --> BR
  T4 --> BR
  T5 --> BR
  T6 --> BR
  T7 --> BR
  T8 --> RT
  T9 --> RT
  BR --> DQ
  BR --> SI --> GO
  RT --> GO
  RT --> T8
  RT --> T9
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

Silver parses explicit/version-aware schemas, classifies malformed and semantically invalid rows, corrects player clocks, normalises seconds/milliseconds, deduplicates business keys, versions subscriptions (SCD2), builds viewing sessions and reconciles ad breaks/impressions.

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
