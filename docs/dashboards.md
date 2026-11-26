# StreamBharat dashboard design

The warehouse schema and the Metabase saved-question plan live in:
- \`sql/001_warehouse.sql\`
- \`config/dashboard/streambharat-metabase-questions.md\`

The target BI surface has three dashboards:
1. Executive OTT Performance: daily watch hours, DAU/MAU, top titles by language, subscription status/plan mix and campaign spend versus budget.
2. Streaming Operations: input/processing throughput, batch duration, Kafka partition lag, gold freshness and DLQ counts.
3. Finance & Data Quality: unsettled/duplicate charges, quarterly partner payout, voluntary/involuntary subscription churn, rule failures and ad pacing alerts.

Metabase is configured after Postgres contains data. Screenshots and verified business values are runtime deliverables, not generated fixtures.
