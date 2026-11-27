# StreamBharat Metabase dashboards

The local warehouse exposes Postgres tables and query-ready views. On a fresh
Postgres volume, `docker-compose.kafka.yml` initializes `001_warehouse.sql`
followed by `003_dashboard_views.sql`. On an existing volume, apply
`sql/003_dashboard_views.sql` manually once.

Connect Metabase to:
- Host: `postgres` inside Compose, or `localhost` from the desktop
- Port: `5432`
- Database: `ottdw`
- User: `ott`
- Password: the `POSTGRES_PASSWORD` value in `.env`
- Schema: `ottdw`

Create the collection **StreamBharat Operations & Business Intelligence**.

## Dashboard 1 — Executive OTT Performance

| Card | Source | Visualization |
|---|---|---|
| Watch hours / daily viewers / DAU / MAU | `v_executive_daily` | Multi-series time series |
| Top 10 titles by language | `v_top10_titles_this_week` | Ranked bar chart with language filter |
| Current subscriptions by plan and state | `v_subscription_current_mix` | Stacked bar / donut |
| Payment failures in grace period | `v_subscription_grace_period` | Table with grace expiry |
| Campaign spend vs daily budget | `campaign_daily` | Combo chart |
| Viewing sessions and completion | `v_viewing_session_kpis_daily` | Time series |

## Dashboard 2 — Streaming & Delivery Operations

| Card | Source | Visualization |
|---|---|---|
| Input/processed rates, event time and freshness | `v_stream_health_latest` | KPI + time series |
| Latest Kafka lag by topic | `v_stream_health_latest` | Horizontal bar |
| Micro-batch duration trend | `stream_metrics` | Time series |
| Ad fill rate by hour | `ad_fill_rate_hourly` | Time series |
| Delivered, unfilled, orphan and genre mismatch counts | `v_ad_delivery_quality_daily` | Stacked bar |
| CDN egress and cost by region/provider | `v_cdn_monthly_cost` | Monthly trend and region table |
| Invalid source messages by source/reason/day | `v_dlq_metrics_daily` | Stacked bar + detail table |

## Dashboard 3 — Finance & Data Quality

| Card | Source | Visualization |
|---|---|---|
| Settled, unsettled, duplicate and orphan settlements | `v_revenue_reconciliation_summary` | Status bar + total amounts |
| Quarterly partner payout / guarantee top-up | `v_royalty_payouts` | Partner table |
| FAIL/WARN rows and pass rate | `v_dq_rule_status` | Rule status table |
| Voluntary vs involuntary churn | `subscription_churn` | Bar chart |
| Active campaign pacing alerts | `ad_pacing_alerts` | Alert table |

## Minimum saved questions

The SQL specification in `config/dashboard/streambharat-metabase-questions.md`
is parsed by `scripts/bootstrap_metabase.py` during local initialization. The
script creates the initial administrator, warehouse connection, collection,
saved SQL questions and three dashboards. Cards start as tables for reliable
inspection; charts and filters can be refined after the first data load. No
sample data or screenshots are presented as if they were measured output.
