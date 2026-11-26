# StreamBharat Metabase dashboard specification

Connect Metabase to PostgreSQL:
- Host: localhost (desktop) / postgres (inside Compose network)
- Port: 5432
- Database: ottdw
- Username: ott
- Password: the local POSTGRES_PASSWORD from .env
- Schema: ottdw

Create a collection named **StreamBharat Operations & Business Intelligence**.

## Dashboard 1 — Executive OTT Performance
1. Daily watch hours: sum(watched_seconds)/3600 by viewing_date from watch_time_daily.
2. DAU and MAU: active_users_daily by activity_date, split by metric_type.
3. Top 10 titles by language: v_top10_titles_this_week, filter by language.
4. Subscription mix: current rows from subscriptions_scd2 grouped by status and plan.
5. Payment-failure grace: current payment_failed users and oldest valid_from timestamp.
6. Campaign spend versus budget: campaign_daily combo chart.

## Dashboard 2 — Streaming Operations
1. Input and processing rate by query from stream_metrics.
2. Latest consumer lag grouped by topic.
3. Micro-batch duration trend from batch_duration_ms.
4. Gold data freshness using freshness_seconds.
5. DLQ records by reason/source once its warehouse aggregate is loaded.

## Dashboard 3 — Finance & Data Quality
1. Unsettled/duplicate charges from revenue_reconciliation.
2. Quarterly partner payout from royalties_quarterly.
3. Failed DQ records by rule from dq_metrics.
4. Voluntary versus involuntary churn from subscription_churn.
5. Recent ad pacing alerts from ad_pacing_alerts.

The SQL DDL and question spec are committed. Metabase still requires initial setup, saving questions and assembling dashboards after data is available.

## Suggested SQL cards

**Current subscription state by plan**

```sql
SELECT status, COALESCE(plan, 'unknown') AS plan, COUNT(*) AS users
FROM ottdw.subscriptions_scd2
WHERE is_current = TRUE
GROUP BY status, COALESCE(plan, 'unknown')
ORDER BY users DESC;
```

**Payment failures inside the grace period**

```sql
SELECT user_id, plan, valid_from AS payment_failed_at,
       valid_from + INTERVAL '7 days' AS grace_expires_at
FROM ottdw.subscriptions_scd2
WHERE is_current = TRUE AND status = 'payment_failed'
ORDER BY valid_from;
```

**Voluntary versus involuntary churn**

```sql
SELECT churn_type, users, refreshed_at
FROM ottdw.subscription_churn
ORDER BY churn_type;
```

The question definitions are SQL-ready specifications. Metabase still requires initial
connection, saved questions and dashboard assembly after the first successful data run.
