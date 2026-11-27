# StreamBharat Metabase question and dashboard specification

Connect Metabase to PostgreSQL:
- Host: `localhost` on the desktop, or `postgres` inside Compose
- Port: `5432`
- Database: `ottdw`
- Username: `ott`
- Password: `ott_local_dev` (local development only)
- Schema: `ottdw`

Create the collection **StreamBharat Operations & Business Intelligence**.

## Dashboard 1 — Executive OTT Performance

**Daily engagement overview**
```sql
SELECT activity_date, watch_hours, daily_viewers, dau, mau,
       ad_spend_inr, impressions
FROM ottdw.v_executive_daily
ORDER BY activity_date;
```

**Top 10 titles by language**
```sql
SELECT activity_date, language, rank_no, title, viewers, watched_seconds
FROM ottdw.v_top10_titles_this_week
ORDER BY activity_date DESC, language, rank_no;
```

**Current subscription mix**
```sql
SELECT plan, status, users
FROM ottdw.v_subscription_current_mix
ORDER BY plan, users DESC;
```

**Payment failures in seven-day grace**
```sql
SELECT user_id, plan, payment_failed_at, grace_expires_at
FROM ottdw.v_subscription_grace_period
ORDER BY grace_expires_at;
```

**Daily campaign spend vs budget**
```sql
SELECT activity_date, campaign_id, impressions, spend_inr,
       daily_budget_inr, pacing_ratio, alert_status
FROM ottdw.campaign_daily
ORDER BY activity_date, campaign_id;
```

## Dashboard 2 — Streaming & Delivery Operations

**Live session windows (Q9)**
```sql
SELECT viewing_date, language, report_content_id, title,
       COUNT(*) AS sessions,
       COUNT(DISTINCT profile_id) AS profiles,
       SUM(watched_seconds) / 3600.0 AS watched_hours,
       AVG(CASE WHEN completed THEN 1.0 ELSE 0.0 END) AS completion_rate
FROM ottdw.v_viewing_sessions_stream
WHERE viewing_date >= CURRENT_DATE - INTERVAL '7 days'
GROUP BY viewing_date, language, report_content_id, title
ORDER BY viewing_date DESC, watched_hours DESC;
```

**Live ad delivery status (Q10)**
```sql
SELECT activity_date, delivery_status, campaign_id,
       SUM(delivery_rows) AS break_rows,
       SUM(matched_impressions) AS impressions,
       SUM(estimated_spend) AS estimated_spend
FROM ottdw.v_ad_delivery_stream
WHERE activity_date >= CURRENT_DATE - INTERVAL '7 days'
GROUP BY activity_date, delivery_status, campaign_id
ORDER BY activity_date DESC, estimated_spend DESC;
```

**Orphan impressions**
```sql
SELECT * FROM ottdw.v_orphan_impressions_stream
ORDER BY activity_date DESC, orphan_impressions DESC;
```

**DLQ records by source and reason**
```sql
SELECT activity_date, source_topic, reason_code, invalid_records,
       affected_partitions, first_seen_at, last_seen_at
FROM ottdw.v_dlq_metrics_daily
ORDER BY activity_date DESC, invalid_records DESC;
```

**Latest stream health by query/topic**
```sql
SELECT metric_ts, query_name, topic, input_rows_per_second,
       processed_rows_per_second, batch_duration_ms,
       consumer_lag, freshness_seconds
FROM ottdw.v_stream_health_latest
ORDER BY query_name, topic;
```

**Hourly ad fill rate**
```sql
SELECT hour_start, total_breaks, filled_breaks, unfilled_breaks, fill_rate
FROM ottdw.ad_fill_rate_hourly
ORDER BY hour_start;
```

**Ad delivery exceptions**
```sql
SELECT *
FROM ottdw.v_ad_delivery_quality_daily
ORDER BY activity_date;
```

**Regional CDN cost**
```sql
SELECT month_start, cdn, region, requests, egress_gb, cost_inr,
       missing_rate_requests
FROM ottdw.v_cdn_cost_by_region_month
ORDER BY month_start, cdn, region;
```

## Dashboard 3 — Finance & Data Quality

**Revenue reconciliation**
```sql
SELECT settlement_status, records, charge_amount_inr, linked_settlements
FROM ottdw.v_revenue_reconciliation_summary
ORDER BY settlement_status;
```

**Partner royalties and minimum guarantee**
```sql
SELECT quarter_start, partner_id, revenue_share_inr,
       guarantee_topup_inr, payout_inr, payout_basis
FROM ottdw.v_royalty_payouts
ORDER BY quarter_start, partner_id;
```

**DQ pass rate and failed rows**
```sql
SELECT batch_id, rule_name, severity, passed_rows, failed_rows, pass_rate
FROM ottdw.v_dq_rule_status
ORDER BY batch_id, severity, rule_name;
```

**Published board variance (0.5% tolerance)**
```sql
SELECT month, metric, published_value, actual_value, variance_pct,
       reconciliation_status, investigation_hint
FROM ottdw.v_published_metric_variance
ORDER BY month, reconciliation_status, metric;
```

**Subscription churn**
```sql
SELECT churn_type, users, refreshed_at
FROM ottdw.subscription_churn
ORDER BY churn_type;
```

**Live ad pacing alerts**
```sql
SELECT alert_type, campaign_id, user_id, event_ts, spend_inr,
       pacing_ratio, details
FROM ottdw.ad_pacing_alerts
ORDER BY event_ts DESC;
```

## Metabase setup boundary

The SQL views and card queries are committed. Metabase must still be connected
to the populated warehouse, the saved questions created from the SQL above,
and the three dashboards assembled in the UI. The DLQ topic/Delta table does
not yet have a Postgres aggregate table; add that serving aggregate before
creating a DLQ chart.
