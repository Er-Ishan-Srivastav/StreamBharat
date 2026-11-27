-- Query-ready views for the StreamBharat Metabase collections.
-- Run after 001_warehouse.sql and after the local Postgres warehouse is initialized.

CREATE OR REPLACE VIEW ottdw.v_executive_daily AS
WITH dates AS (
    SELECT viewing_date AS activity_date FROM ottdw.watch_time_daily
    UNION
    SELECT activity_date FROM ottdw.active_users_daily
    UNION
    SELECT activity_date FROM ottdw.campaign_daily
),
watch AS (
    SELECT viewing_date AS activity_date,
           SUM(watched_seconds) / 3600.0 AS watch_hours,
           COUNT(DISTINCT user_id) AS viewers
    FROM ottdw.watch_time_daily
    GROUP BY viewing_date
),
active AS (
    SELECT activity_date,
           MAX(active_users) FILTER (WHERE metric_type = 'DAU') AS dau,
           MAX(active_users) FILTER (WHERE metric_type = 'MAU') AS mau
    FROM ottdw.active_users_daily
    GROUP BY activity_date
),
ads AS (
    SELECT activity_date, SUM(spend_inr) AS ad_spend_inr,
           SUM(impressions) AS impressions
    FROM ottdw.campaign_daily
    GROUP BY activity_date
)
SELECT d.activity_date,
       COALESCE(w.watch_hours, 0) AS watch_hours,
       COALESCE(w.viewers, 0) AS daily_viewers,
       COALESCE(a.dau, 0) AS dau,
       COALESCE(a.mau, 0) AS mau,
       COALESCE(ads.ad_spend_inr, 0) AS ad_spend_inr,
       COALESCE(ads.impressions, 0) AS impressions
FROM dates d
LEFT JOIN watch w USING (activity_date)
LEFT JOIN active a USING (activity_date)
LEFT JOIN ads USING (activity_date);

CREATE OR REPLACE VIEW ottdw.v_viewing_session_kpis_daily AS
SELECT viewing_date,
       COUNT(*) AS sessions,
       COUNT(DISTINCT profile_id) AS profiles,
       SUM(watched_seconds) AS watched_seconds,
       AVG(completion_rate) AS average_completion_rate,
       AVG(dropoff_seconds) AS average_dropoff_seconds,
       SUM(CASE WHEN completed THEN 1 ELSE 0 END) AS completed_sessions
FROM ottdw.viewing_sessions
GROUP BY viewing_date;

CREATE OR REPLACE VIEW ottdw.v_ad_delivery_quality_daily AS
SELECT activity_date,
       COUNT(*) FILTER (WHERE delivery_status = 'DELIVERED') AS delivered_impressions,
       COUNT(*) FILTER (WHERE delivery_status = 'UNFILLED_BREAK') AS unfilled_breaks,
       COUNT(*) FILTER (WHERE delivery_status = 'FILLED_WITHOUT_IMPRESSION') AS missing_impressions,
       COUNT(*) FILTER (WHERE delivery_status = 'ORPHAN_IMPRESSION') AS orphan_impressions,
       COUNT(*) FILTER (WHERE genre_eligible = FALSE) AS genre_target_mismatches,
       SUM(spend_inr) AS delivered_spend_inr
FROM ottdw.ad_delivery
GROUP BY activity_date;

CREATE OR REPLACE VIEW ottdw.v_revenue_reconciliation_summary AS
SELECT settlement_status,
       COUNT(*) AS records,
       SUM(charge_amount) AS charge_amount_inr,
       COUNT(*) FILTER (WHERE settlement_id IS NOT NULL) AS linked_settlements
FROM ottdw.revenue_reconciliation
GROUP BY settlement_status;

CREATE OR REPLACE VIEW ottdw.v_royalty_payouts AS
SELECT quarter_start, partner_id, revenue_share_inr,
       guarantee_topup_inr, payout_inr,
       CASE WHEN guarantee_topup_inr > 0 THEN 'MINIMUM_GUARANTEE_APPLIED'
            ELSE 'REVENUE_SHARE' END AS payout_basis
FROM ottdw.royalties_quarterly;

CREATE OR REPLACE VIEW ottdw.v_dq_rule_status AS
SELECT batch_id, rule_name, severity, passed_rows, failed_rows,
       CASE WHEN failed_rows = 0 THEN 1.0
            ELSE passed_rows::double precision
                 / NULLIF(passed_rows + failed_rows, 0) END AS pass_rate
FROM ottdw.dq_metrics;

CREATE OR REPLACE VIEW ottdw.v_stream_health_latest AS
SELECT DISTINCT ON (query_name, topic)
       metric_ts, query_name, topic, input_rows_per_second,
       processed_rows_per_second, batch_duration_ms, event_time_max,
       consumer_lag, freshness_seconds
FROM ottdw.stream_metrics
ORDER BY query_name, topic, metric_ts DESC;

CREATE OR REPLACE VIEW ottdw.v_subscription_current_mix AS
SELECT plan, status, COUNT(DISTINCT user_id) AS users
FROM ottdw.subscriptions_scd2
WHERE is_current = TRUE
GROUP BY plan, status;

CREATE OR REPLACE VIEW ottdw.v_subscription_grace_period AS
SELECT user_id, plan, valid_from AS payment_failed_at,
       valid_from + INTERVAL '7 days' AS grace_expires_at
FROM ottdw.subscriptions_scd2
WHERE is_current = TRUE AND status = 'payment_failed';

CREATE OR REPLACE VIEW ottdw.v_cdn_monthly_cost AS
SELECT date_trunc('month', activity_date)::date AS month_start,
       cdn, region, SUM(request_count) AS requests,
       SUM(egress_gb) AS egress_gb, SUM(cost_inr) AS cost_inr,
       SUM(missing_rate_requests) AS missing_rate_requests
FROM ottdw.cdn_egress_daily
GROUP BY 1, 2, 3;


CREATE OR REPLACE VIEW ottdw.v_published_metric_variance AS
SELECT month, metric, actual_metric, published_value, actual_value,
       variance_absolute, variance_pct, tolerance_pct,
       reconciliation_status, investigation_hint, published_ts, compared_at
FROM ottdw.published_metrics_reconciliation;


CREATE OR REPLACE VIEW ottdw.v_dlq_metrics_daily AS
SELECT activity_date, source_topic, reason_code,
       invalid_records, affected_partitions,
       first_seen_at, last_seen_at, first_offset, last_offset
FROM ottdw.dlq_metrics_daily;


CREATE OR REPLACE VIEW ottdw.v_viewing_sessions_stream AS
SELECT viewing_date, profile_id, user_id, device_id, content_id,
       report_content_id, title, language, session_start_ts, session_end_ts,
       watched_seconds, event_count, heartbeat_count, completed
FROM ottdw.viewing_sessions_stream;

CREATE OR REPLACE VIEW ottdw.v_ad_delivery_stream AS
SELECT activity_date, delivery_status, campaign_id, content_id,
       COUNT(*) AS delivery_rows,
       COUNT(impression_id) AS matched_impressions,
       SUM(COALESCE(cpm_inr, 0) / 1000.0) AS estimated_spend
FROM ottdw.ad_delivery_stream
GROUP BY activity_date, delivery_status, campaign_id, content_id;

CREATE OR REPLACE VIEW ottdw.v_orphan_impressions_stream AS
SELECT activity_date, campaign_id, content_id, COUNT(*) AS orphan_impressions,
       MIN(impression_end_ts) AS first_seen_at,
       MAX(impression_end_ts) AS last_seen_at
FROM ottdw.orphan_impressions_stream
GROUP BY activity_date, campaign_id, content_id;
