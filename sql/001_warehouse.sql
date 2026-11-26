CREATE SCHEMA IF NOT EXISTS ottdw;

CREATE TABLE IF NOT EXISTS ottdw.watch_time_daily (
    viewing_date DATE NOT NULL, profile_id TEXT NOT NULL, user_id TEXT NOT NULL,
    content_id TEXT NOT NULL, watched_seconds BIGINT NOT NULL DEFAULT 0,
    completion_rate NUMERIC(8,5), refreshed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (viewing_date, profile_id, content_id)
);
CREATE TABLE IF NOT EXISTS ottdw.active_users_daily (
    activity_date DATE NOT NULL,
    metric_type TEXT NOT NULL CHECK (metric_type IN ('DAU','MAU')),
    active_users BIGINT NOT NULL, refreshed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (activity_date, metric_type)
);
CREATE TABLE IF NOT EXISTS ottdw.top_titles_daily (
    activity_date DATE NOT NULL, language TEXT NOT NULL, rank_no INTEGER NOT NULL,
    content_id TEXT NOT NULL, title TEXT, viewers BIGINT NOT NULL,
    watched_seconds BIGINT NOT NULL DEFAULT 0, refreshed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (activity_date, language, rank_no)
);
CREATE TABLE IF NOT EXISTS ottdw.campaign_daily (
    activity_date DATE NOT NULL, campaign_id TEXT NOT NULL,
    impressions BIGINT NOT NULL DEFAULT 0,
    spend_inr NUMERIC(18,2) NOT NULL DEFAULT 0,
    daily_budget_inr NUMERIC(18,2),
    pacing_ratio NUMERIC(12,5),
    unfilled_breaks BIGINT NOT NULL DEFAULT 0,
    filled_missing_impression BIGINT NOT NULL DEFAULT 0,
    orphan_impressions BIGINT NOT NULL DEFAULT 0,
    frequency_cap_breaches BIGINT NOT NULL DEFAULT 0,
    genre_mismatch_count BIGINT NOT NULL DEFAULT 0,
    alert_status TEXT,
    refreshed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(), PRIMARY KEY (activity_date, campaign_id)
);
CREATE TABLE IF NOT EXISTS ottdw.revenue_reconciliation (
    charge_id TEXT PRIMARY KEY, user_id TEXT NOT NULL, charge_amount NUMERIC(18,2),
    settlement_id TEXT, settlement_status TEXT NOT NULL,
    charge_ts TIMESTAMPTZ, settlement_ts TIMESTAMPTZ,
    refreshed_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE TABLE IF NOT EXISTS ottdw.royalties_quarterly (
    quarter_start DATE NOT NULL, partner_id TEXT NOT NULL,
    revenue_share_inr NUMERIC(18,2) NOT NULL DEFAULT 0,
    guarantee_topup_inr NUMERIC(18,2) NOT NULL DEFAULT 0,
    payout_inr NUMERIC(18,2) NOT NULL DEFAULT 0,
    refreshed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(), PRIMARY KEY (quarter_start, partner_id)
);
CREATE TABLE IF NOT EXISTS ottdw.dq_metrics (
    batch_id TEXT NOT NULL, rule_name TEXT NOT NULL, severity TEXT NOT NULL,
    passed_rows BIGINT NOT NULL, failed_rows BIGINT NOT NULL,
    refreshed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(), PRIMARY KEY (batch_id, rule_name)
);
CREATE TABLE IF NOT EXISTS ottdw.stream_metrics (
    metric_ts TIMESTAMPTZ NOT NULL, query_name TEXT NOT NULL, topic TEXT,
    input_rows_per_second DOUBLE PRECISION, processed_rows_per_second DOUBLE PRECISION,
    batch_duration_ms BIGINT, event_time_max TIMESTAMPTZ, consumer_lag BIGINT,
    freshness_seconds DOUBLE PRECISION,
    PRIMARY KEY (metric_ts, query_name, topic)
);
CREATE TABLE IF NOT EXISTS ottdw.ad_pacing_alerts (
    alert_id TEXT PRIMARY KEY, alert_type TEXT NOT NULL, campaign_id TEXT, user_id TEXT,
    event_ts TIMESTAMPTZ NOT NULL, spend_inr NUMERIC(18,2), pacing_ratio NUMERIC(12,5),
    details JSONB NOT NULL DEFAULT '{}'::jsonb, created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_watch_time_daily_date ON ottdw.watch_time_daily(viewing_date);
CREATE INDEX IF NOT EXISTS idx_top_titles_date_lang ON ottdw.top_titles_daily(activity_date, language);
CREATE INDEX IF NOT EXISTS idx_campaign_daily_date ON ottdw.campaign_daily(activity_date);
CREATE INDEX IF NOT EXISTS idx_stream_metrics_query_ts ON ottdw.stream_metrics(query_name, metric_ts DESC);

CREATE OR REPLACE VIEW ottdw.v_top10_titles_this_week AS
SELECT activity_date, language, rank_no, content_id, title, viewers, watched_seconds
FROM ottdw.top_titles_daily
WHERE activity_date >= CURRENT_DATE - INTERVAL '6 days' AND rank_no <= 10;

CREATE OR REPLACE VIEW ottdw.v_campaign_pacing_status AS
SELECT activity_date, campaign_id, impressions, spend_inr, daily_budget_inr, pacing_ratio,
       CASE WHEN pacing_ratio > 1.2 THEN 'AHEAD / OVERSPENDING'
            WHEN pacing_ratio < 0.7 THEN 'BEHIND PLAN' ELSE 'ON TRACK' END AS pacing_label
FROM ottdw.campaign_daily;

CREATE TABLE IF NOT EXISTS ottdw.subscriptions_scd2 (
    user_id TEXT NOT NULL,
    valid_from TIMESTAMPTZ NOT NULL,
    plan TEXT,
    status TEXT NOT NULL,
    event_id TEXT NOT NULL,
    event_type TEXT NOT NULL,
    valid_to TIMESTAMPTZ,
    is_current BOOLEAN NOT NULL,
    churn_type TEXT,
    is_synthetic BOOLEAN NOT NULL DEFAULT FALSE,
    kafka_timestamp TIMESTAMPTZ,
    kafka_partition INTEGER,
    kafka_offset BIGINT,
    refreshed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (user_id, valid_from)
);
CREATE INDEX IF NOT EXISTS idx_subscriptions_current
    ON ottdw.subscriptions_scd2(user_id) WHERE is_current = TRUE;

CREATE TABLE IF NOT EXISTS ottdw.subscription_churn (
    churn_type TEXT PRIMARY KEY CHECK (churn_type IN ('VOLUNTARY','INVOLUNTARY')),
    users BIGINT NOT NULL,
    refreshed_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS ottdw.viewing_sessions (
    session_id TEXT PRIMARY KEY,
    viewing_date DATE NOT NULL,
    user_id TEXT,
    profile_id TEXT NOT NULL,
    device_id TEXT NOT NULL,
    content_id TEXT NOT NULL,
    report_content_id TEXT,
    title TEXT,
    language TEXT,
    session_start_ts TIMESTAMPTZ NOT NULL,
    session_end_ts TIMESTAMPTZ NOT NULL,
    session_duration_seconds BIGINT NOT NULL DEFAULT 0,
    event_count BIGINT NOT NULL DEFAULT 0,
    watched_seconds BIGINT NOT NULL DEFAULT 0,
    max_position_sec DOUBLE PRECISION,
    duration_sec DOUBLE PRECISION,
    completion_rate NUMERIC(8,5),
    completed BOOLEAN NOT NULL DEFAULT FALSE,
    dropoff_seconds DOUBLE PRECISION,
    refreshed_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_viewing_sessions_date_profile
    ON ottdw.viewing_sessions(viewing_date, profile_id);

CREATE TABLE IF NOT EXISTS ottdw.content_dropoff_daily (
    viewing_date DATE NOT NULL,
    report_content_id TEXT NOT NULL,
    language TEXT NOT NULL,
    sessions BIGINT NOT NULL,
    viewers BIGINT NOT NULL,
    watched_seconds BIGINT NOT NULL,
    completed_sessions BIGINT NOT NULL,
    average_completion_rate NUMERIC(8,5),
    average_dropoff_seconds DOUBLE PRECISION,
    refreshed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (viewing_date, report_content_id, language)
);


-- Ad delivery facts retain the break/impression join result and Kafka lineage.
CREATE TABLE IF NOT EXISTS ottdw.ad_delivery (
    delivery_id TEXT PRIMARY KEY,
    break_id TEXT,
    impression_id TEXT,
    campaign_id TEXT,
    user_id TEXT,
    profile_id TEXT,
    content_id TEXT,
    break_filled INTEGER,
    break_ts TIMESTAMPTZ,
    ad_break_ts TIMESTAMPTZ,
    duration_sec INTEGER,
    completed INTEGER,
    cpm_inr NUMERIC(18,4),
    delivery_status TEXT NOT NULL,
    genre_eligible BOOLEAN,
    activity_date DATE,
    spend_inr NUMERIC(18,4) NOT NULL DEFAULT 0,
    break_partition INTEGER,
    break_offset BIGINT,
    impression_partition INTEGER,
    impression_offset BIGINT,
    refreshed_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_ad_delivery_date_campaign
    ON ottdw.ad_delivery(activity_date, campaign_id);
CREATE INDEX IF NOT EXISTS idx_ad_delivery_user_campaign
    ON ottdw.ad_delivery(user_id, campaign_id, activity_date);

CREATE TABLE IF NOT EXISTS ottdw.ad_fill_rate_hourly (
    hour_start TIMESTAMPTZ PRIMARY KEY,
    total_breaks BIGINT NOT NULL,
    filled_breaks BIGINT NOT NULL,
    unfilled_breaks BIGINT NOT NULL,
    fill_rate NUMERIC(8,5),
    refreshed_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Additive migration clauses keep an existing local Postgres volume upgradeable.
ALTER TABLE ottdw.campaign_daily
    ADD COLUMN IF NOT EXISTS unfilled_breaks BIGINT NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS filled_missing_impression BIGINT NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS orphan_impressions BIGINT NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS frequency_cap_breaches BIGINT NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS genre_mismatch_count BIGINT NOT NULL DEFAULT 0;


-- Upgrade existing local development volumes without discarding warehouse data.
ALTER TABLE ottdw.ad_delivery
    ADD COLUMN IF NOT EXISTS break_filled INTEGER;

UPDATE ottdw.stream_metrics SET topic = 'unknown' WHERE topic IS NULL;
ALTER TABLE ottdw.stream_metrics
    ALTER COLUMN topic SET NOT NULL;
ALTER TABLE ottdw.stream_metrics
    DROP CONSTRAINT IF EXISTS stream_metrics_pkey;
ALTER TABLE ottdw.stream_metrics
    ADD CONSTRAINT stream_metrics_pkey
    PRIMARY KEY (metric_ts, query_name, topic);
