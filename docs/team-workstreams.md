# Team workstreams — implementation guide for Q1–Q20

This page documents the code now present in `src/streambharat/`. The assignment source is Workbook 2, *Project 4: StreamBharat: OTT Streaming Analytics Platform*. Output paths below are repository-relative Delta paths unless otherwise stated.

## Q1 — Nested catalogue and dimensions

**Owner:** Harshvardhan · **Module:** `q1_catalog.py`

Reads a nested monthly catalogue snapshot with an explicit schema, flattens movie/series titles and episode rows, retains the parent-series and season/episode keys, and publishes `dim_content`, `content_language`, `content_genre` and `content_cast`. The genre bridge joins the supplied `genre_mapping.csv`; unmapped values are preserved rather than dropped. Language/genre catalogue-hour summaries are also published under `data/delta/silver/catalog/`.

## Q2 — Playback cleaning and device-clock correction

**Owner:** Rutik · **Module:** `q2_playback_clean.py`

Deduplicates by `event_id`, estimates each device's `client_ts - server_ts` offset from online events, uses the per-device median and median absolute deviation to assess reliability, and corrects timestamps only when the sample count, dispersion and offset bounds pass. Unreliable devices fall back to server time. Offline events are retained and flagged; they are not used to estimate online clock offsets. Outputs: `playback_clean`, `device_clock_offsets` and `q2_daily_clock_impact` under `data/delta/silver/`.

## Q3 — CDN provider parsing and quarantine

**Owner:** Siddhi · **Module:** `q3_cdn.py`

Parses the AkashCDN quoted access-log format and NimbusEdge tab-separated format into one schema; converts Nimbus epoch timestamps in UTC; maps provider edge/POP identifiers through `cdn_edges.csv`; quarantines malformed records and records malformed counts by source file. Successful HTTP 200 requests are aggregated hourly by provider, region and content. Outputs: `data/delta/bronze/cdn_requests`, `data/delta/quarantine/cdn_bad_lines`, `data/delta/quality/cdn_quarantine_counts` and `data/delta/silver/cdn_usage_hourly`.

## Q4 — Incremental playback ingestion

**Owner:** Ishan · **Module:** `q4_incremental_ingestion.py`

Uses an explicit file-source schema, `maxFilesPerTrigger`, `availableNow`, a persistent checkpoint and a `foreachBatch` Delta merge on `event_id`. It adds source-file/ingestion-time lineage and supports the nullable additive `cast_device` field. Output: `data/delta/bronze/playback`; checkpoint: `data/checkpoints/q4_playback`. Validate replay behaviour and schema evolution on the pinned Delta runtime before treating the stage as released.

## Q5 — Viewing sessions and watch time

**Owner:** Rutik · **Module:** `q5_sessions.py`

Orders events by device and corrected event time, creates a new session after 30 minutes of inactivity, and counts only heartbeat intervals no longer than six minutes when the previous event is `start` or `heartbeat`. Pause and seek records break watch-time intervals. Per session/title watch time is capped at 1.5× content duration; completion is capped at 100%. Outputs: `data/delta/gold/viewing_sessions` and `data/delta/gold/watch_time_daily`.

## Q6 — Subscription lifecycle and gateway reconciliation

**Owner:** Kshitij Jaisawal · **Module:** `q6_subscriptions.py`

Deduplicates and orders subscription events, creates trial/paid/paused/grace/churned periods, gives payment failures a seven-day recovery window, and creates a churned state at the end of an unrecovered grace period. Daily snapshots use end-exclusive periods. Expected MRR is calculated once per subscriber/month (latest valid paid period) before comparing with gateway settlements; repeated renewals/upgrades in the same month must not multiply one subscriber's MRR. Outputs: `subscription_periods`, `subscribers_daily`, `paid_subscriber_counts` and `gateway_reconciliation` under `data/delta/gold/`.

## Q7 — Search normalisation and content gaps

**Owner:** Siddhi · **Module:** `q7_search.py`

Normalises case, punctuation, repeated letters and whitespace; groups near-duplicate searches using first-character/length blocking plus Levenshtein distance; links search events to a playback start within ten minutes; and ranks the 50 largest weekly zero-result query groups. A fuzzy title match marks likely spelling/discovery issues separately from candidate catalogue gaps. Outputs: `data/delta/silver/search_clean`, `data/delta/gold/search_conversion` and `data/delta/gold/weekly_content_gaps`. Similarity grouping is a heuristic, not a full transitive clustering algorithm.

## Q8 — Content SCD Type 2 and regional licences

**Owner:** Harshvardhan · **Module:** `q8_content_history.py`

Loads all monthly catalogue snapshots, creates a Delta history commit per snapshot, and builds an SCD2 dimension for title, tier, partner, age-rating and duration changes. Playback is joined to the city/state map and licence validity periods to identify regional violations. Delta time travel publishes the 1-Aug-2026 snapshot. Outputs: `data/delta/gold/dim_content_scd2`, `data/delta/gold/licence_violations`, `data/delta/reference/catalog_snapshot_history` and `data/delta/reference/catalog_as_of_2026_08_01`.

## Q9 — DAU, WAU, MAU and stickiness

**Owner:** Ankita · **Module:** `q9_active_users.py`

Treats a user as active on a date after at least one minute of watch time, counts distinct users rather than devices, and computes exact and approximate rolling 7-/30-day counts plus stickiness by plan/platform/state. Subscription plans are assigned only to paid/grace snapshots; trial/paused/churned/non-paid rows fall into the free audience grouping. The board comparison uses calendar-month MAU and average daily active users, joined by both `month` and `metric`. Outputs: `data/delta/gold/active_users_daily`, `data/delta/quality/q9_published_metric_comparison` and `data/delta/quality/q9_recomputed_metrics`.

## Q10 — Regional and national Top 10

**Owner:** Harshvardhan · **Module:** `q10_top10.py`

Rolls episode activity to parent series, counts distinct viewers with at least two minutes watched, ranks by viewers and then watch minutes, and creates separate kids/non-kids lists. States below the 50-viewer threshold receive the national ranking as a fallback; a national storefront is also published. Rank movement is calculated against the previous available day. Output: `data/delta/gold/top10_daily`.

## Q11 — Subscriber cohorts and MRR bridge

**Owner:** Kshitij Jaisawal · **Module:** `q11_cohorts_mrr.py`

Builds first-paid-month cohorts, a cohort/month retention matrix and a month spine that includes zero-MRR months. It classifies MRR changes as new, expansion, contraction, churned, reactivated or no-change. End-exclusive periods are expanded only through the day before their end date. Outputs: `data/delta/gold/cohort_retention`, `cohort_retention_pivot` and `mrr_bridge_monthly`.

## Q12 — Account sharing

**Owner:** Rutik · **Module:** `q12_account_sharing.py`

Expands online sessions into five-minute time slots, counts distinct simultaneous devices/cities, compares concurrency against the plan entitlement valid on each date, and flags accounts exceeding their limit on five or more days or repeatedly streaming from multiple cities simultaneously. Estimated additional households and potential revenue are heuristic estimates. The dataset has city names but no geographic coordinates, so the multi-city signal is a proxy rather than a measured physical distance. Output: `data/delta/gold/account_sharing_flags`.

## Q13 — Advertising delivery, frequency and campaign pacing

**Owner:** Ankita · **Module:** `q13_ads.py`

Combines ad-break and impression data to calculate platform/day fill, campaign/day impressions, completion, CPM revenue and budget pacing; computes per-user/campaign/day cap violations and credit-backs; tests campaign genre targets; and writes title/day ad revenue for Q15. Since the supplied `ad_breaks.csv` has no campaign ID on unfilled rows, fill rate is reported at platform/day level, not falsely attributed to individual campaigns. Outputs include `ad_delivery_daily`, `frequency_violations`, `campaign_pacing`, `ad_targeting_violations` and `ad_revenue_by_title_day`.

## Q14 — Live-cricket skew and CDN costs

**Owner:** Rutik · **Module:** `q14_skew_optimization.py`

Identifies the largest content key on the configured cricket-final date, times a baseline and AQE join, compares a salted hot-key join, writes a date/hour partitioned Delta layout, attempts content Z-ordering when supported, and calculates CDN cost by title/region using `cdn_rates.csv`. The measured timings are environment-specific and must be captured from the actual Spark runtime. Outputs: `artifacts/reports/q14_performance/`, `data/delta/performance/playback_partitioned`, `data/delta/gold/cdn_cost_by_title` and the cricket-final subset.

## Q15 — Partner royalties and guarantees

**Owner:** Kshitij Jaisawal · **Module:** `q15_royalties.py`

Uses the SCD2 partner/tier valid on the viewing date; allocates the premium subscription pool using partner premium-minute share and contractual revenue share; attributes free-tier ad revenue to title/partner; applies quarterly minimum guarantees; and publishes partner-level reconciliation plus title audit statements. Monthly partner totals include partners earning only free-tier ad share. Largest-remainder rounding distributes residual paise at partner and title level so the audit statement reconciles. Outputs: `partner_royalties_quarterly`, `_title_statement` and `_reconciliation`.

## Q16 — Implicit ALS recommendations

**Owner:** Harshvardhan · **Module:** `q16_recommendations.py`

Builds implicit feedback from watch minutes, completion, rewatch days and ratings; maps profile/content IDs to integer ALS keys; holds out the five most recent titles per profile; trains implicit-feedback MLlib ALS; filters candidates by date-valid state licence, kids age restrictions and previously watched titles; and compares against a language-popularity baseline. Offline evaluation uses training-only history for candidate filtering so held-out positives can be recommended, while published production recommendations exclude the full watched history. Precision@10 includes zero-hit held-out profiles in the denominator. Outputs: `data/delta/gold/recommendations`, `_popularity_baseline` and `_evaluation`.

## Q17 — Real-time trending

**Owner:** Ishan · **Module:** `q17_trending.py`

Reads simulator start events with a ten-minute event-time window sliding every minute, compares distinct profiles with a same-hour seven-day baseline, applies the 200-viewer threshold and ranks the top 20 per language. A bounded Delta event buffer plus `foreachBatch` allows exact distinct-profile counts and incorporates late events. This is a separate long-running stream; see the Q17 implementation note.

## Q18 — Live ad caps and pacing

**Owner:** Kshitij Jaisawal · **Module:** `q18_live_ad_pacing.py`

Consumes simulator ad-impression files, stores events in a Delta ledger keyed by `impression_id`, and recomputes campaign/user/day frequency and spend aggregates from distinct ledger rows. This ledger makes micro-batch retries idempotent. It publishes capped users and over-/under-spend alerts against the configured match window. This is a separate long-running stream and uses its own checkpoint.

## Q19 — Data quality and kids safety

**Owner:** Ankita · **Module:** `q19_quality.py`, `quality_rules.py`

Checks kids-profile plays of 16+/18+ titles against SCD2 validity, per-profile/day watch time above 24 hours, unknown content IDs, royalty reconciliation and published-metric drift beyond 0.5%. Published metric audit snapshots are appended; a missing Q9 recomputation is marked `NOT_RUN`, not falsely passed. Quality gate results are written to `data/delta/quality/quality_gate_results`; failures stop the quality stage.

## Q20 — Churn model and integration

**Owner:** Ishan (Rutik technical support) · **Module:** `q20_churn.py`, `pipeline.py`

Creates monthly subscription-cycle examples with a 30-day cycle plus seven-day grace; a successful renewal inside the 37-day outcome window is retained even if an intermediate payment attempt/cancellation appears. Features use data strictly before the anchor: heartbeat-derived watch minutes, activity days, decline, payment failures, tenure, prior renewals and plan. It trains a chronological Spark MLlib Logistic Regression model, reports ROC-AUC/PR-AUC when computable, refits on all matured labels, and ranks active paid subscriber accounts by churn probability × plan value. Prefer Q2's corrected Silver playback input.

## Validation boundary

All modules have Python syntax validation. The deterministic suite tests pure rules such as watch-time caps, seven-day payment recovery, metric drift, exact-currency allocation and minimum guarantees. The environment used to prepare this code did not have PySpark installed, so **Spark/Delta execution, full-data output reconciliation and ML evaluation are not claimed as complete**. Run the batch pipeline and the two live simulators in the agreed Java 17 / PySpark 3.5.3 / Delta 3.2.0 environment and record the resulting evidence before submission.
