# Question register and ownership

This register summarises all 20 tasks in Workbook 2 and records the agreed six-person ownership. The student-facing workbook in `source-materials/` remains the detailed assignment specification.

## Team allocation

| Member | Workstream | Questions | Primary responsibility |
|---|---|---|---|
| Ishan | Project lead; ingestion, streaming and integration | Q4, Q17, Q20 | Architecture coordination, shared interfaces, Q4/Q17 implementation, Q20 model and final pipeline integration |
| Rutik | Playback engineering and Spark optimization | Q2, Q5, Q12, Q14 | Clock correction, viewing sessions, account sharing and skew/performance work |
| Harshvardhan | Content modelling and discovery | Q1, Q8, Q10, Q16 | Catalogue model, SCD2/licensing, regional rankings and ALS recommendations |
| Kshitij Jaisawal | Subscriptions, finance and live ad pacing | Q6, Q11, Q15, Q18 | Subscription lifecycle, cohort/MRR, royalties and live pacing/frequency limits |
| Siddhi | CDN processing and search analytics | Q3, Q7 | CDN parsing/usage and search quality/content gaps |
| Ankita | Engagement metrics, advertising and quality | Q9, Q13, Q19 | DAU/WAU/MAU, ad delivery/revenue and data-quality/kids safety tests |

Q20 technical implementation receives support from Rutik; Ishan remains responsible for the end-to-end Q20 pipeline and release integration. No ability-tier labels are used for team members.

## Questions 1–10

| Q | Topic | Main work | Expected output | Owner |
|---|---|---|---|---|
| 1 | Nested catalogue ingestion | Read nested title/season/episode JSON with explicit schemas; flatten titles and episodes; create genre, language and cast bridges; standardise genres. | `dim_content`, `content_genre`, `content_language`, `content_cast` | Harshvardhan |
| 2 | Playback cleaning and device clocks | Deduplicate `event_id`; estimate per-device offset from online events; correct timestamps robustly; preserve offline events; quantify daily watch-hour impact. | `playback_clean` and clock-correction impact report | Rutik |
| 3 | CDN log parsing | Parse both providers' formats; standardise fields; map edges to regions; quarantine malformed lines; aggregate bandwidth. | Bronze `cdn_requests`, Silver `cdn_usage_hourly`, quarantine counts | Siddhi |
| 4 | Incremental playback ingestion | File-source Structured Streaming, `availableNow`, Delta partitioning, checkpoints, repeat-run idempotency and additive `cast_device` schema support. | Incremental Bronze playback table and checkpoint/evidence | Ishan |
| 5 | Viewing sessions and watch time | Lag-based heartbeat intervals; exclude pauses and skipped seeks; cap implausibly long viewing; sessionise; calculate completion. | `viewing_sessions`, `watch_time_daily` | Rutik |
| 6 | Subscription lifecycle | Deduplicate and sequence billing events; derive trial/paid/paused/grace/churn periods; handle seven-day recovery; reconcile settlements. | `subscription_periods`, `subscribers_daily` | Kshitij |
| 7 | Search analytics and content gaps | Normalise and cluster similar queries; measure search-to-play conversion; distinguish typos from missing catalogue titles. | `search_clean`, `search_conversion`, weekly content-gap list | Siddhi |
| 8 | Tier and licence history | Build SCD Type 2 content history; point-in-time tier/partner joins; detect regional licence violations; query historical catalogue state. | SCD2 content dimension and licence-violation report | Harshvardhan |
| 9 | Active users | Define one-minute active threshold; compute DAU/WAU/MAU, stickiness, exact vs approximate counts and dimensional breakdowns. | `active_users_daily` | Ankita |
| 10 | Regional Top 10 | Roll episodes to series; rank by distinct viewers and watch-hours; separate kids list; minimum state audience with national fallback; rank movement. | `top10_daily` | Harshvardhan |

## Questions 11–20

| Q | Topic | Main work | Expected output | Owner |
|---|---|---|---|---|
| 11 | Subscriber cohorts and MRR | First-paid-month cohorts; retention matrix; monthly recurring revenue bridge for new, expansion, contraction, churn and reactivation; channel comparisons. | `cohort_retention`, `mrr_bridge_monthly` | Kshitij |
| 12 | Account sharing | Concurrent-session/device counts; monthly city/device footprint; compare concurrency with plan limits; estimate additional-household conversion value. | `account_sharing_flags`, revenue estimate | Rutik |
| 13 | Advertising delivery and revenue | Fill rate, impressions, completion, revenue, frequency-cap violations/credits, targeting compliance and campaign pacing. | `ad_delivery_daily`, `frequency_violations`, pacing report | Ankita |
| 14 | Live-cricket data skew | Quantify hot-key skew; compare baseline, AQE and salting; test date/hour partitioning and Z-ordering; calculate CDN costs. | Performance comparison and `cdn_cost_by_title` | Rutik |
| 15 | Content-partner royalties | Allocate premium subscription pool by partner watch share; add free-tier ad revenue; apply quarterly guarantees; reconcile to the paisa. | `partner_royalties_quarterly`, partner statements | Kshitij |
| 16 | Collaborative-filtering recommendations | Build implicit feedback; train MLlib ALS; hold out recent titles; evaluate precision@10; filter already-watched, unlicensed and adult-for-kids recommendations; compare popularity baseline. | Recommendations and evaluation report | Harshvardhan |
| 17 | Real-time trending | Event-time 10-minute windows sliding every minute; watermark; same-hour prior-seven-day baseline; growth ratio, threshold and top 20 by language. | `trending_now` | Ishan |
| 18 | Live ad pacing and caps | Stateful per-user/campaign/day impression counts; suppress capped users; compare spend with ideal pace; reset by date. | `capped_users_live`, `pacing_alerts` | Kshitij |
| 19 | Data quality and kids safety | Detect age-rating violations; profile anomalies; watch-time bounds; subscription/royalty reconciliation; published-metric drift; automated tests. | Quality report, exception tables and tests | Ankita |
| 20 | Churn prediction and pipeline integration | Define seven-day renewal outcomes; build pre-anchor features; time split; train MLlib classifier; evaluate AUC; rank by risk × plan value; orchestrate ingestion to scoring. | Churn model/report, prioritised risk scores, `src/streambharat/pipeline.py` | Ishan (Rutik technical support) |

## Dependency order

The project is designed to be implemented in dependency order rather than as 20 isolated scripts:

1. Q1 catalogue model and Q2 corrected playback.
2. Q3 CDN parsing and Q4 incremental ingestion.
3. Q5 viewing sessions; Q6 subscription periods; Q7 search cleaning; Q8 content SCD2/licence history.
4. Q9/Q10 engagement and rankings; Q11/Q12 finance and sharing; Q13/Q14 advertising/CDN analytics; Q15/Q16 royalties/recommendations.
5. Q17/Q18 live streams; Q19 quality gates; Q20 churn and integrated pipeline.

Some modules can be developed in parallel once their input/output contracts are agreed. Q17 requires its simulator and content enrichment; Q20 should use Q2's corrected Silver playback table for final feature quality.
