# StreamBharat Dataset

Data for **Project 4 — StreamBharat: OTT Streaming Analytics Platform** in the
"PySpark Industry Projects" workbook. Every student gets these exact files.

**Sample:** 2,500 accounts (4,736 profiles), a scaled-down sample of the platform.
**Periods:** subscriptions May 2025 – October 2026; playback, search and ads July – October 2026;
CDN logs September 2026 (the month of the cricket final).
**Size:** about 590 MB unzipped; 1.6 million player events.
**Warning:** this data imitates real systems, problems included. Finding and handling them is part
of the project. Do not edit raw files by hand.

**Note on heartbeats:** to keep the data a manageable size, the player sends a heartbeat every
**5 minutes** (300 seconds) while playing, not every 30 seconds. One heartbeat = up to 5 minutes watched.

## Quick start

1. Unzip so you have `StreamBharat_dataset/data/raw/...`.
2. Put the `data/` folder inside your `streambharat/` project folder.
3. For Q17 and Q18, run `python ott_stream_simulator.py --stream plays` (or `ads`)
   in a second terminal from the project folder while your streaming job runs.

## Files

| Path | Format | Rows | Used in |
| --- | --- | --- | --- |
| content/catalog_YYYY-MM-01.json | Nested JSON array, monthly snapshots May–Oct 2026 | 389 titles, 740 episodes | Q1, Q8 |
| content/licences.csv | CSV | 4,668 | Q8, Q16 |
| player/playback_YYYY-MM-DD.json | JSON lines, **one file per server-arrival day** | 1,621,902 | Q2, Q4, Q5, Q9–Q12, Q14 |
| billing/subscription_events.json | JSON lines | 20,033 | Q6, Q11, Q20 |
| billing/gateway_settlements.csv | CSV | 16,342 | Q6, Q11, Q15 |
| app/users.json, app/profiles.json | JSON lines | 2,500 / 4,736 | many |
| app/ratings.json | JSON lines | 4,274 | Q16 |
| search/search_logs.json | JSON lines | 43,271 | Q7 |
| ads/ad_campaigns.csv | CSV | 12 | Q13, Q18 |
| ads/ad_breaks.csv | CSV, one row per ad break served to a free user | 49,953 | Q13 |
| ads/ad_impressions.csv | CSV | 41,981 | Q13, Q15 |
| cdn/akashcdn_2026-09.log | Raw access log (provider 1) | 166,384 | Q3, Q14 |
| cdn/nimbusedge_2026-09.tsv | Tab-separated log (provider 2) | 166,682 | Q3, Q14 |
| reference/*.csv | CSV | small | many |
| ott_stream_simulator.py | Python script | live | Q17, Q18 |

## Columns

**catalog** — content_id, title, type (movie/series/live), languages[], genres[], cast[{name, role}], release_date,
age_rating (U/7+/13+/16+/18+), partner_id, tier (free/premium), duration_sec (movies and live);
series instead have `seasons[{season, episodes[{content_id, episode_no, title, duration_sec, release_date}]}]`.

**licences** — content_id, region (state), licence_start, licence_end

**playback events**

```json
{"event_id": "E0000393287", "user_id": "U000105", "profile_id": "U000105-P1", "device_id": "DV47175420",
 "content_id": "CNT00976", "event_type": "start", "position_sec": 0,
 "client_ts": "2026-08-04T20:32:16", "server_ts": "2026-08-10T00:32:17", "ip_city": "Pune",
 "device_type": "android", "app_version": "7.0.4", "download_id": "DL522218403"}
```

event_type: start, heartbeat, pause, seek, stop, complete. `client_ts` is the device clock (can be wrong);
`server_ts` is when the event reached the server. `download_id` is set for offline (downloaded) playback,
whose events are uploaded days later. Episodes have their own content_id; the catalog maps them to their series.

**subscription_events** — user_id, event_type (trial_start/subscribe/renew/upgrade/downgrade/pause/resume/cancel/payment_failed),
plan (basic/premium), price (INR, only on charged events), event_ts, gateway
**gateway_settlements** — gateway, user_id, amount_inr, settled_ts, event_type
**users** — user_id, signup_ts, city, state, age_band, acquisition_channel
**profiles** — profile_id, user_id, is_kids
**ratings** — profile_id, content_id, rating (1–5), ts
**search_logs** — search_id, profile_id, query_text, results_count, clicked_content_id, ts
**ad_campaigns** — campaign_id, advertiser, budget_inr, start_date, end_date, daily_frequency_cap (per user per day),
target_genres (`|`-separated, or ALL), cpm_inr
**ad_breaks** — break_id, user_id, profile_id, content_id, break_ts, filled (1/0), impression_id
**ad_impressions** — impression_id, user_id, profile_id, content_id, campaign_id, ad_break_ts, duration_sec, completed (1/0), cpm_inr

**CDN logs** — one line per video request.
Provider 1 (AkashCDN), space-separated with a quoted request:

```
2026-09-01T00:04:59Z edge-pnq-01 38842c74 "GET /v/CNT00024/720p/seg_00350.ts HTTP/2" 200 90082159 "StreamBharat-android/7.2.1"
```

Provider 2 (NimbusEdge), tab-separated: epoch_seconds, pop, client_hash, path (`vod/{content_id}/{quality}/{segment}`), status, bytes, user_agent

```
1788221077	POP_DEL-02	34c797df	vod/CNT00232/480p/100	200	36288252	StreamBharat-android/7.3.0
```

**reference/**
- content_partners.csv — partner_id, name, revenue_share_pct, minimum_guarantee_inr_per_quarter
- cdn_rates.csv — cdn, region, rate_inr_per_gb
- cdn_edges.csv — edge, pop_code, region (maps both providers' edge names to regions)
- genre_mapping.csv — raw_genre, standard_genre
- city_state.csv — city, state (to compare viewing location with licence regions)
- plans.csv — plan, price_inr_per_month, max_screens
- published_metrics.csv — month, metric, value, published_ts (numbers already sent to the board)

**Streams (from the simulator)**
- `data/stream/plays/` — event_id, user_id, profile_id, content_id, language, event_ts
- `data/stream/ads/` — impression_id, user_id, campaign_id, ad_break_ts, duration_sec, cpm_inr

## Notes

- All titles, people, brands and advertisers are fictional.
- Amounts are in Indian rupees.
