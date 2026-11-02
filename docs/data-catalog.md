# Dataset catalogue

The dataset is supplied separately as `StreamBharat_dataset.zip`. The complete student-facing dataset guide is copied to `source-materials/StreamBharat-Dataset-README.md`. This page records its core inventory and conventions for repository contributors.

## Sample profile

- 2,500 user accounts and 4,736 profiles.
- Subscription history: May 2025–October 2026.
- Playback, search and advertising: July–October 2026.
- CDN access logs: September 2026.
- Approximately 1.62 million player events; approximately 590 MB uncompressed.
- Heartbeat cadence is five minutes (300 seconds) in this course sample; do not assume the production scenario's 30-second cadence.
- Data entities are synthetic; currency is INR.

## Input inventory

| Path under `data/raw/` | Format / key fields | Approx. volume | Questions |
|---|---|---:|---|
| `content/catalog_YYYY-MM-01.json` | Monthly nested JSON; title, languages, genres, cast, seasons/episodes, release date, age rating, partner, tier | 389 titles, 740 episodes | Q1, Q8, Q10, Q15, Q16, Q17, Q19 |
| `content/licences.csv` | `content_id`, state/region, start/end dates | 4,668 rows | Q8, Q16 |
| `player/playback_YYYY-MM-DD.json` | JSONL; event/user/profile/device/content IDs, event type, position, client/server timestamps, city, device, app version, download ID | 1,621,902 events | Q2, Q4, Q5, Q9–Q12, Q14, Q20 |
| `billing/subscription_events.json` | JSONL; user, event type, plan, price, event timestamp, gateway | 20,033 events | Q6, Q11, Q20 |
| `billing/gateway_settlements.csv` | Gateway, user, amount, settlement timestamp, event type | 16,342 rows | Q6, Q11, Q15 |
| `app/users.json`, `app/profiles.json` | User and profile dimensions, city/state, signup, acquisition channel, kids flag | 2,500 / 4,736 | Many |
| `app/ratings.json` | `profile_id`, `content_id`, rating 1–5, timestamp | 4,274 rows | Q16 |
| `search/search_logs.json` | Query, result count, clicked content, profile, timestamp | 43,271 rows | Q7 |
| `ads/ad_campaigns.csv` | Budget, campaign dates, daily frequency cap, target genres, CPM | 12 campaigns | Q13, Q18 |
| `ads/ad_breaks.csv` | One row per free-user break, fill and impression IDs | 49,953 rows | Q13 |
| `ads/ad_impressions.csv` | Impression, user/profile/content/campaign, timestamp, completion, CPM | 41,981 rows | Q13, Q15, Q18 |
| `cdn/akashcdn_2026-09.log` | Provider-specific space-separated access log | 166,384 lines | Q3, Q14 |
| `cdn/nimbusedge_2026-09.tsv` | Provider-specific tab-separated access log | 166,682 lines | Q3, Q14 |
| `reference/*.csv` | Partners, CDN rates/edges, genre map, city/state, plans, published metrics | Small | Many |

## Important data conventions

- **Playback file date is server-arrival date**, not necessarily event/viewing date. There are one-file-per-arrival-day player JSONL files.
- `client_ts` is device time and may be wrong; `server_ts` is arrival time. Offline events have `download_id` and may upload days after viewing.
- Deduplicate playback by `event_id` before downstream engagement calculations.
- One heartbeat represents up to five minutes of viewing in this sample. Forward seeks are skipped content, not watch time.
- Series episodes have separate content IDs; the catalogue supplies the parent series relationship.
- Subscription events include `trial_start`, `subscribe`, `renew`, `upgrade`, `downgrade`, `pause`, `resume`, `cancel` and `payment_failed`.
- `ad_campaigns.target_genres` is `|`-separated or `ALL`.
- CDN providers use different log formats and edge names; `reference/cdn_edges.csv` maps provider edges/POPs to regions.
- `reference/published_metrics.csv` contains previously published board metrics for comparison; do not overwrite it.

## Reference files

`reference/` contains `content_partners.csv`, `cdn_rates.csv`, `cdn_edges.csv`, `genre_mapping.csv`, `city_state.csv`, `plans.csv` and `published_metrics.csv`.

## Local placement

Extract the supplied dataset archive, then copy its `data/` directory into the repository root. The raw files remain untracked through `.gitignore`; commit schemas, transformation code, documentation, tests and reproducible configuration instead.
