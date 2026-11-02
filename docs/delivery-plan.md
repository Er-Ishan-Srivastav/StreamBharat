# Delivery plan, interfaces and completion gates

## Shared conventions

- All paths below are repository-relative and should be configurable through CLI arguments or a small project configuration file.
- Use lowercase snake_case for column names and explicit types.
- Preserve source identifiers (`event_id`, `user_id`, `profile_id`, `content_id`) across layers.
- Raw source files are immutable; quarantine invalid rows rather than silently dropping them.
- Use Delta for persistent Bronze/Silver/Gold outputs where supported by the assignment.
- Each module must document its run command, output path, schema, assumptions, and validation evidence.

## Cross-team data contracts

| Producer | Contract | Consumers | Required fields / notes |
|---|---|---|---|
| Q1 — catalogue | `dim_content` and bridge tables | Q5, Q8, Q10, Q13–Q17, Q19 | Title/episode IDs, parent series, type, duration, language, genres, age rating, partner and tier |
| Q2 — playback cleaning | `playback_clean` | Q5, Q9–Q12, Q14, Q20 | Deduplicated event, `event_ts_corrected`, `is_offline`, device/profile/user/content, event type and city |
| Q4 — incremental ingest | `data/delta/bronze/playback/` | Q2, Q20 fallback, Q17 baseline | Bronze source records with event IDs and ingestion lineage; includes nullable `cast_device` |
| Q5 — engagement | `viewing_sessions`, `watch_time_daily` | Q9–Q16, Q20 | Profile/user/content, viewing date, watched minutes, completion, session boundaries |
| Q6 — subscriptions | `subscription_periods`, `subscribers_daily` | Q9, Q11–Q13, Q15, Q20 | User, plan, status, valid period, monthly price |
| Q8 — content history | `dim_content_scd2`, licence validity | Q15, Q16, Q19 | Effective dates, historical partner/tier and region licence intervals |
| Q13 — ads | `ad_delivery_daily`, `frequency_violations`, `ad_revenue_by_title_day` | Q15, Q19 | Platform/day fill rate (break data has no campaign ID for unfilled breaks), campaign/day delivery, revenue, credits, genre-target compliance |
| Q14 — skew/optimization | `q14_performance/`, `cdn_cost_by_title/` | Platform analysis | Hot-key diagnosis, AQE/salting comparison, physical layout timing and provider/region rates |
| Q16 — recommendations | `data/delta/gold/recommendations/` | Discovery output | Profile, user, content, score and rank; licence/kids restrictions; evaluation against held-out titles |
| Q17 — trending | `data/delta/gold/trending/` | Demo / product output | Window start/end, language, rank, title/content ID, viewers, baseline and growth ratio |
| Q18 — live ad pacing | `capped_users_live`, `pacing_alerts_live` | Live ad-serving demo | Unique impression ledger, per-user/campaign/day cap state and spend pace |
| Q20 — churn | `data/delta/gold/churn_scores/` | Retention prioritisation | User, prediction timestamp, churn probability, risk value, rank |

**Q20 integration preference:** pass the Q2 Silver playback path using `--playback` and expose `event_ts_corrected` (or `event_ts`). The Bronze `server_ts` fallback is provided for standalone development but is less accurate for offline and delayed events.

## Suggested milestone sequence

1. **Foundation:** repository conventions, dataset placement, shared Spark session, schemas and local tests.
2. **Core ingestion/cleaning:** Q1–Q4; agree on catalogue and playback contracts.
3. **Business facts:** Q5–Q8; publish session/watch-time and subscription tables.
4. **Analytics:** Q9–Q16; validate audience, search, content, finance, advertising, CDN and recommendations.
5. **Streaming and quality:** Q17–Q19; run simulator demos, isolate checkpoints and enforce checks.
6. **Integration/release:** Q20, end-to-end orchestration, regression validation, docs and demo evidence.

The repository's finite orchestrator is `python -m streambharat.pipeline`. It runs Q1–Q16, Q19 and Q20 in dependency order. Q17 and Q18 are live streams and must be launched separately with the supplied simulator.

## Definition of done for each question

- Code imports and executes in the agreed environment.
- The expected output path exists and schema matches the documented contract.
- Counts, keys, nulls and duplicate handling are checked.
- Business-specific edge cases in the question are covered by deterministic tests or reconciliation queries.
- The owner can explain design choices, trade-offs and limitations.
- Run command, sample result and known limitations are documented.

## Release validation checklist

- [ ] Dataset README reviewed; raw files remain unchanged.
- [ ] All module owners confirm stable input/output contracts.
- [ ] Q4 second run with unchanged checkpoint processes no new source files.
- [ ] Q2 timestamp correction validated against client/server and offline examples.
- [ ] Q5 watch-time caps, seek handling and session boundaries validated.
- [ ] Q6 payment-retry logic reconciled to gateway settlements.
- [ ] Gold finance/ads/royalty totals reconcile within the defined rounding policy.
- [ ] Q16/Q20 evaluation uses time-aware holdout and no future leakage.
- [ ] Q17/Q18 streaming jobs have unique stable checkpoints and late-event tests.
- [ ] Q19 quality gates and kids-safety tests execute in CI.
- [ ] Full batch pipeline and demonstration run end-to-end.
- [ ] README and presentation/demo evidence reflect actual measured results.
