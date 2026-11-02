# Q20 — Churn model and pipeline integration

## Objective

Predict accounts at risk of cancelling, evaluate with a chronological split and rank outreach candidates by churn risk multiplied by plan value.

## Implementation

Module: `src/streambharat/q20_churn.py`

Orchestration: `src/streambharat/pipeline.py`

Default inputs/outputs:

- Playback: `data/delta/bronze/playback/` (standalone fallback)
- Preferred playback: Q2 cleaned Silver Delta path via `--playback`
- Subscription events: `data/raw/billing/subscription_events.json`
- Users: `data/raw/app/users.json`
- Model: `artifacts/models/q20_churn_logreg/`
- Gold risk scores: `data/delta/gold/churn_scores/`
- Evaluation report: `artifacts/reports/q20_metrics.json`
- Ranking cap: 50,000 accounts

## Labels and observation window

- A subscription cycle is modelled as 30 days plus a seven-day grace period (37 days total), consistent with the monthly pattern in the supplied subscription history.
- A successful renewal inside the grace period is retained (0).
- A voluntary cancellation is churn (1).
- A payment failure not recovered by a renewal within seven days is involuntary churn (1).
- A failed payment recovered within seven days is not churn.
- Cycles without a fully observed 37-day outcome by the chosen `--as-of` date are excluded as right-censored.
- Training snapshots start from 1 July 2026, the beginning of available playback history.

## Features

Features are calculated strictly before each snapshot's anchor timestamp. They include current and preceding 28-day watch minutes, active days in both periods, relative watch decline, payment failures in the preceding 90 days, prior renewals, account tenure, monthly plan price and premium-plan indicator. Five minutes are attributed per heartbeat, reflecting the course sample's five-minute cadence. A completion-only user/day receives one five-minute fallback only when no heartbeat exists; completion is not double-counted when heartbeats are present. Offline playback is excluded from the fallback calculation because server arrival may be days after the viewing timestamp.

For final integration, use Q2's corrected Silver event timestamp and cleaned events. The Bronze `server_ts` path is for standalone execution only and is less accurate for offline or delayed events.

## Model and evaluation

- Spark MLlib Logistic Regression.
- Chronological train/test split, not a random split.
- Report ROC-AUC and PR-AUC when both classes exist in the holdout; include precision/recall at the default threshold.
- Refit the final model on all matured labelled snapshots.
- Score eligible active accounts and rank by `churn_probability × monthly_plan_price_inr`.

## Run

```bash
python -m streambharat.q20_churn --as-of 2026-10-31
```

Preferred integrated invocation:

```bash
python -m streambharat.q20_churn \
  --playback data/delta/silver/playback_clean \
  --as-of 2026-10-31
```

Run the batch pipeline:

```bash
python -m streambharat.pipeline --as-of 2026-10-31
```

## Validation and reporting

The package previously checked the raw subscription label rules and observed approximately 4,170 matured snapshots (3,725 retained and 445 churn) for `--as-of 2026-10-31`. These are label-construction counts, not model metrics. Metrics must be taken from `artifacts/reports/q20_metrics.json` after Spark execution; do not invent model performance values.

## Integration responsibilities

Ishan owns pipeline integration and final delivery; Rutik provides technical support. The remaining required integration stages include Q2 Silver playback, Q5 engagement features, common quality gates, Gold table contracts and final execution evidence.
