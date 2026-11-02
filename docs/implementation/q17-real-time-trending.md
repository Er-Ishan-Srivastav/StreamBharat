# Q17 — Real-time trending

## Objective

Identify titles whose audience is rising quickly relative to the same-hour seven-day baseline, rather than merely listing the most watched titles.

## Implementation

Module: `src/streambharat/q17_trending.py`

Input and outputs:

- Simulator input: `data/stream/plays/`
- Bronze playback history: `data/delta/bronze/playback/`
- Catalogue snapshot: `data/raw/content/catalog_2026-10-01.json`
- Baseline: `data/delta/reference/trending_baseline/`
- Live buffer: `data/delta/silver/trending_event_buffer/`
- Gold output: `data/delta/gold/trending/`
- Checkpoint: `data/checkpoints/q17_trending/`

Windowing and ranking:

- Event-time window: 10 minutes.
- Slide interval: 1 minute.
- Late-arrival allowance: 15 minutes.
- Active buffer horizon: 25 minutes (window plus late allowance).
- Minimum audience: 200 distinct profiles.
- Output: top 20 per language and window, ranked by growth ratio and then distinct viewers.

The stream uses `foreachBatch` and a bounded Delta event buffer to recalculate exact distinct profile counts for eligible windows. This is intentionally a bounded batch aggregation rather than a native streaming exact `countDistinct` operation. The seven-day baseline uses online `start` events and the primary language in the catalogue; its hour-level unique-viewer average is normalised to a 10-minute comparison rate by dividing by six.

## Run

Terminal A:

```bash
python ott_stream_simulator.py --stream plays
```

Terminal B:

```bash
python -m streambharat.q17_trending --as-of 2026-11-02
```

Stop both processes with Ctrl+C after the demo.

## Validation evidence to record

- Baseline population date range and completeness.
- Event-time window and late-arrival behaviour.
- One known surge from the simulator and its expected top-ranking period.
- At least one window with fewer than 200 viewers filtered out.
- No more than 20 rows per language/window after ranking.
- Corrected ranking when late events revise an existing window.

## Assumptions

- The simulator's `start` records are online starts; offline history is excluded from the baseline by the `download_id` rule.
- The baseline's hourly-to-10-minute conversion assumes a uniform within-hour rate. This is a documented approximation, not a measured 10-minute historical baseline.
- Spark/Delta runtime validation remains pending.
