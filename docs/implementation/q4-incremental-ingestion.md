# Q4 — Incremental playback ingestion

## Objective

Process only newly arrived playback files, write them to Bronze Delta, resume safely through checkpoints and accept the additive `cast_device` field.

## Implementation

Module: `src/streambharat/q4_incremental_ingestion.py`

Default paths:

- Source: `data/raw/player/`
- Bronze Delta: `data/delta/bronze/playback/`
- Checkpoint: `data/checkpoints/q4_playback/`

The job reads JSONL using an explicit schema and `maxFilesPerTrigger`, attaches `_source_file` and `_ingested_at`, and uses `trigger(availableNow=True)` for a finite incremental run. The `foreachBatch` writer deduplicates within a micro-batch and merges into Delta on `event_id`. The schema includes nullable `cast_device`; an additive migration adds it to an existing Delta table if needed. Keep the source and target schemas aligned during upgrades; verify additive-column merge behaviour on the team's pinned Delta runtime before a full historical backfill.

## Run and verify

```bash
python -m streambharat.q4_incremental_ingestion
```

Run it again with the same source directory and checkpoint. The second run should discover no new files and should not add duplicate events. Preserve the checkpoint between normal restarts; deleting it resets file-source progress.

## Validation evidence to record

- File count discovered and rows ingested on the first run.
- New files and rows processed on the second run (expected: zero if no files were added).
- `event_id` duplicate count in Bronze (expected: zero after merge/deduplication).
- Bronze schema before and after an input containing `cast_device`.
- Checkpoint path and Spark/Delta versions.

## Limitations / integration

Spark/Delta runtime validation is pending in the packaged environment. The job's schema is explicit and should be checked against the supplied JSONL. Q2 owns timestamp correction; Q4 does not replace Q2's Silver cleansing logic.
