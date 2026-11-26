"""Restartable July–September playback backfill into canonical Silver player data."""
from __future__ import annotations

import argparse
import os
import re
from datetime import date
from pathlib import Path

from delta.tables import DeltaTable
from pyspark.sql import functions as F

from streambharat.silver_player import PLAYER_SCHEMA, merge_silver, transform_player_events
from streambharat.spark_session import create_spark


FILE_PATTERN = re.compile(r"playback_(\d{4}-\d{2}-\d{2})\.json$")


def select_arrival_files(data_root: str, start_date: str, end_date: str) -> list[Path]:
    """Select playback files by their server-arrival date, not viewing date."""
    start = date.fromisoformat(start_date)
    end = date.fromisoformat(end_date)
    if end < start:
        raise ValueError("end_date must be on or after start_date")
    folder = Path(data_root) / "player"
    selected = []
    for path in sorted(folder.glob("playback_*.json")):
        match = FILE_PATTERN.fullmatch(path.name)
        if not match:
            continue
        arrival_day = date.fromisoformat(match.group(1))
        if start <= arrival_day <= end:
            selected.append(path)
    if not selected:
        raise FileNotFoundError(
            f"No playback_YYYY-MM-DD.json arrival files found in {folder} "
            f"for {start_date} through {end_date}"
        )
    return selected


def _read_arrival_file(spark, path: Path):
    """Read one file day, preserving its source identity and server arrival time."""
    events = (
        spark.read.schema(PLAYER_SCHEMA)
        .json(str(path))
        .withColumn("source_file", F.lit(path.name))
        .withColumn("source_system", F.lit("batch"))
        .withColumn("source_topic", F.lit(None).cast("string"))
        .withColumn("kafka_partition", F.lit(None).cast("int"))
        .withColumn("kafka_offset", F.lit(None).cast("long"))
        .withColumn("kafka_timestamp", F.lit(None).cast("timestamp"))
        .withColumn("ingested_at", F.to_timestamp("server_ts"))
    )
    payload_columns = [field.name for field in PLAYER_SCHEMA.fields]
    return events.withColumn(
        "raw_value",
        F.to_json(F.struct(*[F.col(column) for column in payload_columns])),
    )


def _compute_backfill_offsets(spark, files: list[Path]):
    """Use all selected arrival days for a stable per-device median clock offset."""
    history = spark.read.schema(PLAYER_SCHEMA).json([str(path) for path in files])
    online = history.filter(
        (F.col("download_id").isNull()) | (F.trim("download_id") == "")
    ).filter(
        F.col("device_id").isNotNull()
        & F.to_timestamp("client_ts").isNotNull()
        & F.to_timestamp("server_ts").isNotNull()
    )
    return (
        online.withColumn(
            "offset_seconds",
            F.unix_timestamp(F.to_timestamp("server_ts"))
            - F.unix_timestamp(F.to_timestamp("client_ts")),
        )
        .groupBy("device_id")
        .agg(
            F.expr("percentile_approx(offset_seconds, 0.5)")
            .alias("median_clock_offset_seconds"),
            F.count(F.lit(1)).cast("long").alias("offset_sample_count"),
        )
    )


def build_player_reconciliation(silver):
    """Summarise viewing dates against server-arrival dates and source systems."""
    dated = silver.filter(
        F.col("event_date").isNotNull() & F.col("arrival_date").isNotNull()
    )
    daily = (
        dated.groupBy("event_date", "arrival_date", "source_system")
        .agg(
            F.count(F.lit(1)).cast("long").alias("event_count"),
            F.countDistinct("event_id").cast("long").alias("distinct_event_count"),
            F.countDistinct("user_id").cast("long").alias("users"),
        )
        .withColumn(
            "arrival_lag_days",
            F.datediff("arrival_date", "event_date"),
        )
        .withColumn(
            "is_late_arrival",
            F.col("arrival_date") > F.col("event_date"),
        )
    )
    september_to_october = (
        dated.filter(
            (F.col("event_date") >= F.lit("2026-09-01").cast("date"))
            & (F.col("event_date") < F.lit("2026-10-01").cast("date"))
            & (F.col("arrival_date") >= F.lit("2026-10-01").cast("date"))
            & (F.col("arrival_date") < F.lit("2026-11-01").cast("date"))
        )
        .groupBy("event_date", "arrival_date", "source_system")
        .agg(
            F.count(F.lit(1)).cast("long").alias("september_viewing_events_arrived_in_october"),
            F.countDistinct("event_id").cast("long").alias("distinct_events"),
            F.countDistinct("user_id").cast("long").alias("affected_users"),
        )
    )
    return daily, september_to_october


def run_backfill(
    data_root: str = "data/raw",
    start_date: str = "2026-07-01",
    end_date: str = "2026-09-30",
    silver_path: str = "lake/silver/player_events",
    offsets_path: str = "lake/silver/device_clock_offsets_backfill",
    reconciliation_path: str = "lake/gold/recon_player_daily",
    september_late_path: str = "lake/gold/recon_september_arrivals",
    manifest_path: str = "lake/silver/player_backfill_files",
) -> dict[str, int]:
    """MERGE file-day batches by event_id; rerunning any day does not duplicate rows."""
    files = select_arrival_files(data_root, start_date, end_date)
    spark = create_spark("StreamBharat-Player-Historical-Backfill")
    counts: dict[str, int] = {}
    try:
        offsets = _compute_backfill_offsets(spark, files)
        offsets.write.format("delta").mode("overwrite").option(
            "overwriteSchema", "true"
        ).save(offsets_path)

        completed = set()
        if DeltaTable.isDeltaTable(spark, manifest_path):
            completed = {
                row["source_file"]
                for row in spark.read.format("delta").load(manifest_path).select("source_file").collect()
            }

        for path in files:
            if path.name in completed:
                print(f"Backfill already complete; skipping {path.name}")
                continue
            raw = _read_arrival_file(spark, path)
            conformed, _ = transform_player_events(raw, device_offsets=offsets)
            valid = conformed.filter(
                F.col("event_id").isNotNull()
                & F.col("event_ts_corrected").isNotNull()
            )
            merge_silver(spark, valid, silver_path)
            counts[path.name] = raw.count()
            manifest_row = spark.createDataFrame(
                [(path.name, str(path), counts[path.name])],
                ["source_file", "source_path", "source_rows"],
            ).withColumn("completed_at", F.current_timestamp())
            if DeltaTable.isDeltaTable(spark, manifest_path):
                (
                    DeltaTable.forPath(spark, manifest_path).alias("target")
                    .merge(manifest_row.alias("source"), "target.source_file = source.source_file")
                    .whenMatchedUpdateAll()
                    .whenNotMatchedInsertAll()
                    .execute()
                )
            else:
                manifest_row.write.format("delta").mode("append").save(manifest_path)
            print(f"Backfill merged {path.name}: {counts[path.name]:,} source records")

        silver = spark.read.format("delta").load(silver_path)
        daily, september_to_october = build_player_reconciliation(silver)
        daily.write.format("delta").mode("overwrite").option(
            "overwriteSchema", "true"
        ).save(reconciliation_path)
        september_to_october.write.format("delta").mode("overwrite").option(
            "overwriteSchema", "true"
        ).save(september_late_path)

        late_summary = september_to_october.agg(
            F.sum("september_viewing_events_arrived_in_october").alias("events"),
            F.sum("affected_users").alias("user_day_count"),
        ).first()
        print(
            "September viewing events arriving in October: "
            f"{int(late_summary['events'] or 0):,}"
        )
    finally:
        spark.stop()
    return counts


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Backfill July–September playback files by server-arrival day"
    )
    parser.add_argument("--data-root", default="data/raw")
    parser.add_argument("--start-date", default="2026-07-01")
    parser.add_argument("--end-date", default="2026-09-30")
    parser.add_argument("--silver", default="lake/silver/player_events")
    parser.add_argument("--offsets", default="lake/silver/device_clock_offsets_backfill")
    parser.add_argument("--reconciliation", default="lake/gold/recon_player_daily")
    parser.add_argument("--september-late", default="lake/gold/recon_september_arrivals")
    parser.add_argument("--manifest", default="lake/silver/player_backfill_files")
    args = parser.parse_args()
    run_backfill(
        args.data_root, args.start_date, args.end_date, args.silver,
        args.offsets, args.reconciliation, args.september_late, args.manifest,
    )


if __name__ == "__main__":
    main()
