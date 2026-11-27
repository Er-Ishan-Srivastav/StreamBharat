"""Shared Bronze-to-DLQ offset exclusion helper."""
from __future__ import annotations

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, functions as F


def exclude_dlq_offsets(
    frame: DataFrame,
    source_topic: str,
    dlq_path: str = "lake/bronze/dlq",
) -> DataFrame:
    """Remove source offsets already classified as invalid by the generic DLQ job."""
    required = {"kafka_partition", "kafka_offset"}
    if not required.issubset(frame.columns):
        raise ValueError(
            f"Bronze frame for {source_topic} lacks Kafka offset identity columns"
        )
    if not DeltaTable.isDeltaTable(frame.sparkSession, dlq_path):
        return frame
    dlq = frame.sparkSession.read.format("delta").load(dlq_path)
    invalid = (
        dlq.filter(F.col("source_topic") == source_topic)
        .select("kafka_partition", "kafka_offset")
        .dropDuplicates()
    )
    return frame.join(invalid, ["kafka_partition", "kafka_offset"], "left_anti")
