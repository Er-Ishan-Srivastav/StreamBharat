"""Reconcile Bronze record counts against Kafka offset ranges per partition."""
from __future__ import annotations

import argparse
import os
from pyspark.sql import functions as F

from .spark_session import create_spark
from .bronze_ingest import SOURCE_TOPICS


def read_offset_ranges(bootstrap_servers: str, topics: list[str]):
    from confluent_kafka import Consumer, TopicPartition
    consumer = Consumer({
        "bootstrap.servers": bootstrap_servers,
        "group.id": "streambharat-offset-audit",
        "enable.auto.commit": False,
    })
    metadata = consumer.list_topics(timeout=20)
    rows = []
    try:
        for topic in topics:
            topic_meta = metadata.topics.get(topic)
            if topic_meta is None:
                continue
            for partition_id in topic_meta.partitions:
                low, high = consumer.get_watermark_offsets(
                    TopicPartition(topic, partition_id), timeout=20, cached=False
                )
                rows.append((topic, partition_id, low, high, max(high-low, 0)))
    finally:
        consumer.close()
    return rows


def reconcile(bootstrap_servers="localhost:9092",
              bronze_root="lake/bronze",
              output_path="lake/gold/bronze_offset_reconciliation",
              topics=None):
    selected = topics or list(SOURCE_TOPICS)
    offset_rows = read_offset_ranges(
        bootstrap_servers, [SOURCE_TOPICS[name] for name in selected]
    )
    spark = create_spark("StreamBharat-Bronze-Offset-Reconciliation")
    try:
        ranges = spark.createDataFrame(
            offset_rows, ["source_topic","kafka_partition","low_offset","high_offset","retained_messages"]
        )
        counts=[]
        for alias in selected:
            path=os.path.join(bronze_root,alias)
            if os.path.exists(path):
                counts.append(
                    spark.read.format("delta").load(path)
                    .groupBy("source_topic","kafka_partition")
                    .agg(F.count(F.lit(1)).alias("bronze_rows"))
                )
        if counts:
            observed=counts[0]
            for frame in counts[1:]:
                observed=observed.unionByName(frame)
        else:
            observed=spark.createDataFrame([], "source_topic string, kafka_partition int, bronze_rows long")
        report=(ranges.join(observed,["source_topic","kafka_partition"],"left")
                .fillna({"bronze_rows":0})
                .withColumn("count_delta",F.col("bronze_rows")-F.col("retained_messages"))
                .withColumn("reconciled",F.col("count_delta")==0)
                .withColumn("checked_at",F.current_timestamp()))
        report.write.format("delta").mode("overwrite").save(output_path)
        report.orderBy("source_topic","kafka_partition").show(100,False)
    finally:
        spark.stop()


def main():
    p=argparse.ArgumentParser()
    p.add_argument("--bootstrap-servers",default=os.getenv("KAFKA_BOOTSTRAP_SERVERS","localhost:9092"))
    p.add_argument("--bronze-root",default="lake/bronze")
    p.add_argument("--output",default="lake/gold/bronze_offset_reconciliation")
    p.add_argument("--topics",help="Comma-separated Bronze aliases")
    a=p.parse_args()
    aliases=[x.strip() for x in a.topics.split(",")] if a.topics else None
    reconcile(a.bootstrap_servers,a.bronze_root,a.output,aliases)

if __name__=="__main__":
    main()
