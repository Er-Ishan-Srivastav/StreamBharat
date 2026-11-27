"""Verify Kafka, Spark, Delta Lake and PostgreSQL connectivity in the local stack.

Run from the workspace container:
    python -m streambharat.smoke

The dataset is optional for this infrastructure smoke test.
"""
from __future__ import annotations

import os
import sys
import time
from pathlib import Path

from streambharat.create_topics import TOPICS
from streambharat.spark_session import create_spark


def _step(name, action):
    started = time.time()
    try:
        detail = action()
        print(f"  OK    {name:<34} {detail or ''} ({time.time() - started:.1f}s)")
        return True
    except Exception as exc:
        print(f"  FAIL  {name:<34} {type(exc).__name__}: {str(exc).splitlines()[0][:180]}")
        return False


def main() -> None:
    print("StreamBharat local stack connectivity check")
    bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    results = []

    def kafka_topics():
        from confluent_kafka.admin import AdminClient
        metadata = AdminClient({"bootstrap.servers": bootstrap}).list_topics(timeout=15)
        missing = sorted(set(TOPICS) - set(metadata.topics))
        if missing:
            raise RuntimeError(f"Missing topics: {missing}")
        wrong = {
            name: (len(metadata.topics[name].partitions), expected)
            for name, expected in TOPICS.items()
            if len(metadata.topics[name].partitions) != expected
        }
        if wrong:
            raise RuntimeError(f"Partition contract mismatch: {wrong}")
        return f"{len(TOPICS)} topics, partition contracts verified"

    results.append(_step("Kafka and topic contracts", kafka_topics))

    spark = None
    try:
        spark = create_spark("StreamBharat-Local-Smoke")
        results.append(_step(
            "Spark session and cluster",
            lambda: f"Spark {spark.version}; master {spark.sparkContext.master}",
        ))

        def kafka_source():
            count = (
                spark.read.format("kafka")
                .option("kafka.bootstrap.servers", bootstrap)
                .option("subscribe", "ott.player.events")
                .option("startingOffsets", "earliest")
                .load().limit(1).count()
            )
            return f"Kafka source connector readable; sample rows={count}"

        results.append(_step("Spark Kafka connector", kafka_source))

        def delta_roundtrip():
            path = "lake/_smoke/delta_roundtrip"
            spark.range(5).write.format("delta").mode("overwrite").save(path)
            count = spark.read.format("delta").load(path).count()
            if count != 5:
                raise AssertionError(f"Expected 5 Delta rows, found {count}")
            return f"Delta write/read passed at {path}"

        results.append(_step("Delta Lake round-trip", delta_roundtrip))

        def postgres_roundtrip():
            from pyspark.sql import Row
            url = (
                f"jdbc:postgresql://{os.getenv('POSTGRES_HOST', 'localhost')}:"
                f"{os.getenv('POSTGRES_PORT', '5432')}/"
                f"{os.getenv('POSTGRES_DB', 'ottdw')}"
            )
            properties = {
                "user": os.getenv("POSTGRES_USER", "ott"),
                "password": os.getenv("POSTGRES_PASSWORD", "ott_local_dev"),
                "driver": "org.postgresql.Driver",
            }
            table = "public._streambharat_smoke"
            spark.createDataFrame([Row(id=1, status="ok")]).write.jdbc(
                url, table, mode="overwrite", properties=properties
            )
            count = spark.read.jdbc(url, table, properties=properties).count()
            if count != 1:
                raise AssertionError(f"Expected 1 Postgres row, found {count}")
            import psycopg2
            connection = psycopg2.connect(
                host=os.getenv("POSTGRES_HOST", "localhost"),
                port=os.getenv("POSTGRES_PORT", "5432"),
                dbname=os.getenv("POSTGRES_DB", "ottdw"),
                user=properties["user"],
                password=properties["password"],
            )
            try:
                with connection:
                    with connection.cursor() as cursor:
                        cursor.execute("DROP TABLE IF EXISTS public._streambharat_smoke")
            finally:
                connection.close()
            return "Postgres JDBC write/read/drop passed"

        results.append(_step("Postgres JDBC round-trip", postgres_roundtrip))
    finally:
        if spark is not None:
            spark.stop()

    raw_root = Path(os.getenv("STREAMBHARAT_DATA_ROOT", "data/raw"))
    if raw_root.is_dir():
        print(f"  INFO  Dataset folder present: {raw_root}")
    else:
        print(f"  INFO  Dataset not mounted yet; data preflight is deferred ({raw_root})")

    passed = sum(results)
    print(f"\n{passed}/{len(results)} infrastructure checks passed.")
    if passed != len(results):
        sys.exit(1)
    print("ALL CHECKS PASSED")


if __name__ == "__main__":
    main()
