from streambharat.dlq_utils import exclude_dlq_offsets


def test_bronze_transform_excludes_only_matching_source_topic_offsets(spark, tmp_path):
    bronze = spark.createDataFrame(
        [(0, 10, "valid-a"), (0, 11, "invalid-player"), (1, 20, "valid-b")],
        ["kafka_partition", "kafka_offset", "event_id"],
    )
    dlq = spark.createDataFrame(
        [
            ("ott.player.events", 0, 11),
            ("ott.billing.subscr", 1, 20),
        ],
        ["source_topic", "kafka_partition", "kafka_offset"],
    )
    dlq_path = str(tmp_path / "dlq")
    dlq.write.format("delta").mode("overwrite").save(dlq_path)

    result = exclude_dlq_offsets(bronze, "ott.player.events", dlq_path)
    assert [row.event_id for row in result.orderBy("kafka_offset").collect()] == [
        "valid-a", "valid-b"
    ]


def test_missing_dlq_table_leaves_bronze_unchanged(spark, tmp_path):
    bronze = spark.createDataFrame(
        [(0, 10, "valid-a")],
        ["kafka_partition", "kafka_offset", "event_id"],
    )
    result = exclude_dlq_offsets(
        bronze, "ott.player.events", str(tmp_path / "not-created")
    )
    assert result.count() == 1
