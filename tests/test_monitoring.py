import json

from streambharat.monitoring import parse_end_offsets


def test_parse_kafka_end_offsets_returns_topic_partition_next_offset():
    raw = json.dumps({
        "ott.player.events": {"0": 10, "1": 25},
        "ott.billing.subscr": {"2": 99},
    })
    assert parse_end_offsets(raw) == [
        ("ott.player.events", 0, 10),
        ("ott.player.events", 1, 25),
        ("ott.billing.subscr", 2, 99),
    ]


def test_parse_kafka_end_offsets_ignores_invalid_values():
    assert parse_end_offsets(None) == []
    assert parse_end_offsets("not-json") == []
    assert parse_end_offsets({"topic": {"bad": "offset", "0": 5}}) == [
        ("topic", 0, 5)
    ]
