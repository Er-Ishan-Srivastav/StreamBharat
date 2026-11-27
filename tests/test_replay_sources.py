from __future__ import annotations

import importlib.util
import json
import sys
from datetime import date
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
PRODUCERS = ROOT / "producers"
if str(PRODUCERS) not in sys.path:
    sys.path.insert(0, str(PRODUCERS))

SPEC = importlib.util.spec_from_file_location("streambharat_replay_sources", PRODUCERS / "sources.py")
SOURCES = importlib.util.module_from_spec(SPEC)
assert SPEC is not None and SPEC.loader is not None
SPEC.loader.exec_module(SOURCES)


class DateContext:
    def __init__(self, root: Path):
        self.data = str(root)
        self.d_from = date(2026, 11, 1)
        self.d_to = date(2026, 11, 3)

    def in_range(self, value):
        return self.d_from <= value.date() <= self.d_to


def test_jsonl_source_reader_accepts_compact_json_rows(tmp_path: Path):
    source = tmp_path / "subscription_events.json"
    source.write_text(
        "\n".join(
            [
                json.dumps(
                    {
                        "event_id": "SUB-U1-INIT",
                        "user_id": "U1",
                        "event_type": "subscribe",
                        "plan": "premium",
                        "price": "399.00",
                        "event_ts": "2026-11-02T08:00:00",
                    },
                    separators=(",", ":"),
                ),
                json.dumps(
                    {
                        "event_id": "SUB-U2-INIT",
                        "user_id": "U2",
                        "event_type": "subscribe",
                        "plan": "premium",
                        "price": "399.00",
                        "event_ts": "2026-11-04T08:00:00",
                    },
                    separators=(",", ":"),
                ),
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    rows = SOURCES._json_rows(
        str(source), "event_ts", "user_id", DateContext(tmp_path)
    )

    assert len(rows) == 1
    assert rows[0][1] == "U1"
    assert rows[0][2]["event_id"] == "SUB-U1-INIT"
