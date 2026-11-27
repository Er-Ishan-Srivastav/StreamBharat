from __future__ import annotations

import importlib.util
import json
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts" / "build_public_interest_features.py"
SPEC = importlib.util.spec_from_file_location("public_interest_features", SCRIPT)
MODULE = importlib.util.module_from_spec(SPEC)
assert SPEC is not None and SPEC.loader is not None
SPEC.loader.exec_module(MODULE)


def test_public_interest_features_join_by_wikidata_qid_and_keep_semantics(tmp_path: Path):
    public_catalog = tmp_path / "public_catalog.json"
    public_catalog.write_text(
        json.dumps(
            {
                "records": [
                    {"wikidata_qid": "Q1", "title": "Feature Film"},
                    {"wikidata_qid": "Q2", "title": "Unmatched Film"},
                ]
            }
        ),
        encoding="utf-8",
    )
    pageviews = tmp_path / "pageviews.jsonl"
    pageviews.write_text(
        "\n".join(
            [
                json.dumps({"wikidata_qid": "Q1", "date": "2026-11-01", "pageviews": 10}),
                json.dumps({"wikidata_qid": "Q1", "date": "2026-11-02", "pageviews": 12}),
                json.dumps({"wikidata_qid": "Q2", "date": "2026-11-01", "pageviews": 99}),
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    hybrid_catalog = tmp_path / "hybrid_catalog.json"
    hybrid_catalog.write_text(
        json.dumps(
            [
                {"wikidata_qid": "Q1", "content_id": "CNT00001"},
                {"wikidata_qid": "Q3", "content_id": "CNT00003"},
            ]
        ),
        encoding="utf-8",
    )

    rows = MODULE.build_features(public_catalog, pageviews, hybrid_catalog)

    assert len(rows) == 2
    assert [row["article_pageviews"] for row in rows] == [10, 12]
    assert [row["rolling_7d_article_pageviews"] for row in rows] == [10, 22]
    assert {row["content_id"] for row in rows} == {"CNT00001"}
    assert all(row["provenance"] == "public_external" for row in rows)
    assert all("not OTT" in row["measure_semantics"] for row in rows)
