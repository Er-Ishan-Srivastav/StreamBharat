from __future__ import annotations

import argparse
import hashlib
import importlib.util
import json
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
GENERATOR_PATH = ROOT / "scripts" / "generate_hybrid_dataset.py"
SPEC = importlib.util.spec_from_file_location("hybrid_dataset_generator", GENERATOR_PATH)
GENERATOR = importlib.util.module_from_spec(SPEC)
assert SPEC is not None and SPEC.loader is not None
SPEC.loader.exec_module(GENERATOR)


def make_public_catalog(path: Path, size: int = 20) -> Path:
    records = []
    for number in range(size):
        records.append(
            {
                "wikidata_qid": f"Q{100000 + number}",
                "title": f"Public Film {number:03d}",
                "release_date": f"{2020 + number % 6}-01-15",
                "runtime_minutes": 95 + number,
                "genres": ["Drama", "Thriller"] if number % 2 else ["Comedy"],
                "original_languages": ["Hindi"] if number % 2 else ["English"],
                "wikipedia_title": f"Public_Film_{number:03d}",
                "source_url": f"https://www.wikidata.org/wiki/Q{100000 + number}",
            }
        )
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(
            {
                "dataset": "wikidata_film_catalogue",
                "license": "CC0-1.0",
                "source_endpoint": "https://query.wikidata.org/sparql",
                "retrieved_at_utc": "2026-11-28T00:00:00+00:00",
                "records": records,
            }
        ),
        encoding="utf-8",
    )
    return path


def args_for(tmp_path: Path, output_name: str) -> argparse.Namespace:
    catalog = make_public_catalog(tmp_path / "external" / "catalog.json")
    return argparse.Namespace(
        catalog=str(catalog),
        output=str(tmp_path / output_name),
        start_date="2026-11-01",
        days=4,
        users=30,
        catalog_size=20,
        seed=42,
        overwrite=False,
    )


def read_jsonl(path: Path) -> list[dict]:
    if not path.exists():
        return []
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line]


def test_hybrid_factory_writes_source_contracts_and_provenance(tmp_path):
    args = args_for(tmp_path, "hybrid")
    manifest = GENERATOR.generate(args)
    root = Path(args.output)

    catalogue_path = root / "content" / "catalog_2026-11-01.json"
    catalogue = json.loads(catalogue_path.read_text(encoding="utf-8"))
    assert len(catalogue) == 20
    assert catalogue[0]["provenance"] == "public_external+synthetic_derived"
    assert catalogue[0]["external_license"] == "CC0-1.0"

    users = read_jsonl(root / "app" / "users.json")
    profiles = read_jsonl(root / "app" / "profiles.json")
    assert len(users) == 30
    assert len(profiles) >= len(users)
    assert all(row["provenance"] == "synthetic_generated" for row in users)

    playback_paths = sorted((root / "player").glob("playback_*.json"))
    assert playback_paths
    playback = [row for path in playback_paths for row in read_jsonl(path)]
    assert playback
    assert {row["event_type"] for row in playback} >= {"start", "heartbeat"}
    assert all(row["content_id"].startswith("CNT") for row in playback)
    assert all(row["provenance"] == "synthetic_generated" for row in playback)
    assert all(row["client_ts"].endswith("Z") for row in playback)
    assert all(row["server_ts"].endswith("Z") for row in playback)
    assert all(row["signup_ts"].endswith("Z") for row in users)
    assert any(row["event_type"] == "complete" for row in playback)

    subscriptions = read_jsonl(root / "billing" / "subscription_events.json")
    assert subscriptions
    assert all(row["user_id"] in {user["user_id"] for user in users} for row in subscriptions)
    assert len({row["event_id"] for row in subscriptions}) == len(subscriptions)
    assert any(
        row["event_type"] == "activate" and row["plan"] == "free"
        for row in subscriptions
    )

    searches = read_jsonl(root / "search" / "search_logs.json")
    assert all(
        not row["clicked_content_id"] or int(row["results_count"]) > 0
        for row in searches
    )
    ratings = read_jsonl(root / "app" / "ratings.json")
    rating_keys = [(row["profile_id"], row["content_id"]) for row in ratings]
    assert len(rating_keys) == len(set(rating_keys))

    impressions = []
    with (root / "ads" / "ad_impressions.csv").open(encoding="utf-8") as handle:
        import csv
        impressions = list(csv.DictReader(handle))
    breaks = []
    with (root / "ads" / "ad_breaks.csv").open(encoding="utf-8") as handle:
        import csv
        breaks = list(csv.DictReader(handle))
    assert len(impressions) <= len(breaks)
    assert all(row["impression_id"] for row in impressions)

    assert (root / "reference" / "cdn_edges.csv").is_file()
    with (root / "reference" / "plans.csv").open(encoding="utf-8") as handle:
        import csv
        plan_rows = list(csv.DictReader(handle))
    assert {row["plan"] for row in plan_rows} == {"free", "basic", "premium"}
    assert (root / "cdn").is_dir()
    assert manifest["dataset_type"] == "hybrid"
    assert manifest["configuration"]["seed"] == 42
    assert set(manifest["provenance"]) == {
        "public_external",
        "synthetic_derived",
        "synthetic_generated",
    }
    assert manifest["files_sha256"]
    assert manifest["input_snapshots"]["wikidata_catalogue"]["sha256"] == hashlib.sha256(
        Path(args.catalog).read_bytes()
    ).hexdigest()
    assert (root / "manifest.json").is_file()


def test_hybrid_factory_is_deterministic_for_a_fixed_seed(tmp_path):
    first = args_for(tmp_path, "first")
    second = args_for(tmp_path, "second")
    first_manifest = GENERATOR.generate(first)
    second_manifest = GENERATOR.generate(second)

    assert first_manifest["files_sha256"] == second_manifest["files_sha256"]
    assert first_manifest["row_counts"] == second_manifest["row_counts"]


def test_hybrid_factory_does_not_overwrite_existing_data_by_default(tmp_path):
    args = args_for(tmp_path, "existing")
    GENERATOR.generate(args)
    try:
        GENERATOR.generate(args)
    except FileExistsError:
        pass
    else:
        raise AssertionError("Expected the factory to protect a non-empty output directory")
