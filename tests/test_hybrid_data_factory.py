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
    assert all(row["client_ts"].endswith("+00:00") for row in playback)
    assert all(row["server_ts"].endswith("+00:00") for row in playback)
    assert all(row["signup_ts"].endswith("+00:00") for row in users)
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
    assert all(row["filled"] in {"0", "1"} for row in breaks)
    assert all(
        (row["filled"] == "1") == bool(row["impression_id"])
        for row in breaks
    )

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



def test_monthly_catalogue_history_labels_synthetic_commercial_changes(tmp_path):
    args = args_for(tmp_path, "monthly")
    args.catalog = str(make_public_catalog(tmp_path / "external" / "catalog.json", size=120))
    args.start_date = "2026-11-01"
    args.days = 90
    args.users = 10
    args.catalog_size = 120
    GENERATOR.generate(args)

    root = Path(args.output)
    snapshot_paths = sorted((root / "content").glob("catalog_*.json"))
    assert len(snapshot_paths) == 3
    snapshots = [
        {
            row["wikidata_qid"]: row
            for row in json.loads(path.read_text(encoding="utf-8"))
        }
        for path in snapshot_paths
    ]

    changed_rows = 0
    for qid, current in snapshots[1].items():
        previous = snapshots[0][qid]
        partner_changed = current["partner_id"] != previous["partner_id"]
        tier_changed = current["tier"] != previous["tier"]
        if partner_changed or tier_changed:
            changed_rows += 1
        reasons = set(current["synthetic_change_reasons"])
        assert partner_changed == ("synthetic_partner_assignment_change" in reasons)
        assert tier_changed == ("synthetic_tier_change" in reasons)
    assert changed_rows > 0

    preflight_path = ROOT / "scripts" / "preflight_hybrid_dataset.py"
    preflight_spec = importlib.util.spec_from_file_location(
        "hybrid_dataset_preflight_monthly", preflight_path
    )
    preflight = importlib.util.module_from_spec(preflight_spec)
    assert preflight_spec is not None and preflight_spec.loader is not None
    preflight_spec.loader.exec_module(preflight)
    report = preflight.validate_dataset(args.output)
    assert report["status"] == "passed", report["errors"]



def test_external_pageviews_weight_synthetic_playback_sampling_and_are_manifested(tmp_path):
    args = args_for(tmp_path, "pageview_weighted")
    pageviews = tmp_path / "external" / "pageviews.jsonl"
    pageviews.write_text(
        "\n".join(
            [
                json.dumps({
                    "wikidata_qid": "Q100000",
                    "date": "2026-10-01",
                    "pageviews": 10000,
                    "license": "CC0-1.0",
                }),
                json.dumps({
                    "wikidata_qid": "Q100001",
                    "date": "2026-10-01",
                    "pageviews": 10,
                    "license": "CC0-1.0",
                }),
            ]
        ) + "\n",
        encoding="utf-8",
    )
    args.pageviews = str(pageviews)
    manifest = GENERATOR.generate(args)

    assert manifest["configuration"]["playback_title_sampling"] == "pageview_weighted"
    snapshot = manifest["input_snapshots"]["wikimedia_pageviews"]
    assert snapshot["sha256"] == hashlib.sha256(pageviews.read_bytes()).hexdigest()
    assert snapshot["license"] == "CC0-1.0"
    assert snapshot["matched_catalogue_titles"] == 2
    assert "synthetic playback" in snapshot["semantics"].lower()
    assert manifest["quality_features"]["playback_title_sampling"] == snapshot["semantics"]


def test_playback_sampling_is_uniform_without_pageview_input():
    catalog = [
        {"wikidata_qid": "Q1"},
        {"wikidata_qid": "Q2"},
    ]
    weights, metadata = GENERATOR.build_playback_sampling_weights(catalog)
    assert weights == [1.0, 1.0]
    assert metadata is None


def test_pageview_sampling_rejects_non_cc0_input(tmp_path):
    pageviews = tmp_path / "pageviews.jsonl"
    pageviews.write_text(
        json.dumps({"wikidata_qid": "Q1", "date": "2026-10-01", "pageviews": 10, "license": "other"}) + "\n",
        encoding="utf-8",
    )
    try:
        GENERATOR.build_playback_sampling_weights(
            [{"wikidata_qid": "Q1"}], pageviews
        )
    except ValueError as exc:
        assert "CC0-1.0" in str(exc)
    else:
        raise AssertionError("Expected non-CC0 pageview input to be rejected")



def test_pageview_prior_rejects_observations_at_or_after_simulation_start(tmp_path):
    pageviews = tmp_path / "future_pageviews.jsonl"
    pageviews.write_text(
        json.dumps({
            "wikidata_qid": "Q1",
            "date": "2026-11-01",
            "pageviews": 100,
            "license": "CC0-1.0",
        }) + "\n",
        encoding="utf-8",
    )
    try:
        GENERATOR.build_playback_sampling_weights(
            [{"wikidata_qid": "Q1"}],
            pageviews,
            GENERATOR.date.fromisoformat("2026-11-01"),
        )
    except ValueError as exc:
        assert "must be earlier than simulation start" in str(exc)
    else:
        raise AssertionError("Expected pageviews on the simulation start date to be rejected")
