from __future__ import annotations

import importlib.util
import json
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
PREFLIGHT_PATH = ROOT / "scripts" / "preflight_hybrid_dataset.py"
SPEC = importlib.util.spec_from_file_location("hybrid_dataset_preflight", PREFLIGHT_PATH)
PREFLIGHT = importlib.util.module_from_spec(SPEC)
assert SPEC is not None and SPEC.loader is not None
SPEC.loader.exec_module(PREFLIGHT)


def args_for(tmp_path: Path, output_name: str) -> object:
    catalog = tmp_path / "external" / "catalog.json"
    records = []
    for number in range(20):
        records.append({
            "wikidata_qid": f"Q{100000 + number}",
            "title": f"Public Film {number:03d}",
            "release_date": f"{2020 + number % 6}-01-15",
            "runtime_minutes": 95 + number,
            "genres": ["Drama", "Thriller"],
            "original_languages": ["Hindi"],
            "wikipedia_title": f"Public_Film_{number:03d}",
        })
    catalog.parent.mkdir(parents=True, exist_ok=True)
    catalog.write_text(json.dumps({
        "dataset": "wikidata_film_catalogue",
        "license": "CC0-1.0",
        "source_endpoint": "https://query.wikidata.org/sparql",
        "retrieved_at_utc": "2026-11-28T00:00:00+00:00",
        "records": records,
    }), encoding="utf-8")
    return type("Args", (), {
        "catalog": str(catalog),
        "output": str(tmp_path / output_name),
        "start_date": "2026-11-01",
        "days": 4,
        "users": 30,
        "catalog_size": 20,
        "seed": 42,
        "overwrite": False,
    })()


def generate_dataset(args):
    generator_path = ROOT / "scripts" / "generate_hybrid_dataset.py"
    gen_spec = importlib.util.spec_from_file_location("hybrid_dataset_generator_test", generator_path)
    generator = importlib.util.module_from_spec(gen_spec)
    assert gen_spec is not None and gen_spec.loader is not None
    gen_spec.loader.exec_module(generator)
    return generator.generate(args)


def test_hybrid_preflight_accepts_generated_source_tree(tmp_path: Path):
    args = args_for(tmp_path, "hybrid")
    generate_dataset(args)

    report = PREFLIGHT.validate_dataset(args.output)
    assert report["status"] == "passed", report["errors"]
    assert report["row_counts_observed"]["player/playback_events"] > 0
    assert report["row_counts_observed"]["cdn/requests"] > 0


def test_hybrid_preflight_detects_checksum_tampering(tmp_path):
    args = args_for(tmp_path, "hybrid")
    generate_dataset(args)

    users_path = Path(args.output) / "app" / "users.json"
    users_path.write_text(users_path.read_text(encoding="utf-8") + "\n", encoding="utf-8")
    report = PREFLIGHT.validate_dataset(args.output)
    assert report["status"] == "failed"
    assert any("Checksum mismatch" in error for error in report["errors"])


def test_hybrid_preflight_detects_broken_foreign_keys(tmp_path):
    args = args_for(tmp_path, "hybrid")
    generate_dataset(args)

    ratings_path = Path(args.output) / "app" / "ratings.json"
    rows = [json.loads(line) for line in ratings_path.read_text(encoding="utf-8").splitlines()]
    rows[0]["profile_id"] = "MISSING-PROFILE"
    ratings_path.write_text(
        "\n".join(json.dumps(row) for row in rows) + "\n",
        encoding="utf-8",
    )
    # Update the manifest hash so that this test isolates relational validation.
    manifest_path = Path(args.output) / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    import hashlib
    manifest["files_sha256"]["app/ratings.json"] = hashlib.sha256(ratings_path.read_bytes()).hexdigest()
    manifest_path.write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")

    report = PREFLIGHT.validate_dataset(args.output)
    assert report["status"] == "failed"
    assert any("Rating references unknown profile_id" in error for error in report["errors"])
