from __future__ import annotations

import importlib.util
import json
from pathlib import Path

from test_hybrid_data_factory import args_for

ROOT = Path(__file__).resolve().parents[1]
PREFLIGHT_PATH = ROOT / "scripts" / "preflight_hybrid_dataset.py"
SPEC = importlib.util.spec_from_file_location("hybrid_dataset_preflight", PREFLIGHT_PATH)
PREFLIGHT = importlib.util.module_from_spec(SPEC)
assert SPEC is not None and SPEC.loader is not None
SPEC.loader.exec_module(PREFLIGHT)


def test_hybrid_preflight_accepts_generated_source_tree(tmp_path):
    args = args_for(tmp_path, "hybrid")
    ROOT_GENERATOR = ROOT / "scripts" / "generate_hybrid_dataset.py"
    gen_spec = importlib.util.spec_from_file_location("hybrid_dataset_generator_for_preflight", ROOT_GENERATOR)
    generator = importlib.util.module_from_spec(gen_spec)
    assert gen_spec is not None and gen_spec.loader is not None
    gen_spec.loader.exec_module(generator)
    generator.generate(args)

    report = PREFLIGHT.validate_dataset(args.output)
    assert report["status"] == "passed", report["errors"]
    assert report["row_counts_observed"]["player/playback_events"] > 0
    assert report["row_counts_observed"]["cdn/requests"] > 0


def test_hybrid_preflight_detects_checksum_tampering(tmp_path):
    args = args_for(tmp_path, "hybrid")
    ROOT_GENERATOR = ROOT / "scripts" / "generate_hybrid_dataset.py"
    gen_spec = importlib.util.spec_from_file_location("hybrid_dataset_generator_for_tamper", ROOT_GENERATOR)
    generator = importlib.util.module_from_spec(gen_spec)
    assert gen_spec is not None and gen_spec.loader is not None
    gen_spec.loader.exec_module(generator)
    generator.generate(args)

    users_path = Path(args.output) / "app" / "users.json"
    users_path.write_text(users_path.read_text(encoding="utf-8") + "\n", encoding="utf-8")
    report = PREFLIGHT.validate_dataset(args.output)
    assert report["status"] == "failed"
    assert any("Checksum mismatch" in error for error in report["errors"])


def test_hybrid_preflight_detects_broken_foreign_keys(tmp_path):
    args = args_for(tmp_path, "hybrid")
    ROOT_GENERATOR = ROOT / "scripts" / "generate_hybrid_dataset.py"
    gen_spec = importlib.util.spec_from_file_location("hybrid_dataset_generator_for_fk", ROOT_GENERATOR)
    generator = importlib.util.module_from_spec(gen_spec)
    assert gen_spec is not None and gen_spec.loader is not None
    generator.generate(args)

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
