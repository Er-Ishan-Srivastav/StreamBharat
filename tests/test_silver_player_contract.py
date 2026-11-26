def test_schema_version_two_position_is_normalized_to_seconds_contract():
    # Contract-level test; the Spark fixture integration test should validate actual DataFrame values.
    v1 = {"position_sec": 120, "position_ms": None}
    v2 = {"position_sec": None, "position_ms": 120000}
    canonical_v1 = v1["position_ms"] / 1000 if v1["position_ms"] is not None else v1["position_sec"]
    canonical_v2 = v2["position_ms"] / 1000 if v2["position_ms"] is not None else v2["position_sec"]
    assert canonical_v1 == canonical_v2 == 120
