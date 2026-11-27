"""Shared, deterministic Spark fixture for Delta-backed contract tests."""
from __future__ import annotations

import os

import pytest

# Keep timezone-less source timestamps stable across developer machines and CI.
os.environ["SPARK_SESSION_TIMEZONE"] = "UTC"
os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")
os.environ.setdefault("SPARK_MASTER", "local[2]")
os.environ.setdefault("SPARK_SHUFFLE_PARTITIONS", "2")


@pytest.fixture
def spark():
    from streambharat.spark_session import create_spark

    session = create_spark("streambharat-pytest")
    session.sparkContext.setLogLevel("ERROR")
    try:
        yield session
    finally:
        session.stop()
