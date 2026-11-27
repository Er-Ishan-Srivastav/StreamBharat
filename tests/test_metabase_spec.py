import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))

from bootstrap_metabase import parse_dashboard_spec


def test_metabase_spec_contains_three_dashboards_and_saved_sql_cards():
    dashboards = parse_dashboard_spec(
        Path("config/dashboard/streambharat-metabase-questions.md")
    )
    assert len(dashboards) == 3
    assert all(item["questions"] for item in dashboards)
    names = {
        question["name"]
        for dashboard in dashboards
        for question in dashboard["questions"]
    }
    assert "Daily engagement overview" in names
    assert "Live session windows (Q9)" in names
    assert "Live trending titles (Q17)" in names
    assert "DLQ records by source and reason" in names
    assert "Published board variance (0.5% tolerance)" in names
