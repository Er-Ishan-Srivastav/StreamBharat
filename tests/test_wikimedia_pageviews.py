from datetime import date
import importlib.util
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts" / "fetch_wikimedia_pageviews.py"
SPEC = importlib.util.spec_from_file_location("wikimedia_pageviews", SCRIPT)
MODULE = importlib.util.module_from_spec(SPEC)
assert SPEC is not None and SPEC.loader is not None
SPEC.loader.exec_module(MODULE)


def test_pageviews_url_uses_encoded_article_and_daily_boundary_format():
    url = MODULE.build_pageviews_url(
        "Dune (2021 film)",
        date(2026, 11, 1),
        date(2026, 11, 27),
    )
    assert url.endswith(
        "/en.wikipedia.org/all-access/user/Dune_%282021_film%29/daily/2026110100/2026112700"
    )
