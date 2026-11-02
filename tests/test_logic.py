from streambharat.logic import choose_latest_event, churn_terminal_label, has_download_id, is_trending, trending_growth, watch_decline

def test_choose_latest_event_deduplicates_and_keeps_latest_server_arrival():
    rows = [
        {"event_id":"e1","server_ts":"2026-10-01T10:00:00","event_type":"start"},
        {"event_id":"e1","server_ts":"2026-10-01T10:01:00","event_type":"heartbeat"},
        {"event_id":"e2","server_ts":"2026-10-01T10:02:00","event_type":"stop"},
        {"event_id":None,"server_ts":"2026-10-01T10:03:00"},
    ]
    result = {r["event_id"]:r for r in choose_latest_event(rows)}
    assert set(result) == {"e1","e2"}
    assert result["e1"]["event_type"] == "heartbeat"

def test_download_id_detection():
    assert has_download_id("DL123")
    assert not has_download_id(None)
    assert not has_download_id("  ")

def test_trending_threshold_and_growth():
    assert trending_growth(300,100) == 3.0
    assert is_trending(250,100,minimum_viewers=200,minimum_growth=2.0)
    assert not is_trending(150,100,minimum_viewers=200)
    assert trending_growth(10,0) == 10.0

def test_watch_decline_direction():
    assert watch_decline(70,100) == 0.3
    assert watch_decline(120,100) == -0.2
    assert watch_decline(10,0) == 0.0

def test_churn_label_distinguishes_recovery_and_censoring():
    assert churn_terminal_label("voluntary_cancel") == 1
    assert churn_terminal_label("involuntary_failure") == 1
    assert churn_terminal_label("renewed") == 0
    assert churn_terminal_label(None) is None

def test_renewal_within_grace_overrides_intermediate_cancel():
    assert churn_terminal_label("voluntary_cancel", renewed_within_grace=True) == 0
    assert churn_terminal_label("involuntary_failure", renewed_within_grace=True) == 0

def test_unrecovered_failure_is_churn_without_renewal():
    assert churn_terminal_label("involuntary_failure", renewed_within_grace=False) == 1
