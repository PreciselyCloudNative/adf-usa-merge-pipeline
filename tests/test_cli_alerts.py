"""Changefile alert lines (no Spark)."""

from adf_merge.cli import changefile_alerts


def test_no_alert_for_clean_changefile_apply():
    assert changefile_alerts([{"release": "2026.10.3", "result": "complete", "method": "CHANGEFILE"}]) == []


def test_alert_when_deferred():
    out = changefile_alerts([{"release": "2026.10.3", "result": "deferred", "reason": "changefile is at vintage 2026.10.2"}])
    assert len(out) == 1 and "WAITING" in out[0] and "2026.10.3" in out[0]


def test_alert_when_merge_fallback():
    out = changefile_alerts([{"release": "2026.10.3", "result": "complete",
                              "fallback_reason": "changefile not ready: no release marker"}])
    assert len(out) == 1 and "MERGE" in out[0]


def test_noop_and_plain_merge_do_not_alert():
    assert changefile_alerts([{"release": "a", "result": "noop"}, {"release": "b", "result": "complete"}]) == []
