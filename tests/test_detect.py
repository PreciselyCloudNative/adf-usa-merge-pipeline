"""Pure-logic tests for detect.py (no Spark)."""

import json

import pytest

from adf_merge.detect import DetectError, parse_vintage_commits, pending_releases, plan_detection, vintage_key


def _commit(version, vintage, count=100, ts="2026-10-27T16:25:12.000Z", op="SET TBLPROPERTIES"):
    props = {"precisely.data.vintage": vintage, "precisely.data.count": str(count)}
    return {"version": version, "operation": op, "timestamp": ts,
            "operationParameters": json.dumps({"properties": json.dumps(props)})}


def _label(v, pin, status="PROPOSED", state=None):
    return {"release_version": v, "source_delta_version": pin, "status": status, "_state": state}


def test_parse_ignores_other_operations_and_orders_by_version():
    rows = [_commit(227, "2026.10.2"), {"version": 226, "operation": "MERGE", "operationParameters": "{}"},
            _commit(219, "2026.10.1")]
    assert [c["version"] for c in parse_vintage_commits(rows)] == [219, 227]
    assert parse_vintage_commits(rows)[1]["count"] == 100


def test_new_vintage_is_seeded_with_pin_and_count():
    commits = parse_vintage_commits([_commit(219, "2026.10.1"), _commit(227, "2026.10.2", 555)])
    plan = plan_detection(commits, {"2026.10.1": _label("2026.10.1", 219, "CONFIRMED", "COMPLETE")},
                          {"precisely.data.vintage": "2026.10.2"})
    assert plan["anomalies"] == []
    assert plan["new_labels"] == [{"release_version": "2026.10.2", "release_date": "2026-10-27",
                                   "source_delta_version": 227, "vendor_row_count": 555}]


def test_known_and_older_vintages_are_not_reseeded():
    commits = parse_vintage_commits([_commit(100, "2026.09.4"), _commit(219, "2026.10.1")])
    labels = {"2026.10.1": _label("2026.10.1", 219, "CONFIRMED", "COMPLETE")}
    plan = plan_detection(commits, labels, {"precisely.data.vintage": "2026.10.1"})
    assert plan == {"new_labels": [], "anomalies": []}


def test_latest_marker_commit_of_a_vintage_is_the_pin():
    commits = parse_vintage_commits([_commit(10, "2026.11.0"), _commit(12, "2026.11.0")])
    plan = plan_detection(commits, {}, {"precisely.data.vintage": "2026.11.0"})
    assert plan["new_labels"][0]["source_delta_version"] == 12


def test_pin_change_on_unfinished_release_is_an_anomaly_but_not_on_complete():
    commits = parse_vintage_commits([_commit(30, "2026.11.1")])
    props = {"precisely.data.vintage": "2026.11.1"}
    assert plan_detection(commits, {"2026.11.1": _label("2026.11.1", 28)}, props)["anomalies"]
    assert not plan_detection(commits, {"2026.11.1": _label("2026.11.1", 28, "CONFIRMED", "COMPLETE")}, props)["anomalies"]


def test_property_vs_history_mismatch_and_regression_are_anomalies():
    commits = parse_vintage_commits([_commit(10, "2026.11.1"), _commit(12, "2026.11.0")])
    plan = plan_detection(commits, {}, {"precisely.data.vintage": "2026.11.0"})
    assert any("not increasing" in a for a in plan["anomalies"])
    commits = parse_vintage_commits([_commit(10, "2026.11.0")])
    assert plan_detection(commits, {}, {"precisely.data.vintage": "2026.10.9"})["anomalies"]


def test_bad_vintage_format_is_an_anomaly():
    plan = plan_detection(parse_vintage_commits([_commit(1, "oct-2026")]), {}, {})
    assert plan["anomalies"] and plan["new_labels"] == []
    with pytest.raises(DetectError):
        vintage_key("x")


def test_pending_are_unfinished_newer_than_last_complete_in_commit_order():
    labels = {v: _label(v, pin) for v, pin in [("2026.10.1", 219), ("2026.10.2", 227), ("2026.11.0", 300), ("2026.11.1", 310)]}
    labels["2026.11.0"]["status"] = "SUPERSEDED"
    states = {"2026.10.1": "COMPLETE", "2026.10.2": "COMPLETE"}
    assert [r["release_version"] for r in pending_releases(labels, states)] == ["2026.11.1"]
    assert pending_releases(labels, {"2026.10.1": "COMPLETE"})[0]["release_version"] == "2026.10.2"


def test_spark_map_parameters_are_accepted():
    row = _commit(5, "2026.11.0")
    row["operationParameters"] = json.loads(row["operationParameters"])
    assert parse_vintage_commits([row])[0]["vintage"] == "2026.11.0"


def test_old_irregular_history_is_ignored_and_unpadded_vintages_parse():
    assert vintage_key("2026.9.0") == (2026, 9, 0) and vintage_key("2026.3") == (2026, 3, 0)
    commits = parse_vintage_commits([_commit(5, "2026.6.25"), _commit(6, "2026.6.1"), _commit(7, "garbage"),
                                     _commit(219, "2026.10.1"), _commit(227, "2026.10.2")])
    labels = {"2026.10.1": _label("2026.10.1", 219, "CONFIRMED", "COMPLETE"),
              "2026.10.2": _label("2026.10.2", 227, "PROPOSED", "COMPLETE")}
    assert plan_detection(commits, labels, {"precisely.data.vintage": "2026.10.2"}) == {"new_labels": [], "anomalies": []}
