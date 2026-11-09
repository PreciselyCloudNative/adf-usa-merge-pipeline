"""Pure-logic tests for detect.py (no Spark)."""

import json

import pytest

from adf_merge.detect import (
    DetectError,
    parse_vintage_commits,
    pending_amendments,
    pending_releases,
    plan_detection,
    retention_expired,
    vintage_key,
)


def _commit(version, vintage, count=100, ts="2026-10-27T16:25:12.000Z", op="SET TBLPROPERTIES"):
    props = {"precisely.data.vintage": vintage, "precisely.data.count": str(count)}
    return {"version": version, "operation": op, "timestamp": ts,
            "operationParameters": json.dumps({"properties": json.dumps(props)})}


def _label(v, pin, status="PROPOSED", state=None, seq=None, applied=None, count=None):
    return {"release_version": v, "source_delta_version": pin, "status": status, "_state": state, "_seq": seq,
            "_applied_pin": applied, "vendor_row_count": count}


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
    assert plan == {"new_labels": [], "anomalies": [], "remarks": []}


def test_latest_marker_commit_of_a_vintage_is_the_pin():
    commits = parse_vintage_commits([_commit(10, "2026.11.0"), _commit(12, "2026.11.0")])
    plan = plan_detection(commits, {}, {"precisely.data.vintage": "2026.11.0"})
    assert plan["new_labels"][0]["source_delta_version"] == 12


def test_pin_change_on_confirmed_or_in_flight_release_is_an_anomaly():
    commits = parse_vintage_commits([_commit(30, "2026.11.1")])
    props = {"precisely.data.vintage": "2026.11.1"}
    assert plan_detection(commits, {"2026.11.1": _label("2026.11.1", 28, "CONFIRMED")}, props)["anomalies"]
    assert plan_detection(commits, {"2026.11.1": _label("2026.11.1", 28, "PROPOSED", "FAILED", 3, 28)}, props)["anomalies"]


def test_remark_before_the_release_started_just_moves_the_pin():
    commits = parse_vintage_commits([_commit(30, "2026.11.1", 555)])
    plan = plan_detection(commits, {"2026.11.1": _label("2026.11.1", 28, count=560)}, {"precisely.data.vintage": "2026.11.1"})
    assert plan["anomalies"] == []
    assert plan["remarks"] == [{"release_version": "2026.11.1", "old_pin": 28, "new_pin": 30, "old_count": 560,
                                "new_count": 555, "action": "repin"}]


def test_remark_of_newest_complete_release_is_an_amendment_even_if_count_is_unchanged():
    # 2026.10.3: weekly_cdc marker at v223, NJ_DELETION marker at v227 with a lower count
    commits = parse_vintage_commits([_commit(223, "2026.10.3", 194481535), _commit(227, "2026.10.3", 194481507)])
    labels = {"2026.10.3": _label("2026.10.3", 223, "PROPOSED", "COMPLETE", 3, 223, 194481535)}
    plan = plan_detection(commits, labels, {"precisely.data.vintage": "2026.10.3"})
    assert plan["anomalies"] == []
    assert plan["remarks"] == [{"release_version": "2026.10.3", "old_pin": 223, "new_pin": 227, "old_count": 194481535,
                                "new_count": 194481507, "action": "amend"}]
    same_count = parse_vintage_commits([_commit(223, "2026.10.3", 5), _commit(227, "2026.10.3", 5)])
    assert plan_detection(same_count, labels, {"precisely.data.vintage": "2026.10.3"})["remarks"][0]["action"] == "amend"


def test_remark_already_applied_or_repinned_is_not_reported_again():
    commits = parse_vintage_commits([_commit(223, "2026.10.3"), _commit(227, "2026.10.3")])
    props = {"precisely.data.vintage": "2026.10.3"}
    repinned = {"2026.10.3": _label("2026.10.3", 227, "PROPOSED", "COMPLETE", 3, 223)}  # detect ran, release pending
    done = {"2026.10.3": _label("2026.10.3", 227, "PROPOSED", "COMPLETE", 3, 227)}  # amendment finished
    assert plan_detection(commits, repinned, props)["remarks"] == []
    assert plan_detection(commits, done, props) == {"new_labels": [], "anomalies": [], "remarks": []}


def test_remark_of_an_older_complete_release_is_an_anomaly():
    commits = parse_vintage_commits([_commit(230, "2026.10.3")])
    labels = {"2026.10.3": _label("2026.10.3", 227, "PROPOSED", "COMPLETE", 3, 227),
              "2026.11.0": _label("2026.11.0", 229, "PROPOSED", "COMPLETE", 4, 229)}
    plan = plan_detection(commits, {"2026.10.3": labels["2026.10.3"], "2026.11.0": labels["2026.11.0"]},
                          {"precisely.data.vintage": "2026.10.3"})
    assert plan["remarks"] == [] and plan["anomalies"]


def test_pending_amendment_is_the_newest_complete_release_with_a_label_pin_above_its_applied_pin():
    labels = {"2026.10.2": _label("2026.10.2", 213, "CONFIRMED"), "2026.10.3": _label("2026.10.3", 227)}
    log = {"2026.10.2": {"release_version": "2026.10.2", "state": "COMPLETE", "release_seq": 2, "source_delta_version": 213},
           "2026.10.3": {"release_version": "2026.10.3", "state": "COMPLETE", "release_seq": 3, "source_delta_version": 223}}
    assert [r["release_version"] for r in pending_amendments(labels, log)] == ["2026.10.3"]
    log["2026.10.3"]["source_delta_version"] = 227
    assert pending_amendments(labels, log) == []
    log["2026.10.3"].update(state="ROLLED_BACK", source_delta_version=223)
    assert pending_amendments(labels, log) == []  # already rolled back: it is an ordinary pending release now
    labels["2026.10.3"]["status"] = "SUPERSEDED"
    log["2026.10.3"]["state"] = "COMPLETE"
    assert pending_amendments(labels, log) == []


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
    assert plan_detection(commits, labels, {"precisely.data.vintage": "2026.10.2"}) == {"new_labels": [], "anomalies": [], "remarks": []}


def test_retention_expired_detects_vacuumed_pin_and_reports_window():
    err = ("Error: query failed: BAD_REQUEST [DELTA_UNSUPPORTED_TIME_TRAVEL_BEYOND_DELETED_FILE_RETENTION_DURATION] "
           "Cannot time travel beyond delta.deletedFileRetentionDuration (168 HOURS) set on the table.")
    assert retention_expired(err) == "168 HOURS"


def test_retention_expired_ignores_other_errors():
    assert retention_expired("PERMISSION_DENIED: no access to share") is None
    assert retention_expired("") is None
    assert retention_expired(None) is None
