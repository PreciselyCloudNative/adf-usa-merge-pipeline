"""Pure-logic tests for changefile.py (no Spark)."""

import datetime

from adf_merge.changefile import CF_TYPES, marker_is_stale, schema_drift


def test_decimal_coordinates_accepted():
    assert schema_drift(dict(CF_TYPES)) is None


def test_double_coordinates_accepted():
    assert schema_drift({**CF_TYPES, "LATITUDE": "double", "LONGITUDE": "double"}) is None


def test_other_coordinate_type_rejected():
    msg = schema_drift({**CF_TYPES, "LATITUDE": "string"})
    assert msg and "type_changed=['LATITUDE']" in msg


def test_other_column_type_change_still_rejected():
    msg = schema_drift({**CF_TYPES, "CHANGE": "int"})
    assert msg and "type_changed=['CHANGE']" in msg


def test_missing_and_extra_columns_rejected():
    types = {k: v for k, v in CF_TYPES.items() if k != "CHG_CODE"}
    types["NEW_COL"] = "string"
    msg = schema_drift(types)
    assert msg and "missing=['CHG_CODE']" in msg and "extra=['NEW_COL']" in msg


def test_changefile_marked_before_a_main_remark_is_stale():
    # main re-marked by NJ_DELETION at 17:19; the changefile still carries the 07:05 marker
    assert marker_is_stale("2026-11-05T07:05:23.000Z", "2026-11-05T17:19:21.000Z")
    assert marker_is_stale(datetime.datetime(2026, 11, 5, 7, 5), datetime.datetime(2026, 11, 5, 17, 19))


def test_changefile_marked_after_or_near_the_main_marker_is_fresh():
    assert not marker_is_stale("2026-11-05T17:20:28.000Z", "2026-11-05T17:19:21.000Z")  # a minute later
    assert not marker_is_stale("2026-11-05T07:05:10.000Z", "2026-11-05T07:05:23.000Z")  # seconds earlier: within slack


def test_unknown_or_incomparable_timestamps_are_never_stale():
    assert not marker_is_stale(None, "2026-11-05T17:19:21.000Z")
    assert not marker_is_stale("2026-11-05T07:05:23.000Z", None)
    assert not marker_is_stale(datetime.datetime(2026, 11, 5), datetime.datetime(2026, 11, 6, tzinfo=datetime.timezone.utc))
