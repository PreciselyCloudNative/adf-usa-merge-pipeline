"""Pure-logic tests for changefile.py (no Spark)."""

from adf_merge.changefile import CF_TYPES, schema_drift


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
