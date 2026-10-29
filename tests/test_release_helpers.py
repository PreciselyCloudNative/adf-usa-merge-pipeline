"""Pure-logic tests for release.py (no Spark)."""

from adf_merge.release import Params, check_counts, merge_sql, row_count_change_pct
from adf_merge.schema import SOURCE_COLUMNS


def test_added_updated_must_match_exactly():
    problems = check_counts({"added": 9467, "updated": 3388, "deleted": 229}, {"added": 9467, "updated": 3389, "deleted": 229}, 0)
    assert problems == ["updated: got 3388, expected 3389"]


def test_deleted_within_tolerance():
    assert check_counts({"added": 1, "updated": 1, "deleted": 240}, {"added": 1, "updated": 1, "deleted": 229}, 50) == []
    assert check_counts({"added": 1, "updated": 1, "deleted": 300}, {"added": 1, "updated": 1, "deleted": 229}, 50)


def test_missing_expectations_are_skipped():
    assert check_counts({"added": 5, "updated": 6, "deleted": 7}, {}, 0) == []


def test_row_count_change_pct():
    assert row_count_change_pct(101, 100) == 1.0
    assert row_count_change_pct(100, 0) == 100.0


def test_merge_sql_shape():
    sql = merge_sql(Params(catalog="c", release_version="2026.10.2"), 2)
    assert "MERGE INTO c.bronze.adf_usa_current t" in sql
    assert "WHEN NOT MATCHED BY SOURCE AND t.status <> 'DELETED'" in sql
    assert "t.row_hash <> s.row_hash" in sql
    # every source column is inserted, nothing else sneaks in
    for col in SOURCE_COLUMNS:
        assert f"s.{col}" in sql
    # tombstones null every attribute and the hash
    assert sql.count("= NULL") == 31 + 1
    assert "release_seq = 2" in sql and "release_version = '2026.10.2'" in sql
