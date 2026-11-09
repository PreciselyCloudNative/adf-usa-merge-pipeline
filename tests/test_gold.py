"""Pure-logic tests for gold.py and the history archive SQL (no Spark)."""

import pytest

from adf_merge import gold
from adf_merge.release import Params, history_sql
from adf_merge.schema import SOURCE_COLUMNS


def test_view_name_replaces_dots():
    assert gold.view_name("2026.10.3") == "adf_usa_2026_10_3"
    assert gold.view_name("2026.11.0") == "adf_usa_2026_11_0"
    assert gold.view_name("2027.1") == "adf_usa_2027_1"


@pytest.mark.parametrize("bad", ["", "latest", "2026.10.3; DROP TABLE x", "2026-10-3", None])
def test_view_name_rejects_anything_else(bad):
    with pytest.raises(ValueError):
        gold.view_name(bad)


def test_release_view_is_current_plus_still_valid_history():
    sql = gold.release_view_sql(Params(catalog="c"), "2026.10.2", 2)
    assert "CREATE OR REPLACE VIEW c.gold.adf_usa_2026_10_2" in sql
    assert "FROM c.silver.adf_usa_current WHERE release_seq <= 2" in sql
    assert "FROM c.silver.adf_usa_history WHERE release_seq <= 2 AND valid_to_seq > 2" in sql
    assert "UNION ALL" in sql and "SELECT *" not in sql
    for col in SOURCE_COLUMNS:
        assert col in sql
    assert "row_hash" not in sql  # same surface as silver.adf_usa_current


def test_latest_alias_reads_silver_current_directly():
    sql = gold.latest_view_sql(Params(catalog="c"), "2026.10.3", 3)
    assert "VIEW c.gold.adf_usa_latest" in sql and "FROM c.silver.adf_usa_current" in sql
    assert "UNION" not in sql and "LATEST" in sql and "2026.10.3" in sql


def test_custom_schema_names():
    sql = gold.release_view_sql(Params(catalog="c", silver_schema="s", gold_schema="g"), "2026.10.3", 3)
    assert "c.g.adf_usa_2026_10_3" in sql and "c.s.adf_usa_current" in sql and "c.s.adf_usa_history" in sql


def test_stale_views_only_touches_release_views():
    existing = ["adf_usa_2026_10_3", "adf_usa_2026_10_4", "adf_usa_latest", "adf_usa_versions", "other_view"]
    assert gold.stale_views(existing, {"adf_usa_2026_10_3"}) == ["adf_usa_2026_10_4"]
    assert gold.stale_views(existing, set(existing)) == []


def test_history_sql_archives_live_versions_of_changed_keys():
    sql = history_sql(Params(catalog="c", release_version="2026.10.4"), 4, 17)
    assert "INSERT INTO c.bronze.adf_usa_history" in sql
    assert "c.bronze.adf_usa_current VERSION AS OF 17" in sql
    assert "release_seq = 4" in sql and "4 AS valid_to_seq" in sql and "'2026.10.4' AS valid_to_version" in sql
    assert "o.status <> 'DELETED' AND o.release_seq < 4" in sql
    for col in SOURCE_COLUMNS:
        assert f"o.{col}" in sql
