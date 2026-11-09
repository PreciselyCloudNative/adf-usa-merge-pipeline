"""Gold schema: Address Fabric USA as of every applied release, as views only (no data copies).

silver.adf_usa_current holds each live record at its last change. silver.adf_usa_history holds the versions that a later
release updated or deleted, each valid from release_seq (inclusive) to valid_to_seq (exclusive). Together they give the
exact table as of release N:

    live records last changed at or before N       (silver.adf_usa_current WHERE release_seq <= N)
    UNION ALL
    versions that were still valid at N            (silver.adf_usa_history WHERE release_seq <= N < valid_to_seq)

`refresh` creates one view per COMPLETE release (gold.adf_usa_<release with dots as underscores>), points
gold.adf_usa_latest at silver.adf_usa_current, tags the newest view release_status=latest (the others archived) and
drops views of releases that are no longer COMPLETE (rolled back). It is idempotent and cheap, so every job run calls it.
"""

import re

from adf_merge.release import Params
from adf_merge.schema import SOURCE_COLUMNS

RELEASE_RE = re.compile(r"^\d{4}\.\d{1,2}(\.\d+)?$")
VIEW_RE = re.compile(r"^adf_usa_\d{4}_\d{1,2}(_\d+)?$")
TAG = "release_status"
VIEW_COLUMNS = SOURCE_COLUMNS + ["status", "release_version", "release_seq", "changed_at", "reappeared"]


def view_name(release_version: str) -> str:
    if not RELEASE_RE.match(release_version or ""):
        raise ValueError(f"release version {release_version!r} cannot be used in a view name")
    return "adf_usa_" + release_version.replace(".", "_")


def _schemas(p: Params) -> tuple[str, str]:
    return f"{p.catalog}.{p.silver_schema}", f"{p.catalog}.{p.gold_schema}"


def release_view_sql(p: Params, release_version: str, release_seq: int) -> str:
    silver, gold = _schemas(p)
    cols = ", ".join(VIEW_COLUMNS)
    return f"""CREATE OR REPLACE VIEW {gold}.{view_name(release_version)}
COMMENT 'Address Fabric USA as of release {release_version} (release_seq {release_seq}). A view over silver, no data copy.'
AS SELECT {cols} FROM {silver}.adf_usa_current WHERE release_seq <= {release_seq}
UNION ALL
SELECT {cols} FROM {silver}.adf_usa_history WHERE release_seq <= {release_seq} AND valid_to_seq > {release_seq}"""


def latest_view_sql(p: Params, release_version: str, release_seq: int) -> str:
    silver, gold = _schemas(p)
    return f"""CREATE OR REPLACE VIEW {gold}.adf_usa_latest
COMMENT 'LATEST: Address Fabric USA as of release {release_version} (release_seq {release_seq}). Always the newest applied release.'
AS SELECT {", ".join(VIEW_COLUMNS)} FROM {silver}.adf_usa_current"""


def stale_views(existing: list[str], wanted: set[str]) -> list[str]:
    """Release views present in gold that no COMPLETE release asks for (e.g. after a rollback)."""
    return sorted(v for v in existing if VIEW_RE.match(v) and v not in wanted)


def _tag(spark, gold: str, view: str, value: str, errors: list[str]) -> None:
    try:
        spark.sql(f"ALTER VIEW {gold}.{view} SET TAGS ('{TAG}' = '{value}')")
    except Exception as e:  # noqa: BLE001 - tags are metadata; the views and the alias are what consumers rely on
        errors.append(f"{view}: {str(e)[:160]}")


def refresh(spark, p: Params) -> dict:
    _, gold = _schemas(p)
    rels = [
        r.asDict()
        for r in spark.sql(
            f"SELECT release_version, release_seq FROM {p.release_log} WHERE state = 'COMPLETE' ORDER BY release_seq"
        ).collect()
    ]
    if not rels:
        return {"views": 0}
    latest = rels[-1]
    existing = [r["tableName"] for r in spark.sql(f"SHOW TABLES IN {gold}").collect()]
    for r in rels:
        spark.sql(release_view_sql(p, r["release_version"], r["release_seq"]))
    spark.sql(latest_view_sql(p, latest["release_version"], latest["release_seq"]))
    dropped = stale_views(existing, {view_name(r["release_version"]) for r in rels})
    for v in dropped:
        spark.sql(f"DROP VIEW IF EXISTS {gold}.{v}")
    tag_errors: list[str] = []
    for r in rels:
        _tag(spark, gold, view_name(r["release_version"]), "latest" if r is latest else "archived", tag_errors)
    _tag(spark, gold, "adf_usa_latest", "latest", tag_errors)
    out = {"views": len(rels), "latest": latest["release_version"], "dropped": dropped}
    if tag_errors:
        out["tag_errors"] = tag_errors
    return out
