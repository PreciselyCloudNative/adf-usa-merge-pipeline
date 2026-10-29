"""Physical clean-up after releases that delete records (NJ privacy removals must not linger in data files).

MERGE with deletion vectors only soft-deletes the old row version, so the removed address stays in the old
Parquet file until REORG ... APPLY (PURGE) rewrites it, and in older table versions until VACUUM (7-day retention).
"""

from adf_merge.release import Params


def purge_deleted(spark, p: Params) -> None:
    spark.sql(f"REORG TABLE {p.current} APPLY (PURGE)")


def vacuum(spark, p: Params) -> None:
    for table in (p.current, p.change_log):
        spark.sql(f"VACUUM {table}")


def after_releases(spark, p: Params, applied: list[dict]) -> dict:
    done = [r for r in applied if r.get("result") == "complete"]
    did = {"purged": False, "vacuumed": False}
    if any(r["counts"]["deleted"] > 0 for r in done):
        purge_deleted(spark, p)
        did["purged"] = True
    if done:
        vacuum(spark, p)
        did["vacuumed"] = True
    return did
