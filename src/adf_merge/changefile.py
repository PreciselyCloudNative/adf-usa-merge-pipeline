"""Apply a release from Precisely's changefile (primary path); the hash-diff MERGE is the fallback and the audit.

The changefile (`adf_usa_changefile`) is replaced in full for every release and lists only that release's changes:
CHANGE = A (added), U (updated), D (deleted). It lands about two days after the main table's release marker, and
carries LATITUDE/LONGITUDE as decimal(11,6), which we cast to double (hash-identical to the main table, checked
on release 2026.10.2: 12,856 of 12,856 rows).

Decision per release:
    changefile ready + consistent  -> apply it, validate, audit against the pinned main table  (method CHANGEFILE)
    changefile not there yet       -> defer up to `changefile_wait_days` after the release date
    anything else                  -> undo whatever was applied and run the hash-diff MERGE     (method MERGE)
"""

import datetime
from dataclasses import replace

from adf_merge import release as rel
from adf_merge.release import GateError, Params
from adf_merge.schema import HASH_COLUMNS, KEY_COLUMN, SOURCE_COLUMNS

CF_TYPES = {
    **rel.EXPECTED_TYPES,
    "LATITUDE": "decimal(11,6)",
    "LONGITUDE": "decimal(11,6)",
    "CHANGE": "string",
    "CHG_CODE": "string",
    "PBKEY_ALT": "string",
}
CHANGE_TYPES = {"A": "added", "U": "updated", "D": "deleted"}


class ChangefileNotReady(GateError):
    """The changefile does not (yet) describe this release. `deferrable` = it may still arrive."""

    def __init__(self, reason: str, deferrable: bool):
        super().__init__(reason)
        self.deferrable = deferrable


# ---------------------------------------------------------------------------------------------
# Pure helpers
# ---------------------------------------------------------------------------------------------


def changefile_merge_sql(p: Params, release_seq: int, view: str = "adf_cf_h") -> str:
    """Apply A/U/D by key. No NOT MATCHED BY SOURCE clause: the changefile is partial, not a snapshot."""
    m = rel.merge_parts(p, release_seq)
    return f"""
MERGE INTO {p.current} t
USING {view} s
ON t.{KEY_COLUMN} = s.{KEY_COLUMN}
WHEN MATCHED AND s.CHANGE = 'D' AND t.status <> 'DELETED' THEN UPDATE SET
  {m['null_attrs']}, row_hash = NULL, status = 'DELETED', reappeared = false, {m['stamp']}
WHEN MATCHED AND s.CHANGE = 'A' AND t.status = 'DELETED' THEN UPDATE SET
  {m['set_attrs']}, row_hash = s.row_hash, status = 'ADDED', reappeared = true, {m['stamp']}
WHEN MATCHED AND s.CHANGE = 'U' AND t.status <> 'DELETED' AND t.row_hash <> s.row_hash THEN UPDATE SET
  {m['set_attrs']}, row_hash = s.row_hash, status = 'UPDATED', reappeared = false, {m['stamp']}
WHEN NOT MATCHED AND s.CHANGE = 'A' THEN INSERT ({m['insert_cols']}) VALUES ({m['insert_vals']})
"""


def expected_live_rows(live: int, counts: dict) -> int:
    return live + counts["added"] - counts["deleted"]


def is_deferrable(label: dict, wait_days: int, today: datetime.date | None = None) -> bool:
    d = label.get("release_date")
    if d is None or wait_days <= 0:
        return False
    if isinstance(d, str):
        d = datetime.date.fromisoformat(d[:10])
    return ((today or datetime.date.today()) - d).days < wait_days


# ---------------------------------------------------------------------------------------------
# Spark steps
# ---------------------------------------------------------------------------------------------


def changefile_state(spark, p: Params, release_version: str) -> dict:
    """Is the changefile at this release? Returns the version to read (the vendor's marker commit)."""
    from adf_merge import detect

    try:
        props = detect._table_props(spark, p.changefile_table)
        history = detect.parse_vintage_commits(detect._history(spark, p.changefile_table))
    except Exception as e:  # noqa: BLE001 - table missing / not shared yet
        raise ChangefileNotReady(f"changefile unreadable: {str(e)[:200]}", deferrable=True)
    vintage = props.get(detect.VINTAGE_PROP)
    if vintage != release_version:
        try:
            older = detect.vintage_key(vintage) < detect.vintage_key(release_version)
        except detect.DetectError:
            older = False
        raise ChangefileNotReady(f"changefile is at vintage {vintage!r}, release is {release_version!r}", deferrable=older)
    marks = [c for c in history if c["vintage"] == release_version]
    if not marks:
        raise ChangefileNotReady("no release marker commit in changefile history", deferrable=True)
    count = props.get(detect.COUNT_PROP)
    return {"version": marks[-1]["version"], "count": int(count) if count not in (None, "") else None}


def read_changefile(spark, p: Params, version: int):
    return spark.sql(f"SELECT * FROM {p.changefile_table} VERSION AS OF {version}")


def gate_changefile(cf, state: dict) -> dict:
    from pyspark.sql import functions as F

    types = {f.name: f.dataType.simpleString() for f in cf.schema}
    if types != CF_TYPES:
        missing = sorted(set(CF_TYPES) - set(types))
        extra = sorted(set(types) - set(CF_TYPES))
        changed = sorted(c for c in set(types) & set(CF_TYPES) if types[c] != CF_TYPES[c])
        raise GateError(f"changefile schema drift: missing={missing} extra={extra} type_changed={changed}")
    r = cf.agg(
        F.count("*").alias("n"),
        F.countDistinct(KEY_COLUMN).alias("keys"),
        F.sum(F.col(KEY_COLUMN).isNull().cast("int")).alias("null_keys"),
        F.sum((~F.col("CHANGE").isin("A", "U", "D") | F.col("CHANGE").isNull()).cast("int")).alias("bad_change"),
        *[F.sum((F.col("CHANGE") == k).cast("int")).alias(v) for k, v in CHANGE_TYPES.items()],
    ).collect()[0].asDict()
    if r["null_keys"] or r["keys"] != r["n"]:
        raise GateError(f"changefile keys not unique/non-null: {r}")
    if r["bad_change"]:
        raise GateError(f"{r['bad_change']} changefile rows have a CHANGE other than A/U/D")
    if state["count"] is not None and r["n"] != state["count"]:
        raise GateError(f"changefile rows {r['n']} != its precisely.data.count {state['count']}")
    return {"added": r["added"], "updated": r["updated"], "deleted": r["deleted"], "rows": r["n"]}


def hashed_changefile(cf):
    from pyspark.sql import functions as F

    cast = cf.withColumn("LATITUDE", F.col("LATITUDE").cast("double")).withColumn("LONGITUDE", F.col("LONGITUDE").cast("double"))
    return cast.select(*SOURCE_COLUMNS, rel.row_hash_expr().alias("row_hash"), "CHANGE")


def precheck(spark, p: Params, cf_h, counts: dict, label: dict) -> dict:
    """The changefile must be consistent with our current state, or we do not trust it."""
    from pyspark.sql import functions as F

    cur = spark.table(p.current).select(F.col(KEY_COLUMN).alias("ckey"), F.col("status").alias("cstatus"))
    live = F.col("cstatus").isNotNull() & (F.col("cstatus") != "DELETED")
    v = (
        cf_h.select(KEY_COLUMN, "CHANGE")
        .join(cur, F.col(KEY_COLUMN) == F.col("ckey"), "left")
        .agg(
            F.sum(((F.col("CHANGE") == "A") & live).cast("int")).alias("add_already_live"),
            F.sum(((F.col("CHANGE") == "U") & ~live).cast("int")).alias("update_not_live"),
            F.sum(((F.col("CHANGE") == "D") & ~live).cast("int")).alias("delete_not_live"),
        )
        .collect()[0]
        .asDict()
    )
    problems = [f"{k}={n}" for k, n in v.items() if n]
    live_rows = spark.table(p.current).filter("status <> 'DELETED'").count()
    after = expected_live_rows(live_rows, counts)
    if label.get("vendor_row_count") is not None and after != label["vendor_row_count"]:
        problems.append(f"live rows would be {after}, vendor_row_count is {label['vendor_row_count']}")
    if problems:
        raise GateError("changefile precondition failed: " + ", ".join(problems))
    return {"live_rows_before": live_rows, "live_rows_after": after}


def run_release_changefile(spark, p: Params, plan: dict) -> dict:
    from adf_merge import detect

    label, seq = plan["label"], plan["release_seq"]
    pin = label["source_delta_version"]
    state = changefile_state(spark, p, p.release_version)
    cf = read_changefile(spark, p, state["version"])
    counts = gate_changefile(cf, state)
    cf_h = hashed_changefile(cf)
    pre = precheck(spark, p, cf_h, counts, label)

    rows_expected = pre["live_rows_after"]
    if not plan["existing_log"]:
        spark.sql(
            f"""INSERT INTO {p.release_log} (release_version, release_seq, run_id, state, source_delta_version,
                source_row_count, hash_spec_version, started_at)
            VALUES ('{p.release_version}', {seq}, '{p.run_id}', 'STARTED', {pin}, {rows_expected},
                    {rel.HASH_SPEC_VERSION}, current_timestamp())"""
        )
    else:
        rel._log_update(spark, p, run_id=p.run_id, state="STARTED", error=None)

    try:
        already = spark.table(p.current).filter(f"release_seq = {seq}").limit(1).count() > 0
        applied = {}
        if not already:
            cf_h.createOrReplaceTempView("adf_cf_h")
            spark.sql(changefile_merge_sql(p, seq))
            applied = {"current_table_version": spark.sql(f"DESCRIBE HISTORY {p.current} LIMIT 1").collect()[0]["version"]}
            rel._log_update(spark, p, state="APPLIED", current_table_version=applied["current_table_version"])
        rel.publish_change_log(spark, p, seq)

        audited = p.audit and pin is not None and detect.pin_readable(spark, p, pin)
        src_h = rel.hashed_source(rel.read_source(spark, p, pin)) if audited else None
        v = rel.validate(spark, p, seq, label, src_h)
        problems = list(v["problems"])
        got = v["counts"]
        for key in ("added", "updated", "deleted"):
            if got[key] != counts[key]:
                problems.append(f"{key}: applied {got[key]} != changefile {counts[key]}")
        if v["row_count"] != rows_expected:
            problems.append(f"live rows {v['row_count']} != expected {rows_expected}")
        if problems:
            raise RuntimeError("validation failed: " + "; ".join(problems))
        rel._log_update(spark, p, state="COMPLETE", added=got["added"], updated=got["updated"], deleted=got["deleted"],
                        method="CHANGEFILE", fallback_reason=None, audit="PASSED" if audited else "SKIPPED",
                        finished_at=True)
        return {"release": p.release_version, "result": "complete", "method": "CHANGEFILE", "release_seq": seq,
                "audit": "PASSED" if audited else "SKIPPED", "counts": got, "expected": v["expected"],
                "row_count": v["row_count"], **applied}
    except Exception as e:  # noqa: BLE001
        rel._log_update(spark, p, state="FAILED", error=str(e)[:900], finished_at=True)
        raise


def run_release_auto(spark, p: Params) -> dict:
    """Changefile first; undo and fall back to the hash-diff MERGE on any problem."""
    plan = rel.plan_release(spark, p)
    if plan["noop"]:
        return {"release": p.release_version, "result": "noop", "reason": plan["reason"]}
    if p.dry_run or not p.use_changefile:
        return rel.run_release(spark, replace(p, method="MERGE", fallback_reason=None))
    try:
        return run_release_changefile(spark, p, plan)
    except ChangefileNotReady as e:
        if e.deferrable and is_deferrable(plan["label"], p.changefile_wait_days):
            return {"release": p.release_version, "result": "deferred", "reason": str(e)}
        reason = f"changefile not ready: {e}"
    except Exception as e:  # noqa: BLE001
        reason = f"changefile failed: {e}"
    if spark.table(p.release_log).filter(f"release_version = '{p.release_version}'").limit(1).count():
        rel.rollback_release(spark, p)
    return rel.run_release(spark, replace(p, method="MERGE", fallback_reason=reason[:500]))
