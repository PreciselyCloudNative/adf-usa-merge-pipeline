"""Apply one Precisely release of adf_usa to adf_usa_current (hash-diff MERGE).

Flow (idempotent per release_version):
    plan      -> look up the release_label pin, assign the next release_seq, refuse if already COMPLETE
    gate      -> read the source pinned to that Delta version, check schema / keys / coordinates / row count
    dry run   -> optionally stop here with a full diff preview (no writes)
    apply     -> one atomic MERGE into adf_usa_current (state APPLIED, rollback pointer recorded)
    publish   -> append the slim change log for this release
    validate  -> counts vs expectations, row count and content fingerprint vs the pinned source (state COMPLETE)

Rollback: RESTORE adf_usa_current to the version recorded as `current_table_version` of the PREVIOUS release.
"""

import json
import uuid
from dataclasses import dataclass, field

from adf_merge.hashing import HASH_SPEC_VERSION, row_hash_expr
from adf_merge.schema import HASH_COLUMNS, KEY_COLUMN, SOURCE_COLUMNS

DEFAULT_SOURCE_TABLE = "precisely_product_share.precisely_db_property_graph.adf_usa"
DEFAULT_CHANGEFILE_TABLE = "precisely_product_share.precisely_db_property_graph.adf_usa_changefile"
ATTR_COLUMNS = [c for c in SOURCE_COLUMNS if c != KEY_COLUMN]
EXPECTED_TYPES = {KEY_COLUMN: "string", **{n: t for n, t in HASH_COLUMNS}}


class GateError(RuntimeError):
    """A safety gate failed; nothing was applied."""


@dataclass
class Params:
    catalog: str
    release_version: str = ""
    bronze_schema: str = "bronze"
    ops_schema: str = "ops"
    dry_run: bool = False
    source_table: str = DEFAULT_SOURCE_TABLE
    expect_added: int | None = None
    expect_updated: int | None = None
    expect_deleted: int | None = None
    deleted_tolerance: int = 50
    max_row_count_change_pct: float = 1.0
    allow_large_change: bool = False
    changefile_table: str = DEFAULT_CHANGEFILE_TABLE
    use_changefile: bool = True
    audit: bool = True
    changefile_wait_days: int = 3
    method: str = "MERGE"
    fallback_reason: str | None = None
    run_id: str = field(default_factory=lambda: uuid.uuid4().hex[:12])

    @property
    def current(self):
        return f"{self.catalog}.{self.bronze_schema}.adf_usa_current"

    @property
    def change_log(self):
        return f"{self.catalog}.{self.bronze_schema}.adf_usa_change_log"

    @property
    def release_label(self):
        return f"{self.catalog}.{self.ops_schema}.release_label"

    @property
    def release_log(self):
        return f"{self.catalog}.{self.ops_schema}.release_log"


# ---------------------------------------------------------------------------------------------
# Pure helpers (unit-tested without Spark)
# ---------------------------------------------------------------------------------------------


def check_counts(actual: dict, expected: dict, deleted_tolerance: int) -> list[str]:
    """Compare ADDED/UPDATED exactly and DELETED within tolerance. Returns a list of problems."""
    problems = []
    for key in ("added", "updated"):
        exp = expected.get(key)
        if exp is not None and actual[key] != exp:
            problems.append(f"{key}: got {actual[key]}, expected {exp}")
    exp = expected.get("deleted")
    if exp is not None and abs(actual["deleted"] - exp) > deleted_tolerance:
        problems.append(f"deleted: got {actual['deleted']}, expected {exp} +/- {deleted_tolerance}")
    return problems


def row_count_change_pct(source_rows: int, current_live_rows: int) -> float:
    if current_live_rows == 0:
        return 100.0
    return abs(source_rows - current_live_rows) * 100.0 / current_live_rows


def merge_parts(p: Params, release_seq: int) -> dict:
    """SQL fragments shared by the hash-diff MERGE and the changefile MERGE."""
    stamp = (
        f"release_version = '{p.release_version}', release_seq = {release_seq}, "
        f"changed_at = current_timestamp(), hash_spec_version = {HASH_SPEC_VERSION}"
    )
    insert_cols = ", ".join(
        SOURCE_COLUMNS
        + ["row_hash", "hash_spec_version", "status", "release_version", "release_seq", "changed_at", "reappeared"]
    )
    insert_vals = ", ".join(
        [f"s.{c}" for c in SOURCE_COLUMNS]
        + ["s.row_hash", str(HASH_SPEC_VERSION), "'ADDED'", f"'{p.release_version}'", str(release_seq),
           "current_timestamp()", "false"]
    )
    return {
        "stamp": stamp,
        "set_attrs": ", ".join(f"{c} = s.{c}" for c in ATTR_COLUMNS),
        "null_attrs": ", ".join(f"{c} = NULL" for c in ATTR_COLUMNS),
        "insert_cols": insert_cols,
        "insert_vals": insert_vals,
    }


def merge_sql(p: Params, release_seq: int, view: str = "adf_src_h") -> str:
    """The single atomic MERGE. Unchanged rows match no clause, so they are never rewritten.

    Target columns in SET are unqualified (standard Delta MERGE syntax), source values use alias s.
    """
    m = merge_parts(p, release_seq)
    return f"""
MERGE INTO {p.current} t
USING {view} s
ON t.{KEY_COLUMN} = s.{KEY_COLUMN}
WHEN MATCHED AND t.status = 'DELETED' THEN UPDATE SET
  {m['set_attrs']}, row_hash = s.row_hash, status = 'ADDED', reappeared = true, {m['stamp']}
WHEN MATCHED AND t.row_hash <> s.row_hash THEN UPDATE SET
  {m['set_attrs']}, row_hash = s.row_hash, status = 'UPDATED', reappeared = false, {m['stamp']}
WHEN NOT MATCHED THEN INSERT ({m['insert_cols']}) VALUES ({m['insert_vals']})
WHEN NOT MATCHED BY SOURCE AND t.status <> 'DELETED' THEN UPDATE SET
  {m['null_attrs']}, row_hash = NULL, status = 'DELETED', reappeared = false, {m['stamp']}
"""


# ---------------------------------------------------------------------------------------------
# Spark steps
# ---------------------------------------------------------------------------------------------


def _log_update(spark, p: Params, **fields):
    sets = []
    for k, v in fields.items():
        if v is None:
            sets.append(f"{k} = NULL")
        elif isinstance(v, str):
            sets.append(f"{k} = '{v.replace(chr(39), chr(39) * 2)}'")
        elif k.endswith("_at"):
            sets.append(f"{k} = current_timestamp()")
        else:
            sets.append(f"{k} = {v}")
    spark.sql(f"UPDATE {p.release_log} SET {', '.join(sets)} WHERE release_version = '{p.release_version}'")


def plan_release(spark, p: Params) -> dict:
    label = spark.sql(
        f"SELECT * FROM {p.release_label} WHERE release_version = '{p.release_version}'"
    ).collect()
    if not label:
        raise GateError(f"release {p.release_version} not found in {p.release_label}")
    label = label[0].asDict()
    if label["source_delta_version"] is None:
        raise GateError(f"release {p.release_version} has no source_delta_version pin")

    log = {r["release_version"]: r.asDict() for r in spark.table(p.release_log).collect()}
    mine = log.get(p.release_version)
    if mine and mine["state"] == "COMPLETE":
        return {"noop": True, "reason": "already COMPLETE", "label": label}

    done_seqs = [r["release_seq"] for r in log.values() if r["state"] == "COMPLETE"]
    last_seq = max(done_seqs) if done_seqs else 0
    seq = mine["release_seq"] if mine else last_seq + 1
    if seq != last_seq + 1:
        raise GateError(f"release_seq {seq} is not next after last COMPLETE {last_seq}; apply releases in order")
    return {"noop": False, "label": label, "release_seq": seq, "existing_log": mine}


def read_source(spark, p: Params, pin: int):
    return spark.sql(f"SELECT * FROM {p.source_table} VERSION AS OF {pin}")


def gate_source(spark, p: Params, src, label: dict) -> dict:
    from pyspark.sql import functions as F

    types = {f.name: f.dataType.simpleString() for f in src.schema}
    if types != EXPECTED_TYPES:
        missing = sorted(set(EXPECTED_TYPES) - set(types))
        extra = sorted(set(types) - set(EXPECTED_TYPES))
        changed = sorted(c for c in set(types) & set(EXPECTED_TYPES) if types[c] != EXPECTED_TYPES[c])
        raise GateError(f"source schema drift: missing={missing} extra={extra} type_changed={changed}")

    s = src.agg(
        F.count("*").alias("n"),
        F.countDistinct(KEY_COLUMN).alias("distinct_keys"),
        F.sum(F.col(KEY_COLUMN).isNull().cast("int")).alias("null_keys"),
        F.sum(
            (
                F.isnan("LATITUDE")
                | F.isnan("LONGITUDE")
                | F.col("LATITUDE").isin(float("inf"), float("-inf"))
                | F.col("LONGITUDE").isin(float("inf"), float("-inf"))
            ).cast("int")
        ).alias("nonfinite"),
    ).collect()[0].asDict()
    if s["null_keys"] or s["distinct_keys"] != s["n"]:
        raise GateError(f"source keys not unique/non-null: {s}")
    if s["nonfinite"]:
        raise GateError(f"{s['nonfinite']} source rows have non-finite coordinates")

    if label.get("vendor_row_count") is not None and s["n"] != label["vendor_row_count"]:
        raise GateError(f"source rows {s['n']} != vendor_row_count {label['vendor_row_count']}")

    cur = spark.table(p.current)
    specs = [r[0] for r in cur.filter("status <> 'DELETED'").select("hash_spec_version").distinct().collect()]
    if specs != [HASH_SPEC_VERSION]:
        raise GateError(f"current hash_spec_version {specs} != code {HASH_SPEC_VERSION}; migrate first")

    live = cur.filter("status <> 'DELETED'").count()
    pct = row_count_change_pct(s["n"], live)
    if pct > p.max_row_count_change_pct and not p.allow_large_change:
        raise GateError(f"row count moves {pct:.2f}% (> {p.max_row_count_change_pct}%): source {s['n']} vs live {live}")
    return {"source_rows": s["n"], "current_live_rows": live, "row_count_change_pct": round(pct, 4)}


def hashed_source(src):
    return src.select(*SOURCE_COLUMNS, row_hash_expr().alias("row_hash"))


def diff_preview(spark, p: Params, src_h) -> dict:
    """Read-only classification of what the MERGE would do."""
    from pyspark.sql import functions as F

    cur = spark.table(p.current).select(
        F.col(KEY_COLUMN).alias("ckey"), F.col("row_hash").alias("chash"), F.col("status").alias("cstatus")
    )
    s = src_h.select(F.col(KEY_COLUMN).alias("skey"), F.col("row_hash").alias("shash"))
    j = s.join(cur, F.col("skey") == F.col("ckey"), "full_outer")
    kind = (
        F.when(F.col("ckey").isNull(), "added")
        .when(F.col("skey").isNull() & (F.col("cstatus") != "DELETED"), "deleted")
        .when(F.col("skey").isNull(), "already_deleted")
        .when(F.col("cstatus") == "DELETED", "reappeared")
        .when(F.col("shash") != F.col("chash"), "updated")
        .otherwise("unchanged")
    )
    out = {r["kind"]: r["n"] for r in j.select(kind.alias("kind")).groupBy("kind").agg(F.count("*").alias("n")).collect()}
    for k in ("added", "updated", "deleted", "reappeared", "unchanged", "already_deleted"):
        out.setdefault(k, 0)
    return out


def apply_release(spark, p: Params, plan: dict, src_h) -> dict:
    seq = plan["release_seq"]
    src_h.createOrReplaceTempView("adf_src_h")
    spark.sql(merge_sql(p, seq))
    version = spark.sql(f"DESCRIBE HISTORY {p.current} LIMIT 1").collect()[0]["version"]
    return {"current_table_version": version}


def publish_change_log(spark, p: Params, seq: int) -> None:
    existing = spark.table(p.change_log).filter(f"release_seq = {seq}").limit(1).count()
    if existing:
        return  # re-run after a crash between MERGE and publish
    spark.sql(
        f"""INSERT INTO {p.change_log}
SELECT release_seq, release_version, PBKEY, status AS change_type, row_hash, reappeared, changed_at
FROM {p.current} WHERE release_seq = {seq}"""
    )


def release_counts(spark, p: Params, seq: int) -> dict:
    rows = spark.sql(
        f"SELECT change_type, reappeared, count(*) n FROM {p.change_log} WHERE release_seq = {seq} GROUP BY 1, 2"
    ).collect()
    out = {"added": 0, "updated": 0, "deleted": 0, "reappeared": 0}
    for r in rows:
        key = {"ADDED": "added", "UPDATED": "updated", "DELETED": "deleted"}[r["change_type"]]
        out[key] += r["n"]
        if r["reappeared"]:
            out["reappeared"] += r["n"]
    return out


def fingerprint(df):
    from pyspark.sql import functions as F

    r = df.agg(
        F.count("*").alias("n"),
        F.sum(F.xxhash64(F.col(KEY_COLUMN), F.col("row_hash")).cast("decimal(38,0)")).alias("fp"),
    ).collect()[0]
    return r["n"], r["fp"]


def validate(spark, p: Params, seq: int, label: dict, src_h=None) -> dict:
    """Counts vs expectations; with src_h also row count + content fingerprint vs the pinned source."""
    counts = release_counts(spark, p, seq)
    expected = {
        "added": p.expect_added if p.expect_added is not None else label.get("vendor_added"),
        "updated": p.expect_updated if p.expect_updated is not None else label.get("vendor_updated"),
        "deleted": p.expect_deleted if p.expect_deleted is not None else label.get("vendor_deleted"),
    }
    problems = check_counts(counts, expected, p.deleted_tolerance)
    live = spark.table(p.current).filter("status <> 'DELETED'")
    cur_fp = fingerprint(live)
    if src_h is not None:
        src_fp = fingerprint(src_h)
        if cur_fp != src_fp:
            problems.append(f"current != pinned source (rows/fingerprint): {cur_fp} vs {src_fp}")
    return {"counts": counts, "expected": expected, "problems": problems, "row_count": cur_fp[0]}


def rollback_release(spark, p: Params) -> dict:
    """Undo the newest release: RESTORE current to the previous release's pointer, drop its change-log rows.

    Only the newest release can be rolled back. The release_log row is kept as ROLLED_BACK (audit) and the
    release can then be re-run.
    """
    log = sorted((r.asDict() for r in spark.table(p.release_log).collect()), key=lambda r: r["release_seq"])
    target = next((r for r in log if r["release_version"] == p.release_version), None)
    if not target:
        raise GateError(f"release {p.release_version} not in {p.release_log}")
    if target["release_seq"] != log[-1]["release_seq"]:
        raise GateError(f"{p.release_version} is not the newest release; only the newest can be rolled back")
    seq = target["release_seq"]
    prev = next((r for r in reversed(log) if r["release_seq"] == seq - 1 and r["state"] == "COMPLETE"), None)
    if not prev or prev["current_table_version"] is None:
        raise GateError(f"no rollback pointer: release_seq {seq - 1} is missing or has no current_table_version")
    restored = False
    if spark.table(p.current).filter(f"release_seq >= {seq}").limit(1).count():
        spark.sql(f"RESTORE TABLE {p.current} TO VERSION AS OF {prev['current_table_version']}")
        restored = True
    spark.sql(f"DELETE FROM {p.change_log} WHERE release_seq >= {seq}")
    _log_update(spark, p, state="ROLLED_BACK", current_table_version=None, finished_at=True)
    return {"release": p.release_version, "result": "rolled_back", "restored_to": prev["current_table_version"],
            "restored": restored}


def run_release(spark, p: Params) -> dict:
    """Entry point. Returns a JSON-serialisable result; raises GateError / RuntimeError on failure."""
    plan = plan_release(spark, p)
    if plan["noop"]:
        return {"release": p.release_version, "result": "noop", "reason": plan["reason"]}
    label, seq = plan["label"], plan["release_seq"]
    pin = label["source_delta_version"]

    src = read_source(spark, p, pin)
    gates = gate_source(spark, p, src, label)
    src_h = hashed_source(src)

    if p.dry_run:
        return {"release": p.release_version, "result": "dry_run", "release_seq": seq, "pin": pin, "gates": gates,
                "diff_preview": diff_preview(spark, p, src_h)}

    if not plan["existing_log"]:
        spark.sql(
            f"""INSERT INTO {p.release_log} (release_version, release_seq, run_id, state, source_delta_version,
                source_row_count, hash_spec_version, started_at)
            VALUES ('{p.release_version}', {seq}, '{p.run_id}', 'STARTED', {pin}, {gates['source_rows']},
                    {HASH_SPEC_VERSION}, current_timestamp())"""
        )
    else:
        _log_update(spark, p, run_id=p.run_id, state="STARTED", error=None)

    try:
        already = spark.table(p.current).filter(f"release_seq = {seq}").limit(1).count() > 0
        applied = {} if already else apply_release(spark, p, plan, src_h)
        if applied:
            _log_update(spark, p, state="APPLIED", current_table_version=applied["current_table_version"])
        publish_change_log(spark, p, seq)
        v = validate(spark, p, seq, label, src_h)
        if v["problems"]:
            raise RuntimeError("validation failed: " + "; ".join(v["problems"]))
        c = v["counts"]
        _log_update(spark, p, state="COMPLETE", added=c["added"], updated=c["updated"], deleted=c["deleted"],
                    method=p.method, fallback_reason=p.fallback_reason, audit="PASSED", finished_at=True)
        return {"release": p.release_version, "result": "complete", "release_seq": seq, "pin": pin, "gates": gates,
                "counts": c, "expected": v["expected"], "row_count": v["row_count"], **applied}
    except Exception as e:  # record, then re-raise
        _log_update(spark, p, state="FAILED", error=str(e)[:900], finished_at=True)
        raise


def to_json(result: dict) -> str:
    return json.dumps(result, default=str)
