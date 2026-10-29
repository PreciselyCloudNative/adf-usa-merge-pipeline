"""Synthetic end-to-end scenarios, run on Databricks against a throwaway source table (never the real share).

Each scenario gets its own schemas (src / bronze / ops) in the target catalog, built from the repo DDL, a 2,000-row
synthetic adf_usa source, and a baseline load, then exercises the real release / detect code.
"""

import re
import uuid
from dataclasses import replace
from pathlib import Path

from adf_merge import detect, release
from adf_merge.hashing import HASH_SPEC_VERSION
from adf_merge.release import GateError, Params
from adf_merge.schema import HASH_COLUMNS, SOURCE_COLUMNS

N = 2000
BASE_VINTAGE = "2099.01.0"


def check(cond, msg):
    if not cond:
        raise AssertionError(msg)


def eq(actual, expected, what):
    check(actual == expected, f"{what}: got {actual!r}, expected {expected!r}")


def raises(exc, fn, contains=""):
    try:
        fn()
    except exc as e:
        check(contains in str(e), f"error {e!r} does not contain {contains!r}")
        return
    raise AssertionError(f"expected {exc.__name__}")


class Env:
    """Throwaway source + bronze + ops schemas for one scenario."""

    def __init__(self, spark, catalog: str, ddl_dir: str):
        self.spark, self.catalog = spark, catalog
        tag = uuid.uuid4().hex[:8]
        self.src_schema, self.bronze, self.ops = f"zz_syn_src_{tag}", f"zz_syn_bronze_{tag}", f"zz_syn_ops_{tag}"
        self.src = f"{catalog}.{self.src_schema}.adf_usa"
        for s in (self.src_schema, self.bronze, self.ops):
            spark.sql(f"CREATE SCHEMA {catalog}.{s}")
        for f in sorted(Path(ddl_dir).glob("*.sql")):
            text = f.read_text().replace("${catalog}.bronze.", f"{catalog}.{self.bronze}.")
            text = text.replace("${catalog}.ops.", f"{catalog}.{self.ops}.")
            for stmt in (s.strip() for s in text.split(";")):
                if stmt:
                    spark.sql(stmt)
        self._create_source()
        self.p = Params(catalog=catalog, bronze_schema=self.bronze, ops_schema=self.ops, source_table=self.src,
                        deleted_tolerance=0)

    # ---- source helpers -------------------------------------------------------------------
    @staticmethod
    def row_select(id_expr: str) -> str:
        cols = [f"concat('PB', lpad(cast({id_expr} as string), 7, '0')) AS PBKEY"]
        for name, typ in HASH_COLUMNS:
            if name == "LATITUDE":
                cols.append(f"cast(30.0 + {id_expr} * 0.001 as double) AS LATITUDE")
            elif name == "LONGITUDE":
                cols.append(f"cast(-90.0 - {id_expr} * 0.001 as double) AS LONGITUDE")
            elif name == "UNIT":
                cols.append(f"CASE WHEN {id_expr} % 3 = 0 THEN NULL ELSE concat('UNIT', {id_expr}) END AS UNIT")
            else:
                cols.append(f"concat('{name}_', {id_expr}) AS {name}")
        return ", ".join(cols)

    def key(self, i: int) -> str:
        return f"PB{i:07d}"

    def _create_source(self):
        self.spark.sql(f"CREATE TABLE {self.src} USING DELTA AS SELECT {self.row_select('id')} FROM range({N})")
        self.spark.sql(f"ALTER TABLE {self.src} SET TBLPROPERTIES ('delta.deletedFileRetentionDuration' = 'interval 7 days')")

    def sql(self, q):
        return self.spark.sql(q)

    def src_version(self) -> int:
        return self.sql(f"DESCRIBE HISTORY {self.src} LIMIT 1").collect()[0]["version"]

    def src_insert(self, lo, hi):
        self.sql(f"INSERT INTO {self.src} SELECT {self.row_select('id')} FROM range({lo}, {hi})")

    def src_delete(self, lo, hi):
        self.sql(f"DELETE FROM {self.src} WHERE PBKEY BETWEEN '{self.key(lo)}' AND '{self.key(hi - 1)}'")

    def src_update(self, lo, hi, assignment):
        self.sql(f"UPDATE {self.src} SET {assignment} WHERE PBKEY BETWEEN '{self.key(lo)}' AND '{self.key(hi - 1)}'")

    def mark_release(self, vintage: str) -> int:
        """What the vendor does 1-2 minutes after its MERGE: SET TBLPROPERTIES vintage/count. Returns the pin."""
        n = self.sql(f"SELECT count(*) FROM {self.src}").collect()[0][0]
        self.sql(f"ALTER TABLE {self.src} SET TBLPROPERTIES ('precisely.data.vintage' = '{vintage}', "
                 f"'precisely.data.count' = '{n}')")
        return self.src_version()

    # ---- pipeline helpers -----------------------------------------------------------------
    def load_baseline(self, vintage: str = BASE_VINTAGE):
        """Release seq 1: what bootstrap_load_current.py does for the real table."""
        pin = self.mark_release(vintage)
        src_h = release.hashed_source(release.read_source(self.spark, self.p, pin))
        src_h.createOrReplaceTempView("syn_baseline")
        self.sql(
            f"""INSERT INTO {self.p.current}
            SELECT {', '.join(SOURCE_COLUMNS)}, row_hash, {HASH_SPEC_VERSION}, 'BASELINE', '{vintage}', 1,
                   current_timestamp(), false FROM syn_baseline"""
        )
        v = self.sql(f"DESCRIBE HISTORY {self.p.current} LIMIT 1").collect()[0]["version"]
        self.sql(
            f"""INSERT INTO {self.p.release_label} (release_version, source_delta_version, status, notes)
            VALUES ('{vintage}', {pin}, 'CONFIRMED', 'synthetic baseline')"""
        )
        self.sql(
            f"""INSERT INTO {self.p.release_log} (release_version, release_seq, state, source_delta_version,
            source_row_count, current_table_version, hash_spec_version)
            VALUES ('{vintage}', 1, 'COMPLETE', {pin}, {N}, {v}, {HASH_SPEC_VERSION})"""
        )

    def label(self, vintage, pin, status="CONFIRMED", added=None, updated=None, deleted=None, rows=None):
        def lit(x):
            return "NULL" if x is None else x

        self.sql(
            f"""INSERT INTO {self.p.release_label} (release_version, source_delta_version, status, vendor_added,
            vendor_updated, vendor_deleted, vendor_row_count)
            VALUES ('{vintage}', {pin}, '{status}', {lit(added)}, {lit(updated)}, {lit(deleted)}, {lit(rows)})"""
        )

    def run(self, vintage, **kw):
        return release.run_release(self.spark, replace(self.p, release_version=vintage, **kw))

    def log_state(self, vintage):
        r = self.sql(f"SELECT state FROM {self.p.release_log} WHERE release_version = '{vintage}'").collect()
        return r[0][0] if r else None

    def current_counts(self):
        rows = self.sql(f"SELECT status, count(*) n FROM {self.p.current} GROUP BY 1").collect()
        return {r["status"]: r["n"] for r in rows}

    def current_version(self) -> int:
        return self.sql(f"DESCRIBE HISTORY {self.p.current} LIMIT 1").collect()[0]["version"]

    def change_log_counts(self, seq):
        rows = self.sql(f"SELECT change_type, count(*) n FROM {self.p.change_log} WHERE release_seq = {seq} GROUP BY 1").collect()
        return {r["change_type"]: r["n"] for r in rows}

    def row(self, i):
        r = self.sql(f"SELECT * FROM {self.p.current} WHERE PBKEY = '{self.key(i)}'").collect()
        return r[0].asDict() if r else None

    def drop(self):
        for s in (self.src_schema, self.bronze, self.ops):
            self.sql(f"DROP SCHEMA IF EXISTS {self.catalog}.{s} CASCADE")


# =============================================================================================
# Scenarios. Each takes a ready Env with the baseline (seq 1) loaded.
# =============================================================================================


def s01_normal_release(env: Env):
    env.src_insert(N, N + 20)
    env.src_update(0, 10, "CITY = 'NEWCITY'")
    env.src_delete(100, 105)
    pin = env.mark_release("2099.01.1")
    env.label("2099.01.1", pin, added=20, updated=10, deleted=5, rows=N + 15)
    r = env.run("2099.01.1")
    eq(r["counts"], {"added": 20, "updated": 10, "deleted": 5, "reappeared": 0}, "counts")
    eq(env.current_counts(), {"BASELINE": N - 15, "ADDED": 20, "UPDATED": 10, "DELETED": 5}, "status distribution")
    eq(env.change_log_counts(2), {"ADDED": 20, "UPDATED": 10, "DELETED": 5}, "change log")
    t = env.row(102)
    check(t["status"] == "DELETED" and t["row_hash"] is None and t["CITY"] is None and t["LATITUDE"] is None,
          "tombstone must null attributes and hash")
    untouched = env.row(500)
    eq((untouched["status"], untouched["release_version"], untouched["release_seq"]), ("BASELINE", BASE_VINTAGE, 1),
       "unchanged row keeps baseline stamp")
    eq(env.log_state("2099.01.1"), "COMPLETE", "log state")


def s02_resurrection(env: Env):
    env.src_delete(100, 105)
    pin = env.mark_release("2099.01.1")
    env.label("2099.01.1", pin)
    env.run("2099.01.1")
    check(env.row(100)["status"] == "DELETED", "deleted in release 2")
    env.sql(f"INSERT INTO {env.src} SELECT {env.row_select('id')} FROM range(100, 105)")
    pin = env.mark_release("2099.01.2")
    env.label("2099.01.2", pin, added=5, updated=0, deleted=0)
    r = env.run("2099.01.2")
    eq(r["counts"], {"added": 5, "updated": 0, "deleted": 0, "reappeared": 5}, "counts")
    back = env.row(100)
    eq((back["status"], back["reappeared"], back["release_version"], back["CITY"]), ("ADDED", True, "2099.01.2", "CITY_100"),
       "resurrected row")
    eq(env.sql(f"SELECT count(*) FROM {env.p.current} WHERE status <> 'DELETED'").collect()[0][0], N, "live rows")


def s03_hash_sensitivity(env: Env):
    env.src_update(0, 1, "UNIT = ''")                       # id 0: NULL -> '' is a change
    env.src_update(3, 4, "UNIT = ''")                       # id 3: NULL -> ''
    env.src_update(6, 7, "UNIT = ''")
    env.src_update(1, 3, "LATITUDE = LATITUDE + 0.000000000001")  # 1e-12 is a change, no rounding
    env.src_update(10, 21, "CITY = CITY")                   # rewritten with identical values: not a change
    pin = env.mark_release("2099.01.1")
    env.label("2099.01.1", pin, added=0, updated=5, deleted=0)
    r = env.run("2099.01.1")
    eq(r["counts"], {"added": 0, "updated": 5, "deleted": 0, "reappeared": 0}, "counts")
    eq(env.row(15)["status"], "BASELINE", "identical rewrite stays untouched")


def s04_rebuild_same_content(env: Env):
    env.sql(f"INSERT OVERWRITE {env.src} SELECT * FROM {env.src} ORDER BY PBKEY DESC")
    pin = env.mark_release("2099.01.1")
    env.label("2099.01.1", pin, added=0, updated=0, deleted=0, rows=N)
    r = env.run("2099.01.1")
    eq(r["counts"], {"added": 0, "updated": 0, "deleted": 0, "reappeared": 0}, "counts")
    eq(env.current_counts(), {"BASELINE": N}, "nothing rewritten")
    eq(env.log_state("2099.01.1"), "COMPLETE", "log state")


def s05_large_change_gate(env: Env):
    env.src_delete(0, 30)  # 1.5% of rows
    pin = env.mark_release("2099.01.1")
    env.label("2099.01.1", pin)
    before = env.current_version()
    raises(GateError, lambda: env.run("2099.01.1"), "row count moves")
    eq(env.current_version(), before, "current untouched by a failed gate")
    eq(env.log_state("2099.01.1"), None, "no log row for a gated release")
    r = env.run("2099.01.1", allow_large_change=True)
    eq(r["counts"]["deleted"], 30, "deleted with override")


def s06_schema_drift(env: Env):
    env.sql(f"ALTER TABLE {env.src} ADD COLUMN NEWCOL STRING")
    pin = env.mark_release("2099.01.1")
    env.label("2099.01.1", pin)
    before = env.current_version()
    raises(GateError, lambda: env.run("2099.01.1"), "schema drift")
    eq(env.current_version(), before, "current untouched")
    # type change / missing column, checked on the DataFrame the gate sees
    from pyspark.sql import functions as F

    base = env.spark.table(env.src).drop("NEWCOL")
    label = {"vendor_row_count": None}
    raises(GateError, lambda: release.gate_source(env.spark, env.p, base.withColumn("LATITUDE", F.col("LATITUDE").cast("string")), label), "type_changed")
    raises(GateError, lambda: release.gate_source(env.spark, env.p, base.drop("CITY"), label), "missing")


def s07_bad_source_data(env: Env):
    from pyspark.sql import functions as F

    base = env.spark.table(env.src)
    label = {"vendor_row_count": None}
    raises(GateError, lambda: release.gate_source(env.spark, env.p, base.unionByName(base.limit(1)), label), "unique")
    raises(GateError, lambda: release.gate_source(
        env.spark, env.p, base.withColumn("PBKEY", F.when(F.col("PBKEY") == env.key(5), None).otherwise(F.col("PBKEY"))), label), "unique")
    raises(GateError, lambda: release.gate_source(
        env.spark, env.p, base.withColumn("LATITUDE", F.when(F.col("PBKEY") == env.key(5), float("nan")).otherwise(F.col("LATITUDE"))), label), "non-finite")
    raises(GateError, lambda: release.gate_source(
        env.spark, env.p, base.withColumn("LONGITUDE", F.when(F.col("PBKEY") == env.key(5), float("inf")).otherwise(F.col("LONGITUDE"))), label), "non-finite")


def s08_vendor_count_mismatch(env: Env):
    env.src_insert(N, N + 5)
    pin = env.mark_release("2099.01.1")
    env.label("2099.01.1", pin, rows=N + 6)  # vendor says one more row than the source has
    before = env.current_version()
    raises(GateError, lambda: env.run("2099.01.1"), "vendor_row_count")
    eq(env.current_version(), before, "current untouched")


def s09_validation_failure_and_rollback(env: Env):
    env.src_insert(N, N + 7)
    env.src_update(0, 4, "CITY = 'X'")
    pin = env.mark_release("2099.01.1")
    env.label("2099.01.1", pin, added=7, updated=99, deleted=0)  # wrong expectation for updated
    raises(RuntimeError, lambda: env.run("2099.01.1"), "validation failed")
    eq(env.log_state("2099.01.1"), "FAILED", "state after failed validation")
    check(env.change_log_counts(2) != {}, "change log was published before validation")
    r = release.rollback_release(env.spark, replace(env.p, release_version="2099.01.1"))
    check(r["restored"], "rollback must restore")
    eq(env.current_counts(), {"BASELINE": N}, "current back to baseline")
    eq(env.change_log_counts(2), {}, "change log cleared")
    eq(env.log_state("2099.01.1"), "ROLLED_BACK", "state after rollback")
    env.sql(f"UPDATE {env.p.release_label} SET vendor_updated = 4 WHERE release_version = '2099.01.1'")
    r = env.run("2099.01.1")
    eq(r["counts"], {"added": 7, "updated": 4, "deleted": 0, "reappeared": 0}, "counts after re-run")
    eq(env.change_log_counts(2), {"ADDED": 7, "UPDATED": 4}, "change log has no stale rows")
    eq(env.log_state("2099.01.1"), "COMPLETE", "state after re-run")
    raises(GateError, lambda: release.rollback_release(env.spark, replace(env.p, release_version=BASE_VINTAGE)), "not the newest")


def s10_idempotent_rerun(env: Env):
    env.src_insert(N, N + 3)
    pin = env.mark_release("2099.01.1")
    env.label("2099.01.1", pin)
    env.run("2099.01.1")
    v = env.current_version()
    r = env.run("2099.01.1")
    eq(r["result"], "noop", "second run")
    eq(env.current_version(), v, "second run wrote nothing")
    eq(env.change_log_counts(2), {"ADDED": 3}, "change log not duplicated")


def s11_crash_between_merge_and_publish(env: Env):
    env.src_insert(N, N + 4)
    env.src_update(0, 2, "CITY = 'Y'")
    pin = env.mark_release("2099.01.1")
    env.label("2099.01.1", pin, added=4, updated=2, deleted=0)
    real = release.publish_change_log

    def boom(*a, **k):
        raise RuntimeError("simulated crash after MERGE")

    release.publish_change_log = boom
    try:
        raises(RuntimeError, lambda: env.run("2099.01.1"), "simulated crash")
    finally:
        release.publish_change_log = real
    eq(env.log_state("2099.01.1"), "FAILED", "state after crash")
    check(env.current_counts().get("ADDED") == 4, "MERGE was applied")
    v = env.current_version()
    r = env.run("2099.01.1")
    eq(r["counts"], {"added": 4, "updated": 2, "deleted": 0, "reappeared": 0}, "counts after recovery")
    eq(env.current_version(), v, "recovery did not re-apply the MERGE")
    eq(env.change_log_counts(2), {"ADDED": 4, "UPDATED": 2}, "change log written once")
    eq(env.log_state("2099.01.1"), "COMPLETE", "state")


def s12_detect_and_thursday_delete(env: Env):
    env.src_insert(N, N + 6)
    pin1 = env.mark_release("2099.01.1")
    env.src_delete(200, 203)                 # Thursday NJ delete AFTER the release marker: belongs to the NEXT release
    d = detect.run_detect(env.spark, env.p)
    eq(d, {"seeded": ["2099.01.1"], "pending": ["2099.01.1"]}, "first detect")
    lab = env.sql(f"SELECT * FROM {env.p.release_label} WHERE release_version = '2099.01.1'").collect()[0]
    eq((lab["status"], lab["source_delta_version"], lab["vendor_row_count"]), ("PROPOSED", pin1, N + 6), "seeded label")
    eq(detect.run_detect(env.spark, env.p), {"seeded": [], "pending": ["2099.01.1"]}, "detect is idempotent")
    out = detect.run_pending(env.spark, env.p)
    eq(out["applied"][0]["counts"], {"added": 6, "updated": 0, "deleted": 0, "reappeared": 0}, "release 1 excludes the later delete")
    check(env.row(200)["status"] == "BASELINE", "Thursday delete not in this release")
    pin2 = env.mark_release("2099.01.2")
    d = detect.run_detect(env.spark, env.p)
    eq(d, {"seeded": ["2099.01.2"], "pending": ["2099.01.2"]}, "second detect")
    out = detect.run_pending(env.spark, env.p)
    eq(out["applied"][0]["counts"]["deleted"], 3, "Thursday delete lands in the next release")
    eq(detect.run_pending(env.spark, env.p), {"applied": []}, "nothing pending afterwards")
    env.src_insert(N + 10, N + 12)
    env.mark_release("2099.01.3")
    detect.run_detect(env.spark, env.p)
    raises(GateError, lambda: detect.run_pending(env.spark, env.p, require_confirmed=True), "not CONFIRMED")
    eq(env.log_state("2099.01.3"), None, "unconfirmed release not applied")
    check(pin2 > pin1, "pins increase")


def s13_missed_releases_in_order(env: Env):
    env.src_insert(N, N + 5)
    env.mark_release("2099.01.1")
    env.src_update(0, 3, "CITY = 'Z'")
    env.mark_release("2099.01.2")
    d = detect.run_detect(env.spark, env.p)
    eq(d["pending"], ["2099.01.1", "2099.01.2"], "both pending")
    out = detect.run_pending(env.spark, env.p)
    eq([r["release"] for r in out["applied"]], ["2099.01.1", "2099.01.2"], "applied in order")
    eq([r["release_seq"] for r in out["applied"]], [2, 3], "sequential release_seq")
    eq(out["applied"][1]["counts"]["updated"], 3, "second release carries its own changes")


def s14_expired_pin_is_superseded(env: Env):
    env.src_insert(N, N + 5)
    env.mark_release("2099.01.1")
    env.src_update(0, 3, "CITY = 'Z'")
    env.mark_release("2099.01.2")
    detect.run_detect(env.spark, env.p)
    real = detect.pin_readable
    first_pin = env.sql(f"SELECT source_delta_version FROM {env.p.release_label} WHERE release_version = '2099.01.1'").collect()[0][0]
    detect.pin_readable = lambda spark, p, pin: pin != first_pin
    try:
        out = detect.run_pending(env.spark, env.p)
    finally:
        detect.pin_readable = real
    eq([r["result"] for r in out["applied"]], ["superseded", "complete"], "first superseded, second applied")
    eq(out["applied"][1]["counts"]["added"], 5, "second release absorbs the superseded release's changes")
    eq(out["applied"][1]["release_seq"], 2, "release_seq stays gapless")
    st = env.sql(f"SELECT status FROM {env.p.release_label} WHERE release_version = '2099.01.1'").collect()[0][0]
    eq(st, "SUPERSEDED", "label status")


def s15_detect_anomalies(env: Env):
    env.src_insert(N, N + 2)
    env.mark_release("2099.01.1")
    detect.run_detect(env.spark, env.p)
    # vendor sets an older vintage after a newer one: refuse
    env.sql(f"ALTER TABLE {env.src} SET TBLPROPERTIES ('precisely.data.vintage' = '2098.12.0', 'precisely.data.count' = '1')")
    raises(detect.DetectError, lambda: detect.run_detect(env.spark, env.p), "not increasing")
    # garbage vintage
    env.sql(f"ALTER TABLE {env.src} SET TBLPROPERTIES ('precisely.data.vintage' = 'oct-2026')")
    raises(detect.DetectError, lambda: detect.run_detect(env.spark, env.p), "YYYY.MM.N")


SCENARIOS = [v for k, v in sorted(globals().items()) if re.match(r"s\d\d_", k)]
