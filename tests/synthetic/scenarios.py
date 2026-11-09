"""Synthetic end-to-end scenarios, run on Databricks against a throwaway source table (never the real share).

Each scenario gets its own schemas (src / bronze / ops / silver / gold) in the target catalog, built from the repo DDL, a 2,000-row
synthetic adf_usa source, and a baseline load, then exercises the real release / detect code.
"""

import re
import uuid
from dataclasses import replace
from pathlib import Path

from adf_merge import changefile, detect, gold, maintenance, release
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
        self.silver, self.gold = f"zz_syn_silver_{tag}", f"zz_syn_gold_{tag}"
        self.src = f"{catalog}.{self.src_schema}.adf_usa"
        for s in (self.src_schema, self.bronze, self.ops):
            spark.sql(f"CREATE SCHEMA {catalog}.{s}")
        for f in sorted(Path(ddl_dir).glob("0*.sql")):
            text = f.read_text().replace("${catalog}.bronze.", f"{catalog}.{self.bronze}.")
            text = text.replace("${catalog}.ops.", f"{catalog}.{self.ops}.")
            text = text.replace("${catalog}.silver", f"{catalog}.{self.silver}")
            text = text.replace("${catalog}.gold", f"{catalog}.{self.gold}")
            for stmt in (s.strip() for s in text.split(";")):
                if stmt:
                    spark.sql(stmt)
        self._create_source()
        self.p = Params(catalog=catalog, bronze_schema=self.bronze, ops_schema=self.ops, silver_schema=self.silver,
                        gold_schema=self.gold, source_table=self.src,
                        deleted_tolerance=0, changefile_table=f"{catalog}.{self.src_schema}.adf_usa_changefile",
                        use_changefile=False)

    # ---- source helpers -------------------------------------------------------------------
    @staticmethod
    def row_select(id_expr: str) -> str:
        cols = [f"concat('PB', lpad(cast({id_expr} as string), 7, '0')) AS PBKEY"]
        for name, typ in HASH_COLUMNS:
            if name == "LATITUDE":
                cols.append(f"cast(cast(30.0 + {id_expr} * 0.001 as decimal(11,6)) as double) AS LATITUDE")
            elif name == "LONGITUDE":
                cols.append(f"cast(cast(-90.0 - {id_expr} * 0.001 as decimal(11,6)) as double) AS LONGITUDE")
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

    def label(self, vintage, pin, status="CONFIRMED", added=None, updated=None, deleted=None, rows=None, date=True):
        def lit(x):
            return "NULL" if x is None else x

        self.sql(
            f"""INSERT INTO {self.p.release_label} (release_version, release_date, source_delta_version, status,
            vendor_added, vendor_updated, vendor_deleted, vendor_row_count)
            VALUES ('{vintage}', {"current_date()" if date else "NULL"}, {pin}, '{status}', {lit(added)}, {lit(updated)},
            {lit(deleted)}, {lit(rows)})"""
        )

    def write_changefile(self, vintage, added=None, updated=None, deleted=None, wrong_city=False, count_delta=0):
        """Vendor changefile: full replace, decimal(11,6) coordinates. A/U rows carry the new values (read from the
        main source), D rows the old generated values. Ranges are (lo, hi) id ranges."""
        def sel(frm, change, code, city=None):
            cols = []
            for c in SOURCE_COLUMNS:
                if c in ("LATITUDE", "LONGITUDE"):
                    cols.append(f"cast({c} as decimal(11,6)) AS {c}")
                elif c == "CITY" and city:
                    cols.append(f"'{city}' AS CITY")
                else:
                    cols.append(c)
            return f"SELECT {', '.join(cols)}, '{change}' AS CHANGE, '{code}' AS CHG_CODE, '' AS PBKEY_ALT FROM {frm}"

        def between(rng):
            return f"PBKEY BETWEEN '{self.key(rng[0])}' AND '{self.key(rng[1] - 1)}'"

        parts = []
        if added:
            parts.append(sel(f"(SELECT * FROM {self.src} WHERE {between(added)})", "A", "NEW"))
        if updated:
            parts.append(sel(f"(SELECT * FROM {self.src} WHERE {between(updated)})", "U", "", "WRONG" if wrong_city else None))
        if deleted:
            old = f"(SELECT {self.row_select('id')} FROM range({deleted[0]}, {deleted[1]}))"
            parts.append(sel(old, "D", "BAD"))
        cf = self.p.changefile_table
        self.sql(f"CREATE OR REPLACE TABLE {cf} AS " + " UNION ALL ".join(parts))
        n = self.sql(f"SELECT count(*) FROM {cf}").collect()[0][0]
        self.sql(f"ALTER TABLE {cf} SET TBLPROPERTIES ('precisely.data.vintage' = '{vintage}', "
                 f"'precisely.data.count' = '{n + count_delta}')")

    def auto(self, vintage, **kw):
        return changefile.run_release_auto(self.spark, replace(self.p, release_version=vintage, use_changefile=True, **kw))

    def log_row(self, vintage):
        return self.sql(f"SELECT method, fallback_reason, audit, state FROM {self.p.release_log} "
                        f"WHERE release_version = '{vintage}'").collect()[0].asDict()

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

    def history_count(self, seq):
        return self.sql(f"SELECT count(*) FROM {self.p.history} WHERE valid_to_seq = {seq}").collect()[0][0]

    def gold_view(self, vintage):
        return f"{self.catalog}.{self.gold}.{gold.view_name(vintage)}"

    def pinned(self, vintage):
        """Fingerprint (rows, content) of the source exactly as the vendor published `vintage`."""
        pin = self.sql(f"SELECT source_delta_version FROM {self.p.release_label} WHERE release_version = '{vintage}'").collect()[0][0]
        return release.fingerprint(release.hashed_source(release.read_source(self.spark, self.p, pin)))

    def gold_fingerprint(self, vintage):
        return release.fingerprint(release.hashed_source(self.spark.table(self.gold_view(vintage))))

    def row(self, i):
        r = self.sql(f"SELECT * FROM {self.p.current} WHERE PBKEY = '{self.key(i)}'").collect()
        return r[0].asDict() if r else None

    def drop(self):
        for s in (self.src_schema, self.bronze, self.ops, self.silver, self.gold):
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
    eq(env.history_count(2), 0, "superseded versions cleared with the rollback")
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
    eq(d, {"seeded": ["2099.01.1"], "remarked": [], "pending": ["2099.01.1"]}, "first detect")
    lab = env.sql(f"SELECT * FROM {env.p.release_label} WHERE release_version = '2099.01.1'").collect()[0]
    eq((lab["status"], lab["source_delta_version"], lab["vendor_row_count"]), ("PROPOSED", pin1, N + 6), "seeded label")
    eq(detect.run_detect(env.spark, env.p), {"seeded": [], "remarked": [], "pending": ["2099.01.1"]}, "detect is idempotent")
    out = detect.run_pending(env.spark, env.p)
    eq(out["applied"][0]["counts"], {"added": 6, "updated": 0, "deleted": 0, "reappeared": 0}, "release 1 excludes the later delete")
    check(env.row(200)["status"] == "BASELINE", "Thursday delete not in this release")
    pin2 = env.mark_release("2099.01.2")
    d = detect.run_detect(env.spark, env.p)
    eq(d, {"seeded": ["2099.01.2"], "remarked": [], "pending": ["2099.01.2"]}, "second detect")
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


def s16_maintenance_after_deletes(env: Env):
    env.src_delete(300, 310)
    env.src_update(0, 3, "CITY = 'M'")
    pin = env.mark_release("2099.01.1")
    env.label("2099.01.1", pin, added=0, updated=3, deleted=10)
    r = env.run("2099.01.1")
    before = env.sql(f"SELECT count(*), sum(cast(xxhash64(PBKEY, row_hash) as decimal(38,0))) FROM {env.p.current}").collect()[0]
    did = maintenance.after_releases(env.spark, env.p, [r])
    eq(did, {"purged": True, "vacuumed": True}, "maintenance ran")
    after = env.sql(f"SELECT count(*), sum(cast(xxhash64(PBKEY, row_hash) as decimal(38,0))) FROM {env.p.current}").collect()[0]
    eq(tuple(after), tuple(before), "maintenance does not change table content")
    t = env.row(305)
    check(t["status"] == "DELETED" and t["CITY"] is None, "tombstone still nulled")
    eq(maintenance.after_releases(env.spark, env.p, [{"result": "noop"}]), {"purged": False, "vacuumed": False}, "noop does nothing")


def s17_silver_views(env: Env):
    env.src_insert(N, N + 8)
    env.src_update(0, 4, "CITY = 'V'")
    env.src_delete(400, 406)
    pin = env.mark_release("2099.01.1")
    env.label("2099.01.1", pin, added=8, updated=4, deleted=6)
    env.run("2099.01.1")
    v = f"{env.catalog}.{env.silver}"
    eq(env.sql(f"SELECT count(*) FROM {v}.adf_usa_current").collect()[0][0], N + 2, "current view = live rows")
    cols = set(env.sql(f"SELECT * FROM {v}.adf_usa_current LIMIT 1").columns)
    check("row_hash" not in cols and "hash_spec_version" not in cols and "CITY" in cols, "current view hides hash internals")
    feed = {r["status"]: r["n"] for r in env.sql(f"SELECT status, count(*) n FROM {v}.adf_usa_change_feed GROUP BY 1").collect()}
    eq(feed, {"ADDED": 8, "UPDATED": 4, "DELETED": 6}, "feed excludes baseline, includes tombstones")
    gone = env.sql(f"SELECT count(*) FROM {v}.adf_usa_change_feed WHERE status='DELETED' AND (CITY IS NOT NULL OR LATITUDE IS NOT NULL)").collect()[0][0]
    eq(gone, 0, "tombstones expose no attributes")
    eq(env.sql(f"SELECT count(*) FROM {v}.adf_usa_change_log WHERE release_seq = 2").collect()[0][0], 18, "change log view")
    rel = env.sql(f"SELECT release_version, added, updated, deleted FROM {v}.adf_usa_releases WHERE release_seq = 2").collect()[0]
    eq(tuple(rel), ("2099.01.1", 8, 4, 6), "releases view")


def _release_via_changefile(env: Env, vintage, *, ins=None, upd=None, dele=None, rows=None, **cf_kw):
    if ins:
        env.src_insert(*ins)
    if upd:
        env.src_update(upd[0], upd[1], "CITY = 'NEWCITY'")
    if dele:
        env.src_delete(*dele)
    pin = env.mark_release(vintage)
    n = lambda r: (r[1] - r[0]) if r else 0  # noqa: E731
    env.label(vintage, pin, added=n(ins), updated=n(upd), deleted=n(dele), rows=rows)
    env.write_changefile(vintage, added=ins, updated=upd, deleted=dele, **cf_kw)
    return pin


def s18_remarked_release_is_reapplied(env: Env):
    """The vendor deletes rows and sets the SAME vintage again after we applied it (NJ_DELETION): roll back, re-apply."""
    env.src_insert(N, N + 6)
    pin1 = env.mark_release("2099.01.1")
    detect.run_detect(env.spark, env.p)
    out = detect.run_pending(env.spark, env.p)
    eq(out["applied"][0]["counts"], {"added": 6, "updated": 0, "deleted": 0, "reappeared": 0}, "first apply")
    env.src_delete(200, 203)
    pin2 = env.mark_release("2099.01.1")
    d = detect.run_detect(env.spark, env.p)
    eq([(r["action"], r["old_pin"], r["new_pin"], r["new_count"]) for r in d["remarked"]],
       [("amend", pin1, pin2, N + 3)], "re-mark detected")
    r = detect.run_pending(env.spark, env.p)["applied"][0]
    eq((r["result"], r["release_seq"]), ("complete", 2), "re-applied under the same release_seq")
    eq(r["counts"], {"added": 6, "updated": 0, "deleted": 3, "reappeared": 0}, "release now includes the delete")
    eq(env.sql(f"SELECT source_delta_version FROM {env.p.release_log} WHERE release_version = '2099.01.1'").collect()[0][0],
       pin2, "log carries the new pin")
    eq(env.change_log_counts(2), {"ADDED": 6, "DELETED": 3}, "change log rebuilt for the release")
    check(env.row(200)["status"] == "DELETED", "deleted row applied")
    eq(detect.run_detect(env.spark, env.p)["remarked"], [], "re-mark not reported again")
    eq(detect.run_pending(env.spark, env.p), {"applied": []}, "nothing left to do")


def c01_changefile_primary(env: Env):
    _release_via_changefile(env, "2099.01.1", ins=(N, N + 20), upd=(0, 10), dele=(100, 105), rows=N + 15)
    r = env.auto("2099.01.1")
    eq((r["method"], r["audit"]), ("CHANGEFILE", "PASSED"), "method / audit")
    eq(r["counts"], {"added": 20, "updated": 10, "deleted": 5, "reappeared": 0}, "counts")
    eq(env.log_row("2099.01.1"), {"method": "CHANGEFILE", "fallback_reason": None, "audit": "PASSED", "state": "COMPLETE"}, "log")
    t = env.row(102)
    check(t["status"] == "DELETED" and t["row_hash"] is None and t["CITY"] is None, "tombstone nulled")
    eq(env.row(5)["CITY"], "NEWCITY", "update applied")
    eq(env.current_counts(), {"BASELINE": N - 15, "ADDED": 20, "UPDATED": 10, "DELETED": 5}, "status distribution")


def c02_changefile_resurrection(env: Env):
    _release_via_changefile(env, "2099.01.1", dele=(100, 105))
    env.auto("2099.01.1")
    env.sql(f"INSERT INTO {env.src} SELECT {env.row_select('id')} FROM range(100, 105)")
    pin = env.mark_release("2099.01.2")
    env.label("2099.01.2", pin, added=5, updated=0, deleted=0)
    env.write_changefile("2099.01.2", added=(100, 105))
    r = env.auto("2099.01.2")
    eq(r["method"], "CHANGEFILE", "method")
    eq(r["counts"], {"added": 5, "updated": 0, "deleted": 0, "reappeared": 5}, "counts")
    back = env.row(100)
    eq((back["status"], back["reappeared"], back["CITY"]), ("ADDED", True, "CITY_100"), "resurrected row")


def c03_not_there_yet_defers_then_falls_back(env: Env):
    env.src_insert(N, N + 5)
    env.mark_release("2099.01.1")
    detect.run_detect(env.spark, env.p)
    env.sql(f"UPDATE {env.p.release_label} SET release_date = current_date() WHERE release_version = '2099.01.1'")
    before = env.current_version()
    out = detect.run_pending(env.spark, replace(env.p, use_changefile=True, changefile_wait_days=3))
    eq([r["result"] for r in out["applied"]], ["deferred"], "deferred while the changefile is missing")
    eq(env.current_version(), before, "nothing applied")
    eq(env.log_state("2099.01.1"), None, "no log row")
    out = detect.run_pending(env.spark, replace(env.p, use_changefile=True, changefile_wait_days=0))
    r = out["applied"][0]
    eq(r["result"], "complete", "falls back once the wait is over")
    row = env.log_row("2099.01.1")
    eq(row["method"], "MERGE", "method")
    check("not ready" in row["fallback_reason"], f"reason: {row['fallback_reason']}")
    eq(r["counts"]["added"], 5, "merge counts")


def c04_inconsistent_changefile_falls_back(env: Env):
    # changefile says "added" for keys that are already live
    env.src_insert(N, N + 5)
    pin = env.mark_release("2099.01.1")
    env.label("2099.01.1", pin, added=5, updated=0, deleted=0, rows=N + 5)
    env.write_changefile("2099.01.1", added=(N - 3, N + 5))
    r = env.auto("2099.01.1")
    row = env.log_row("2099.01.1")
    eq((row["method"], row["state"]), ("MERGE", "COMPLETE"), "fell back to MERGE")
    check("precondition" in row["fallback_reason"], f"reason: {row['fallback_reason']}")
    eq(r["counts"]["added"], 5, "merge result is correct")


def c05_audit_catches_wrong_values(env: Env):
    # changefile U rows carry a value that differs from the main table
    _release_via_changefile(env, "2099.01.1", upd=(0, 4), wrong_city=True, rows=N)
    r = env.auto("2099.01.1")
    row = env.log_row("2099.01.1")
    eq((row["method"], row["state"]), ("MERGE", "COMPLETE"), "fell back to MERGE")
    check("failed" in row["fallback_reason"] and "fingerprint" in row["fallback_reason"], f"reason: {row['fallback_reason']}")
    eq(env.row(1)["CITY"], "NEWCITY", "current matches the main table, not the changefile")
    eq(env.change_log_counts(2), {"UPDATED": 4}, "change log from the MERGE only")
    eq(r["counts"]["updated"], 4, "counts")


def c06_changefile_count_property_mismatch(env: Env):
    _release_via_changefile(env, "2099.01.1", ins=(N, N + 3), count_delta=1)
    env.auto("2099.01.1")
    row = env.log_row("2099.01.1")
    eq(row["method"], "MERGE", "fell back")
    check("precisely.data.count" in row["fallback_reason"], f"reason: {row['fallback_reason']}")


def c07_audit_skipped_when_pin_expired(env: Env):
    _release_via_changefile(env, "2099.01.1", ins=(N, N + 3), upd=(0, 2))
    real = detect.pin_readable
    detect.pin_readable = lambda spark, p, pin: False
    try:
        r = env.auto("2099.01.1")
    finally:
        detect.pin_readable = real
    eq((r["method"], r["audit"]), ("CHANGEFILE", "SKIPPED"), "applied without audit")
    eq(env.log_row("2099.01.1")["audit"], "SKIPPED", "log")


def c08_changefile_schema_drift(env: Env):
    _release_via_changefile(env, "2099.01.1", ins=(N, N + 3))
    env.sql(f"ALTER TABLE {env.p.changefile_table} ADD COLUMN SURPRISE STRING")
    # the changefile is read at the vendor's release-marker commit, so re-mark the release after the schema change
    env.sql(f"ALTER TABLE {env.p.changefile_table} SET TBLPROPERTIES ('precisely.data.vintage' = '2099.01.1', 'precisely.data.count' = '3')")
    env.auto("2099.01.1")
    row = env.log_row("2099.01.1")
    eq(row["method"], "MERGE", "fell back")
    check("schema drift" in row["fallback_reason"], f"reason: {row['fallback_reason']}")


def c09_changefile_matches_merge(env: Env):
    """Same release applied both ways gives the same table."""
    _release_via_changefile(env, "2099.01.1", ins=(N, N + 12), upd=(20, 30), dele=(500, 507), rows=N + 5)
    env.auto("2099.01.1")
    r = env.auto("2099.01.1")
    eq(r["result"], "noop", "re-run is a no-op")
    h = release.hashed_source(release.read_source(env.spark, env.p, env.src_version()))
    # audit already compared the live rows with the main table; here we also confirm the MERGE would change nothing
    diff = release.diff_preview(env.spark, env.p, h)
    eq((diff["added"], diff["updated"], diff["deleted"], diff["reappeared"]), (0, 0, 0, 0), "MERGE sees no difference")


# ---- history and gold release views --------------------------------------------------------------


def _three_more_releases(env: Env):
    """2099.01.1 by MERGE, .2 by changefile, .3 by MERGE: updates twice, update then delete, delete then resurrect."""
    env.src_insert(N, N + 10)
    env.src_update(0, 10, "CITY = 'R1'")
    env.src_delete(100, 105)
    pin = env.mark_release("2099.01.1")
    env.label("2099.01.1", pin)
    env.run("2099.01.1")
    gold.refresh(env.spark, env.p)

    env.src_update(5, 15, "CITY = 'R2'")      # 5-9 changed again, 10-14 for the first time
    env.src_delete(0, 3)                      # updated in release 2, now deleted
    pin = env.mark_release("2099.01.2")
    env.label("2099.01.2", pin)
    env.write_changefile("2099.01.2", updated=(5, 15), deleted=(0, 3))
    r = env.auto("2099.01.2")
    eq(r["method"], "CHANGEFILE", "release 3 by changefile")
    gold.refresh(env.spark, env.p)

    env.sql(f"INSERT INTO {env.src} SELECT {env.row_select('id')} FROM range(100, 103)")   # resurrect 100-102
    env.src_update(200, 210, "CITY = 'R3'")
    pin = env.mark_release("2099.01.3")
    env.label("2099.01.3", pin)
    env.run("2099.01.3")
    return gold.refresh(env.spark, env.p)


def g01_every_release_is_reproducible(env: Env):
    out = _three_more_releases(env)
    eq(out["views"], 4, "one view per release")
    for v in (BASE_VINTAGE, "2099.01.1", "2099.01.2", "2099.01.3"):
        eq(env.gold_fingerprint(v), env.pinned(v), f"gold view as of {v} equals the vendor table at that release")
    eq(env.history_count(2), 10 + 5, "release 2 superseded 10 updates + 5 deletes")
    eq(env.history_count(3), 10 + 3, "release 3 superseded 10 updates + 3 deletes")
    eq(env.history_count(4), 10, "release 4 superseded 10 updates; resurrected keys had only tombstones")
    old = env.sql(f"SELECT CITY, release_version FROM {env.gold_view(BASE_VINTAGE)} WHERE PBKEY = '{env.key(7)}'").collect()[0]
    eq(tuple(old), ("CITY_7", BASE_VINTAGE), "a record updated twice shows its original value at the baseline")
    mid = env.sql(f"SELECT CITY, release_version FROM {env.gold_view('2099.01.1')} WHERE PBKEY = '{env.key(7)}'").collect()[0]
    eq(tuple(mid), ("R1", "2099.01.1"), "...its first update in the next release")
    gone = env.sql(f"SELECT count(*) FROM {env.gold_view('2099.01.2')} WHERE PBKEY = '{env.key(101)}'").collect()[0][0]
    eq(gone, 0, "a record deleted in release 2 is absent from release 3 until it is resurrected")
    back = env.sql(f"SELECT release_version, reappeared FROM {env.gold_view('2099.01.3')} WHERE PBKEY = '{env.key(101)}'").collect()[0]
    eq(tuple(back), ("2099.01.3", True), "...and present again in release 4")


def g02_latest_alias_tags_and_versions(env: Env):
    out = _three_more_releases(env)
    check("tag_errors" not in out, f"tagging failed: {out}")
    g = f"{env.catalog}.{env.gold}"

    def tags():
        """None when the caller cannot read information_schema.table_tags (it sits on the system catalog)."""
        try:
            rows = env.sql(f"SELECT table_name, tag_value FROM {env.catalog}.information_schema.table_tags "
                           f"WHERE schema_name = '{env.gold}' AND tag_name = '{gold.TAG}'").collect()
        except Exception as e:  # noqa: BLE001
            check("INSUFFICIENT_PERMISSIONS" in str(e), f"unexpected error reading tags: {e}")
            return None
        return {r["table_name"]: r["tag_value"] for r in rows}

    t = tags()
    if t is not None:
        eq(t["adf_usa_latest"], "latest", "alias tagged latest")
        eq(t[gold.view_name("2099.01.3")], "latest", "newest release view tagged latest")
        others = {v for k, v in t.items() if k not in ("adf_usa_latest", gold.view_name("2099.01.3"))}
        eq(sorted(others), ["archived"], "older tagged archived")
    eq(env.sql(f"SELECT count(*) FROM {g}.adf_usa_latest").collect()[0][0],
       env.sql(f"SELECT count(*) FROM {env.gold_view('2099.01.3')}").collect()[0][0], "alias = newest release view")
    eq(env.sql(f"SELECT view_name FROM {g}.adf_usa_versions WHERE is_latest").collect()[0][0], gold.view_name("2099.01.3"), "versions index")
    eq(env.sql(f"SELECT count(*) FROM {g}.adf_usa_versions").collect()[0][0], 4, "versions index lists every release")
    env.src_update(300, 305, "CITY = 'R4'")
    pin = env.mark_release("2099.01.4")
    env.label("2099.01.4", pin)
    env.run("2099.01.4")
    out = gold.refresh(env.spark, env.p)
    check("tag_errors" not in out, f"tagging failed: {out}")
    eq(env.sql(f"SELECT view_name FROM {g}.adf_usa_versions WHERE is_latest").collect()[0][0], gold.view_name("2099.01.4"), "index moves")
    t = tags()
    if t is not None:
        eq((t[gold.view_name("2099.01.3")], t[gold.view_name("2099.01.4")]), ("archived", "latest"), "the tag moves with the release")


def g03_rollback_drops_view_and_history(env: Env):
    env.src_update(0, 6, "CITY = 'RB'")
    pin = env.mark_release("2099.01.1")
    env.label("2099.01.1", pin)
    env.run("2099.01.1")
    gold.refresh(env.spark, env.p)
    eq(env.history_count(2), 6, "archived before the rollback")
    release.rollback_release(env.spark, replace(env.p, release_version="2099.01.1"))
    out = gold.refresh(env.spark, env.p)
    eq((env.history_count(2), out["dropped"]), (0, [gold.view_name("2099.01.1")]), "history and release view removed")
    eq(env.sql(f"SELECT release_version FROM {env.catalog}.{env.gold}.adf_usa_versions").collect()[0][0], BASE_VINTAGE, "index back to baseline")
    eq(env.gold_fingerprint(BASE_VINTAGE), env.pinned(BASE_VINTAGE), "baseline view still exact")


def g04_crash_before_archive_recovers(env: Env):
    env.src_update(0, 4, "CITY = 'CR'")
    env.src_delete(50, 52)
    pin = env.mark_release("2099.01.1")
    env.label("2099.01.1", pin)
    real = release.publish_history

    def boom(*a, **k):
        raise RuntimeError("simulated crash after MERGE")

    release.publish_history = boom
    try:
        raises(RuntimeError, lambda: env.run("2099.01.1"), "simulated crash")
    finally:
        release.publish_history = real
    eq(env.history_count(2), 0, "nothing archived yet")
    r = env.run("2099.01.1")
    eq(r["counts"]["updated"] + r["counts"]["deleted"], 6, "counts after recovery")
    eq(env.history_count(2), 6, "recovery archived the superseded versions")
    gold.refresh(env.spark, env.p)
    eq(env.gold_fingerprint(BASE_VINTAGE), env.pinned(BASE_VINTAGE), "baseline reproducible after recovery")
    eq(env.gold_fingerprint("2099.01.1"), env.pinned("2099.01.1"), "release reproducible after recovery")


SCENARIOS = [v for k, v in sorted(globals().items()) if re.match(r"[scg]\d\d_", k)]
