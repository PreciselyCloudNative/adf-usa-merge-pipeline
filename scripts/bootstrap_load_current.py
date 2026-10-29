# Databricks notebook source
# One-off bootstrap: load adf_usa_current from the frozen 2026.10.1 baseline and seed the control tables.
# Run on serverless via `databricks jobs submit`. Refuses to run if adf_usa_current already has rows.
# Parameters (notebook base_parameters): catalog, src_path (workspace dir that contains adf_merge/)

# COMMAND ----------

import json
import sys

dbutils.widgets.text("catalog", "address_book")
dbutils.widgets.text("src_path", "")
dbutils.widgets.text("expected_rows", "194466619")

catalog = dbutils.widgets.get("catalog")
src_path = dbutils.widgets.get("src_path")
expected_rows = int(dbutils.widgets.get("expected_rows"))
if src_path:
    sys.path.insert(0, src_path)

from pyspark.sql import functions as F

from adf_merge.hashing import HASH_SPEC_VERSION, row_hash_expr
from adf_merge.schema import HASH_COLUMNS, SOURCE_COLUMNS

BASELINE = f"{catalog}.bronze.adf_usa_baseline_2026_10_1"
CURRENT = f"{catalog}.bronze.adf_usa_current"
RELEASE = "2026.10.1"
RELEASE_SEQ = 1

# COMMAND ----------
# Guards

if spark.table(CURRENT).limit(1).count() != 0:
    raise RuntimeError(f"{CURRENT} is not empty; bootstrap refuses to run twice")

base = spark.table(BASELINE)
n = base.count()
if n != expected_rows:
    raise RuntimeError(f"baseline has {n} rows, expected {expected_rows}")

nonfinite = base.filter(
    F.isnan("LATITUDE")
    | F.isnan("LONGITUDE")
    | F.col("LATITUDE").isin(float("inf"), float("-inf"))
    | F.col("LONGITUDE").isin(float("inf"), float("-inf"))
).count()
if nonfinite:
    raise RuntimeError(f"{nonfinite} rows have non-finite LATITUDE/LONGITUDE")

# COMMAND ----------
# Load

loaded = (
    base.select(
        *SOURCE_COLUMNS,
        row_hash_expr().alias("row_hash"),
        F.lit(HASH_SPEC_VERSION).alias("hash_spec_version"),
        F.lit("BASELINE").alias("status"),
        F.lit(RELEASE).alias("release_version"),
        F.lit(RELEASE_SEQ).cast("bigint").alias("release_seq"),
        F.current_timestamp().alias("changed_at"),
        F.lit(False).alias("reappeared"),
    )
)
loaded.writeTo(CURRENT).append()

# COMMAND ----------
# Verify

cur = spark.table(CURRENT)
stats = cur.agg(
    F.count("*").alias("n"),
    F.countDistinct("PBKEY").alias("distinct_pbkey"),
    F.sum(F.col("row_hash").isNull().cast("int")).alias("null_hash"),
    F.countDistinct("row_hash").alias("distinct_hash"),
).collect()[0].asDict()
if stats["n"] != expected_rows or stats["distinct_pbkey"] != expected_rows or stats["null_hash"] != 0:
    raise RuntimeError(f"verification failed: {stats}")

# COMMAND ----------
# Seed control tables (idempotent via MERGE on release_version)

spark.sql(
    f"""
MERGE INTO {catalog}.ops.release_label t
USING (
  SELECT * FROM VALUES
    ('2026.10.1', DATE'2026-10-23', 219L, 'CONFIRMED', 11738L, 5200L, 23L, 194466619L,
     'bootstrap: counts from the Precisely release document supplied by the user', current_timestamp(),
     'Baseline. Source pin v219 = SET TBLPROPERTIES after the v218 MERGE.'),
    ('2026.10.2', DATE'2026-10-29', 227L, 'CONFIRMED', 9467L, 3389L, 229L, 194475857L,
     'bootstrap: counts from the Precisely release document supplied by the user', current_timestamp(),
     'Source pin v227 = SET TBLPROPERTIES after the v226 MERGE. Row count = 194466619+9467-229.')
  AS s(release_version, release_date, source_delta_version, status, vendor_added, vendor_updated, vendor_deleted,
       vendor_row_count, confirmed_by, confirmed_at, notes)
) s
ON t.release_version = s.release_version
WHEN NOT MATCHED THEN INSERT *
"""
)

spark.sql(
    f"""
MERGE INTO {catalog}.ops.release_log t
USING (SELECT '{RELEASE}' AS release_version) s
ON t.release_version = s.release_version
WHEN NOT MATCHED THEN INSERT
  (release_version, release_seq, run_id, state, source_delta_version, source_row_count, added, updated, deleted,
   current_table_version, hash_spec_version, started_at, finished_at, error)
VALUES ('{RELEASE}', {RELEASE_SEQ}, 'bootstrap', 'COMPLETE', 219, {n}, NULL, NULL, NULL,
        NULL, {HASH_SPEC_VERSION}, current_timestamp(), current_timestamp(), NULL)
"""
)

dbutils.notebook.exit(json.dumps({"loaded": stats, "baseline_rows": n}))
