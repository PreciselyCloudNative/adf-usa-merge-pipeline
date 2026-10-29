# Databricks notebook source
# One-off bootstrap: load adf_usa_current from the frozen baseline table and seed the control tables.
# Run on serverless via `databricks jobs submit`. Refuses to run if adf_usa_current already has rows.
# Parameters (notebook base_parameters), the last four come from scripts/latest_release.py and the baseline CTAS:
#   catalog, src_path (workspace dir that contains adf_merge/), baseline_table, release_version, source_version, expected_rows

# COMMAND ----------

import json
import sys

dbutils.widgets.text("catalog", "address_book")
dbutils.widgets.text("src_path", "")
dbutils.widgets.text("baseline_table", "adf_usa_baseline")
dbutils.widgets.text("release_version", "")
dbutils.widgets.text("source_version", "")
dbutils.widgets.text("expected_rows", "")

catalog = dbutils.widgets.get("catalog")
src_path = dbutils.widgets.get("src_path")
RELEASE = dbutils.widgets.get("release_version")
SOURCE_VERSION = int(dbutils.widgets.get("source_version"))
expected_rows = int(dbutils.widgets.get("expected_rows"))
if src_path:
    sys.path.insert(0, src_path)

from pyspark.sql import functions as F

from adf_merge.hashing import HASH_SPEC_VERSION, row_hash_expr
from adf_merge.schema import HASH_COLUMNS, SOURCE_COLUMNS

BASELINE = f"{catalog}.bronze.{dbutils.widgets.get('baseline_table')}"
CURRENT = f"{catalog}.bronze.adf_usa_current"
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
  SELECT '{RELEASE}' AS release_version, CAST(NULL AS DATE) AS release_date, {SOURCE_VERSION}L AS source_delta_version,
         'CONFIRMED' AS status, CAST(NULL AS BIGINT) AS vendor_added, CAST(NULL AS BIGINT) AS vendor_updated,
         CAST(NULL AS BIGINT) AS vendor_deleted, {expected_rows}L AS vendor_row_count, 'bootstrap' AS confirmed_by,
         current_timestamp() AS confirmed_at, 'Baseline. Source pin = the commit that set precisely.data.vintage.' AS notes
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
   current_table_version, hash_spec_version, started_at, finished_at, error, method)
VALUES ('{RELEASE}', {RELEASE_SEQ}, 'bootstrap', 'COMPLETE', {SOURCE_VERSION}, {n}, NULL, NULL, NULL,
        NULL, {HASH_SPEC_VERSION}, current_timestamp(), current_timestamp(), NULL, 'BASELINE')
"""
)

dbutils.notebook.exit(json.dumps({"loaded": stats, "baseline_rows": n}))
