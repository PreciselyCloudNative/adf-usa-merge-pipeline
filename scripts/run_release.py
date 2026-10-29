# Databricks notebook source
# Apply one release of adf_usa to adf_usa_current. Parameters (base_parameters):
#   catalog, release_version, src_path (workspace dir that contains adf_merge/), dry_run (true|false),
#   expect_added / expect_updated / expect_deleted (optional ints), deleted_tolerance

# COMMAND ----------

import sys

for name, default in [
    ("catalog", "address_book"),
    ("release_version", ""),
    ("src_path", ""),
    ("dry_run", "true"),
    ("expect_added", ""),
    ("expect_updated", ""),
    ("expect_deleted", ""),
    ("deleted_tolerance", "50"),
    ("allow_large_change", "false"),
]:
    dbutils.widgets.text(name, default)

get = dbutils.widgets.get
if get("src_path"):
    sys.path.insert(0, get("src_path"))

from adf_merge.release import Params, run_release, to_json


def _int(v):
    return int(v) if v.strip() else None


params = Params(
    catalog=get("catalog"),
    release_version=get("release_version"),
    dry_run=get("dry_run").lower() == "true",
    expect_added=_int(get("expect_added")),
    expect_updated=_int(get("expect_updated")),
    expect_deleted=_int(get("expect_deleted")),
    deleted_tolerance=int(get("deleted_tolerance")),
    allow_large_change=get("allow_large_change").lower() == "true",
)

# COMMAND ----------

result = run_release(spark, params)
dbutils.notebook.exit(to_json(result))
