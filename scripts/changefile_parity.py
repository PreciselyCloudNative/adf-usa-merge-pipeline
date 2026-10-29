# Databricks notebook source
# Read-only check: does hash(changefile with decimal(11,6) cast to double) equal the stored hash of the main-table row?
import json, sys
dbutils.widgets.text("src_path", "")
sys.path.insert(0, dbutils.widgets.get("src_path"))
from pyspark.sql import functions as F
from adf_merge.hashing import row_hash_expr
from adf_merge.schema import SOURCE_COLUMNS

cf = spark.table("precisely_product_share.precisely_db_property_graph.adf_usa_changefile")
cf = cf.withColumn("LATITUDE", F.col("LATITUDE").cast("double")).withColumn("LONGITUDE", F.col("LONGITUDE").cast("double"))
h = cf.filter("CHANGE in ('A','U')").select(*SOURCE_COLUMNS, row_hash_expr().alias("cf_hash"))
cur = spark.table("address_book.bronze.adf_usa_current").filter("release_seq = 2 AND status IN ('ADDED','UPDATED')").select("PBKEY", "row_hash")
j = h.join(cur, "PBKEY", "full_outer")
res = j.agg(
    F.count("*").alias("rows"),
    F.sum((F.col("cf_hash") == F.col("row_hash")).cast("int")).alias("equal"),
    F.sum((F.col("cf_hash") != F.col("row_hash")).cast("int")).alias("different"),
    F.sum((F.col("cf_hash").isNull() | F.col("row_hash").isNull()).cast("int")).alias("unmatched"),
).collect()[0].asDict()
sample = [r.asDict() for r in j.filter(F.col("cf_hash") != F.col("row_hash")).select("PBKEY").limit(5).collect()]
dbutils.notebook.exit(json.dumps({**res, "sample_diff": sample}, default=str))
