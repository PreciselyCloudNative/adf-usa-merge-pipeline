# Databricks notebook source
# One-off / manual: purge soft-deleted rows and vacuum. Parameters: src_path, catalog
import sys
dbutils.widgets.text("src_path", ""); dbutils.widgets.text("catalog", "address_book")
sys.path.insert(0, dbutils.widgets.get("src_path"))
from adf_merge.maintenance import purge_deleted, vacuum
from adf_merge.release import Params
p = Params(catalog=dbutils.widgets.get("catalog"))
purge_deleted(spark, p)
vacuum(spark, p)
dbutils.notebook.exit("purged+vacuumed")
