"""Spark expression must equal the reference hash byte for byte (local Spark, no cluster)."""

import os
import sys

import pytest

pyspark =pytest.importorskip("pyspark")

from pyspark.sql import SparkSession  # noqa: E402
from pyspark.sql import types as T  # noqa: E402

from adf_merge.hashing import row_hash_expr, row_hash_py  # noqa: E402
from adf_merge.schema import HASH_COLUMNS  # noqa: E402

SCHEMA = T.StructType(
    [
        T.StructField(name, T.StringType() if t == "string" else T.DoubleType(), True)
        for name, t in HASH_COLUMNS
    ]
)


@pytest.fixture(scope="module")
def spark():
    # Workers must use the same interpreter as the driver (the venv), not a system python.
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
    s = (
        SparkSession.builder.master("local[2]")
        .appName("adf-hash-test")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield s
    s.stop()


def _rows():
    def row(**kw):
        d = {name: None for name, _ in HASH_COLUMNS}
        d.update(kw)
        return d

    return [
        row(),
        row(UNIT=""),
        row(CITY="AUSTIN", ADMIN1="TX", POSTALCODE="78701", LATITUDE=30.2672, LONGITUDE=-97.7431),
        row(CITY="Zürich", STREETNAME="AB", STREET="C", LATITUDE=-0.0, LONGITUDE=0.0),
        row(CITY="東京", LATITUDE=71.325002, LONGITUDE=144.932908),
        row(FORMATTEDADDRESS="x" * 300, LATITUDE=13.247235, LONGITUDE=-176.640159),
        row(LATITUDE=30.267200000000001),
        row(LATITUDE=30.26720000000001),
    ]


def test_spark_matches_reference(spark):
    rows = _rows()
    df = spark.createDataFrame([tuple(r[n] for n, _ in HASH_COLUMNS) for r in rows], SCHEMA)
    got = [bytes(r["h"]) for r in df.select(row_hash_expr().alias("h")).collect()]
    want = [row_hash_py(r) for r in rows]
    assert got == want
