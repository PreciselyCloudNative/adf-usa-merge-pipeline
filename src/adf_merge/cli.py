"""Job entry points (python_wheel_task). Each prints a JSON result; failures raise so the task fails."""

import argparse
import json

from adf_merge.detect import run_detect, run_pending
from adf_merge.release import DEFAULT_SOURCE_TABLE, Params, to_json


def _spark():
    from pyspark.sql import SparkSession

    return SparkSession.builder.getOrCreate()


def _parser(description: str) -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser(description=description)
    ap.add_argument("--catalog", required=True)
    ap.add_argument("--bronze-schema", default="bronze")
    ap.add_argument("--ops-schema", default="ops")
    ap.add_argument("--source-table", default=DEFAULT_SOURCE_TABLE)
    return ap


def _params(a, **extra) -> Params:
    return Params(
        catalog=a.catalog, bronze_schema=a.bronze_schema, ops_schema=a.ops_schema, source_table=a.source_table, **extra
    )


def detect_main(argv=None) -> None:
    a = _parser("Seed PROPOSED release labels from the vendor's release commits").parse_args(argv)
    print(to_json(run_detect(_spark(), _params(a))))


def release_main(argv=None) -> None:
    ap = _parser("Apply every pending adf_usa release in order")
    ap.add_argument("--require-confirmed", default="false")
    ap.add_argument("--allow-large-change", default="false")
    ap.add_argument("--max-row-count-change-pct", type=float, default=1.0)
    ap.add_argument("--deleted-tolerance", type=int, default=50)
    a = ap.parse_args(argv)
    p = _params(
        a,
        allow_large_change=a.allow_large_change.lower() == "true",
        max_row_count_change_pct=a.max_row_count_change_pct,
        deleted_tolerance=a.deleted_tolerance,
    )
    print(to_json(run_pending(_spark(), p, require_confirmed=a.require_confirmed.lower() == "true")))


if __name__ == "__main__":
    print(json.dumps({"error": "use the adf-detect / adf-release entry points"}))
