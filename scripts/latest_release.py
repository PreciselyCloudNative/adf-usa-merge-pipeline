"""Print the vendor's latest release marker, i.e. what to use as the baseline for a fresh install.

    python3 scripts/latest_release.py --profile DEFAULT [--source-table precisely_product_share.precisely_db_property_graph.adf_usa]

Reads DESCRIBE HISTORY of the share through the Databricks CLI and takes the newest commit that set
precisely.data.vintage. Prints JSON: release_version, source_version (the pin), row_count, committed_at.
Time travel on the share only reaches back about 7 days, so use the result right away.
"""

import argparse
import json
import subprocess
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))
from adf_merge.detect import parse_vintage_commits  # noqa: E402


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--profile", required=True)
    ap.add_argument("--source-table", default="precisely_product_share.precisely_db_property_graph.adf_usa")
    a = ap.parse_args()
    out = subprocess.run(
        ["databricks", "experimental", "aitools", "tools", "query", "--profile", a.profile,
         f"DESCRIBE HISTORY {a.source_table} LIMIT 60"],
        check=True, capture_output=True, text=True,
    ).stdout
    commits = parse_vintage_commits(json.loads(out))
    if not commits:
        sys.exit("no release marker (precisely.data.vintage) in the last 60 commits")
    c = commits[-1]
    print(json.dumps({"release_version": c["vintage"], "source_version": c["version"], "row_count": c["count"],
                      "committed_at": c["committed_at"]}, indent=2))


if __name__ == "__main__":
    main()
