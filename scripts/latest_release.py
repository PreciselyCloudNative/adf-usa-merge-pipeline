"""Print the vendor's latest release marker, i.e. what to use as the baseline for a fresh install.

    python3 scripts/latest_release.py --profile DEFAULT [--source-table precisely_product_share.precisely_db_property_graph.adf_usa]

Reads DESCRIBE HISTORY of the share through the Databricks CLI and takes the newest commit that set
precisely.data.vintage. Then probes that version with a one-row read, because DESCRIBE HISTORY still lists
versions whose data files the vendor has already vacuumed. Prints JSON: release_version, source_version
(the pin), row_count, committed_at. Exits non-zero (and prints nothing on stdout) if the pin cannot be read.
Time travel on the share only reaches back about 7 days, so use the result right away.
"""

import argparse
import json
import subprocess
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))
from adf_merge.detect import parse_vintage_commits, retention_expired  # noqa: E402


def _query(profile: str, sql: str) -> subprocess.CompletedProcess:
    return subprocess.run(
        ["databricks", "experimental", "aitools", "tools", "query", "--profile", profile, sql],
        capture_output=True, text=True,
    )


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--profile", required=True)
    ap.add_argument("--source-table", default="precisely_product_share.precisely_db_property_graph.adf_usa")
    a = ap.parse_args()

    hist = _query(a.profile, f"DESCRIBE HISTORY {a.source_table} LIMIT 60")
    if hist.returncode != 0:
        sys.exit(f"DESCRIBE HISTORY failed:\n{(hist.stderr or hist.stdout).strip()}")
    commits = parse_vintage_commits(json.loads(hist.stdout))
    if not commits:
        sys.exit("no release marker (precisely.data.vintage) in the last 60 commits")
    c = commits[-1]

    # Read-only probe: the history lists a version even after its data files were vacuumed.
    probe = _query(a.profile, f"SELECT 1 FROM {a.source_table} VERSION AS OF {c['version']} LIMIT 1")
    if probe.returncode != 0:
        err = (probe.stderr or probe.stdout).strip()
        window = retention_expired(err)
        if window:
            sys.exit(
                f"Release {c['vintage']} (source version {c['version']}, committed {c['committed_at']}) is older than "
                f"the share's time-travel window ({window}) and can no longer be read.\n"
                "Do NOT continue to step 5. Wait for Precisely to publish the next release, or ask them to extend "
                "the share's delta.deletedFileRetentionDuration."
            )
        sys.exit(f"Cannot read source version {c['version']} of {a.source_table}:\n{err}")

    print(json.dumps({"release_version": c["vintage"], "source_version": c["version"], "row_count": c["count"],
                      "committed_at": c["committed_at"]}, indent=2))


if __name__ == "__main__":
    main()
