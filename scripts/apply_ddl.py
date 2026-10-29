"""Create the schemas and tables from sql/ddl/ through the Databricks CLI (idempotent: CREATE ... IF NOT EXISTS / OR REPLACE).

    python3 scripts/apply_ddl.py --profile DEFAULT --catalog address_book [--only 01,02] [--dry-run]

03_silver_views.sql needs bronze.adf_usa_current and ops.release_label to exist, so the files run in name order.
"""

import argparse
import re
import subprocess
import sys
from pathlib import Path

DDL_DIR = Path(__file__).resolve().parent.parent / "sql" / "ddl"


def statements(path: Path, catalog: str) -> list[str]:
    text = "\n".join(l for l in path.read_text().splitlines() if not l.lstrip().startswith("--"))
    text = text.replace("${catalog}", catalog)
    return [s.strip() for s in text.split(";") if s.strip()]


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--profile", required=True)
    ap.add_argument("--catalog", required=True)
    ap.add_argument("--only", default="", help="comma list of file prefixes, e.g. 01,03")
    ap.add_argument("--dry-run", action="store_true")
    a = ap.parse_args()
    if not re.fullmatch(r"[A-Za-z0-9_]+", a.catalog):
        sys.exit(f"bad catalog name {a.catalog!r}")

    prefixes = [x for x in a.only.split(",") if x]
    files = [f for f in sorted(DDL_DIR.glob("*.sql")) if not prefixes or f.name[:2] in prefixes]
    todo = [f"CREATE SCHEMA IF NOT EXISTS {a.catalog}.bronze", f"CREATE SCHEMA IF NOT EXISTS {a.catalog}.ops"]
    for f in files:
        todo += statements(f, a.catalog)
    for sql in todo:
        print(("DRY  " if a.dry_run else "RUN  ") + sql.splitlines()[0][:100])
        if not a.dry_run:
            subprocess.run(
                ["databricks", "experimental", "aitools", "tools", "query", "--profile", a.profile, sql], check=True
            )


if __name__ == "__main__":
    main()
