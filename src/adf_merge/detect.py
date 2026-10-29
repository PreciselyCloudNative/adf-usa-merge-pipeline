"""Detect new Precisely releases of adf_usa and apply the pending ones in order.

The vendor sets `precisely.data.vintage` / `precisely.data.count` with a SET TBLPROPERTIES commit 1-2 minutes
AFTER its release MERGE finishes. That commit is the release marker, so:
    * a new vintage in the source history  => a finished release is available
    * the version of that commit           => the pin (later commits, e.g. Thursday NJ deletes, belong to the NEXT release)

Delta Sharing cannot push events, so a scheduled job polls (detect -> release).
"""

import json
import re
from dataclasses import replace

from adf_merge.release import GateError, Params, read_source, run_release

VINTAGE_PROP = "precisely.data.vintage"
COUNT_PROP = "precisely.data.count"
VINTAGE_RE = re.compile(r"^(\d{4})\.(\d{1,2})(?:\.(\d+))?$")
SKIPPED_STATUSES = ("SUPERSEDED", "REJECTED")


class DetectError(RuntimeError):
    """The source looks inconsistent; a human must look before anything is applied."""


# ---------------------------------------------------------------------------------------------
# Pure logic (unit-tested without Spark)
# ---------------------------------------------------------------------------------------------


def vintage_key(vintage: str) -> tuple[int, int, int]:
    m = VINTAGE_RE.match(vintage or "")
    if not m:
        raise DetectError(f"vintage {vintage!r} does not look like YYYY.MM.N")
    return int(m.group(1)), int(m.group(2)), int(m.group(3) or 0)


def parse_vintage_commits(history_rows: list[dict]) -> list[dict]:
    """Release-marker commits from DESCRIBE HISTORY rows, oldest first."""
    out = []
    for r in history_rows:
        if r.get("operation") != "SET TBLPROPERTIES":
            continue
        params = r.get("operationParameters") or {}
        if isinstance(params, str):  # SQL warehouse returns JSON text, Spark returns a map
            params = json.loads(params)
        raw = params.get("properties")
        props = json.loads(raw) if isinstance(raw, str) else (raw or {})
        if VINTAGE_PROP not in props:
            continue
        count = props.get(COUNT_PROP)
        out.append(
            {
                "version": int(r["version"]),
                "vintage": props[VINTAGE_PROP],
                "count": int(count) if count not in (None, "") else None,
                "committed_at": r.get("timestamp"),
            }
        )
    return sorted(out, key=lambda c: c["version"])


def plan_detection(commits: list[dict], labels: dict[str, dict], table_props: dict) -> dict:
    """Decide which release_label rows to seed. `labels` is {release_version: row}.

    Only vintages newer than every known label are seeded, so old history never becomes "pending".
    History before the newest tracked release's pin is ignored entirely (the vendor's older vintages are
    irregular, e.g. 2026.9.0 / 2026.6.25, and out of order).
    """
    anomalies = []
    pins = [r["source_delta_version"] for r in labels.values() if r.get("source_delta_version") is not None]
    if pins:
        commits = [c for c in commits if c["version"] >= max(pins)]
    for c in commits:
        try:
            vintage_key(c["vintage"])
        except DetectError as e:
            anomalies.append(str(e))
    if anomalies:
        return {"new_labels": [], "anomalies": anomalies}

    # latest marker commit per vintage is the pin
    by_vintage: dict[str, dict] = {}
    for c in commits:
        by_vintage[c["vintage"]] = c
    ordered = sorted(by_vintage.values(), key=lambda c: c["version"])
    keys = [vintage_key(c["vintage"]) for c in ordered]
    if keys != sorted(keys):
        anomalies.append("vintages are not increasing with commit order: " + ", ".join(c["vintage"] for c in ordered))

    prop_vintage = table_props.get(VINTAGE_PROP)
    if ordered and prop_vintage != ordered[-1]["vintage"]:
        anomalies.append(f"table property vintage {prop_vintage!r} != newest release commit {ordered[-1]['vintage']!r}")

    known = [vintage_key(v) for v in labels]
    newest_known = max(known) if known else None
    new_labels = []
    for c in ordered:
        k = vintage_key(c["vintage"])
        existing = labels.get(c["vintage"])
        if existing:
            pin = existing.get("source_delta_version")
            skipped = existing.get("status") in SKIPPED_STATUSES or existing.get("_state") == "COMPLETE"
            if pin is not None and pin != c["version"] and not skipped:
                anomalies.append(
                    f"{c['vintage']}: label pin {pin} differs from newest release commit {c['version']}; resolve by hand"
                )
            continue
        if newest_known is not None and k < newest_known:
            continue  # history older than anything we track
        new_labels.append(
            {
                "release_version": c["vintage"],
                "release_date": str(c["committed_at"])[:10] if c["committed_at"] else None,
                "source_delta_version": c["version"],
                "vendor_row_count": c["count"],
            }
        )
    return {"new_labels": new_labels, "anomalies": anomalies}


def pending_releases(labels: dict[str, dict], states: dict[str, str]) -> list[dict]:
    """Labels not yet COMPLETE and newer than the newest COMPLETE release, in commit order."""
    done = [vintage_key(v) for v, s in states.items() if s == "COMPLETE"]
    newest_done = max(done) if done else None
    out = []
    for v, row in labels.items():
        if states.get(v) == "COMPLETE" or row["status"] in SKIPPED_STATUSES:
            continue
        if newest_done is not None and vintage_key(v) <= newest_done:
            continue
        out.append(row)
    return sorted(out, key=lambda r: r["source_delta_version"])


# ---------------------------------------------------------------------------------------------
# Spark steps
# ---------------------------------------------------------------------------------------------


def _history(spark, table: str) -> list[dict]:
    return [r.asDict() for r in spark.sql(f"DESCRIBE HISTORY {table}").collect()]


def _table_props(spark, table: str) -> dict:
    return {r["key"]: r["value"] for r in spark.sql(f"SHOW TBLPROPERTIES {table}").collect()}


def _labels(spark, p: Params) -> dict[str, dict]:
    return {r["release_version"]: r.asDict() for r in spark.table(p.release_label).collect()}


def _states(spark, p: Params) -> dict[str, str]:
    return {r["release_version"]: r["state"] for r in spark.table(p.release_log).collect()}


def run_detect(spark, p: Params) -> dict:
    """Seed PROPOSED labels for new vendor releases. Raises DetectError on inconsistencies."""
    labels = _labels(spark, p)
    states = _states(spark, p)
    for v, row in labels.items():
        row["_state"] = states.get(v)
    commits = parse_vintage_commits(_history(spark, p.source_table))
    plan = plan_detection(commits, labels, _table_props(spark, p.source_table))
    if plan["anomalies"]:
        raise DetectError("; ".join(plan["anomalies"]))
    for n in plan["new_labels"]:
        date = f"DATE'{n['release_date']}'" if n["release_date"] else "NULL"
        count = n["vendor_row_count"] if n["vendor_row_count"] is not None else "NULL"
        spark.sql(
            f"""INSERT INTO {p.release_label}
            (release_version, release_date, source_delta_version, status, vendor_row_count, notes)
            VALUES ('{n['release_version']}', {date}, {n['source_delta_version']}, 'PROPOSED', {count},
                    'auto-detected from vendor SET TBLPROPERTIES commit')"""
        )
    pending = pending_releases(_labels(spark, p), _states(spark, p))
    return {
        "seeded": [n["release_version"] for n in plan["new_labels"]],
        "pending": [r["release_version"] for r in pending],
    }


def pin_readable(spark, p: Params, pin: int) -> bool:
    try:
        read_source(spark, p, pin).limit(1).count()
        return True
    except Exception:  # noqa: BLE001 - expired time travel surfaces as various Delta/Sharing errors
        return False


def run_pending(spark, p: Params, require_confirmed: bool = False) -> dict:
    """Apply every pending release in order, one at a time. Stops (raises) on the first failure.

    A pending release whose pinned version has expired (time-travel window) while a LATER release is pending is
    marked SUPERSEDED: the later release diffs against our current state, so no change is lost, it is just
    attributed to the later release_version.
    """
    pending = pending_releases(_labels(spark, p), _states(spark, p))
    results = []
    for i, label in enumerate(pending):
        version = label["release_version"]
        if require_confirmed and label["status"] != "CONFIRMED":
            raise GateError(f"{version} is {label['status']}, not CONFIRMED, and require_confirmed is set")
        later = i < len(pending) - 1
        if later and not pin_readable(spark, p, label["source_delta_version"]):
            spark.sql(
                f"""UPDATE {p.release_label} SET status = 'SUPERSEDED',
                notes = concat_ws(' | ', notes, 'pin {label['source_delta_version']} expired; changes land in a later release')
                WHERE release_version = '{version}'"""
            )
            results.append({"release": version, "result": "superseded"})
            continue
        results.append(run_release(spark, replace(p, release_version=version)))
    return {"applied": results}
