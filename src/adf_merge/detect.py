"""Detect new Precisely releases of adf_usa and apply the pending ones in order.

The vendor sets `precisely.data.vintage` / `precisely.data.count` with a SET TBLPROPERTIES commit 1-2 minutes
AFTER its release MERGE finishes. That commit is the release marker, so:
    * a new vintage in the source history  => a finished release is available
    * the version of that commit           => the pin

The vendor may re-mark the SAME vintage later (e.g. the Thursday NJ_DELETION job deletes rows and sets the marker
again ~10 hours after the weekly CDC marker, with a new count). A newer marker version for a vintage than the pin we
applied therefore means "the release changed after we took it": detect re-pins the label, and run_pending rolls the
release back and re-applies it at the new pin (see pending_amendments).

Delta Sharing cannot push events, so a scheduled job polls (detect -> release).
"""

import json
import re
from dataclasses import replace

from adf_merge.changefile import run_release_auto
from adf_merge.release import GateError, Params, read_source, rollback_release

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


RETENTION_ERROR = "DELTA_UNSUPPORTED_TIME_TRAVEL_BEYOND_DELETED_FILE_RETENTION_DURATION"
_RETENTION_RE = re.compile(r"deletedFileRetentionDuration \((\d+) (\w+)\)")


def retention_expired(error_text: str) -> str | None:
    """If a time-travel probe failed because the pinned version is past the share's file retention,
    return the retention window as text (e.g. '168 HOURS'); otherwise None."""
    if RETENTION_ERROR not in (error_text or ""):
        return None
    m = _RETENTION_RE.search(error_text)
    return f"{m.group(1)} {m.group(2)}" if m else "unknown"


def plan_detection(commits: list[dict], labels: dict[str, dict], table_props: dict) -> dict:
    """Decide which release_label rows to seed and which known ones were re-marked. `labels` is {release_version: row}
    where run_detect adds `_state`, `_seq` and `_applied_pin` (from release_log) to each.

    A re-mark is a marker commit for a known vintage with a version above its pin (count may or may not change):
        * COMPLETE and the newest applied release  -> "amend": re-pin the label, run_pending re-applies it
        * COMPLETE but a newer release is applied  -> anomaly (needs a human; rollback only reaches the newest)
        * not started yet, label still PROPOSED    -> "repin": just move the pin
        * anything else in flight / CONFIRMED      -> anomaly (as before)

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
        return {"new_labels": [], "anomalies": anomalies, "remarks": []}

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
    complete_seqs = [r["_seq"] for r in labels.values() if r.get("_state") == "COMPLETE" and r.get("_seq") is not None]
    newest_complete_seq = max(complete_seqs) if complete_seqs else None
    new_labels = []
    remarks = []
    for c in ordered:
        k = vintage_key(c["vintage"])
        existing = labels.get(c["vintage"])
        if existing:
            pin = existing.get("source_delta_version")
            state = existing.get("_state")
            if existing.get("status") in SKIPPED_STATUSES or pin is None or c["version"] <= pin:
                continue
            remark = {
                "release_version": c["vintage"],
                "old_pin": pin,
                "new_pin": c["version"],
                "old_count": existing.get("vendor_row_count"),
                "new_count": c["count"],
            }
            if state == "COMPLETE":
                applied = existing.get("_applied_pin")
                if applied is not None and c["version"] <= applied:
                    continue
                if existing.get("_seq") == newest_complete_seq:
                    remarks.append({**remark, "action": "amend"})
                else:
                    anomalies.append(
                        f"{c['vintage']}: re-marked at version {c['version']} (applied {applied}) but a newer release "
                        "is already applied; resolve by hand"
                    )
            elif state in (None, "ROLLED_BACK") and existing.get("status") == "PROPOSED":
                remarks.append({**remark, "action": "repin"})
            else:
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
    return {"new_labels": new_labels, "anomalies": anomalies, "remarks": remarks}


def pending_amendments(labels: dict[str, dict], log: dict[str, dict]) -> list[dict]:
    """The newest COMPLETE release whose label was re-pinned above the pin it was applied at (detect did that)."""
    done = [r for r in log.values() if r["state"] == "COMPLETE"]
    if not done:
        return []
    newest = max(done, key=lambda r: r["release_seq"])
    label = labels.get(newest["release_version"])
    if not label or label["status"] in SKIPPED_STATUSES:
        return []
    pin, applied = label.get("source_delta_version"), newest.get("source_delta_version")
    if pin is None or applied is None or pin <= applied:
        return []
    return [label]


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


def _log(spark, p: Params) -> dict[str, dict]:
    return {r["release_version"]: r.asDict() for r in spark.table(p.release_log).collect()}


def _states(spark, p: Params) -> dict[str, str]:
    return {v: r["state"] for v, r in _log(spark, p).items()}


def run_detect(spark, p: Params) -> dict:
    """Seed PROPOSED labels for new vendor releases. Raises DetectError on inconsistencies."""
    labels = _labels(spark, p)
    log = _log(spark, p)
    for v, row in labels.items():
        mine = log.get(v) or {}
        row["_state"], row["_seq"], row["_applied_pin"] = mine.get("state"), mine.get("release_seq"), mine.get("source_delta_version")
    commits = parse_vintage_commits(_history(spark, p.source_table))
    plan = plan_detection(commits, labels, _table_props(spark, p.source_table))
    if plan["anomalies"]:
        raise DetectError("; ".join(plan["anomalies"]))
    for r in plan["remarks"]:
        count = r["new_count"] if r["new_count"] is not None else "NULL"
        note = f"re-marked by vendor ({r['action']}): pin {r['old_pin']}->{r['new_pin']}, count {r['old_count']}->{r['new_count']}"
        spark.sql(
            f"""UPDATE {p.release_label} SET source_delta_version = {r['new_pin']}, vendor_row_count = {count},
            notes = concat_ws(' | ', notes, '{note}') WHERE release_version = '{r['release_version']}'"""
        )
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
        "remarked": plan["remarks"],
        "pending": [r["release_version"] for r in pending],
    }


def pin_readable(spark, p: Params, pin: int) -> bool:
    try:
        read_source(spark, p, pin).limit(1).count()
        return True
    except Exception:  # noqa: BLE001 - expired time travel surfaces as various Delta/Sharing errors
        return False


def run_amendments(spark, p: Params) -> list[dict]:
    """Re-apply the newest COMPLETE release if the vendor re-marked it (label pin moved above the applied pin).

    Rolls it back (RESTORE to the previous release's version, drop its change-log rows) and runs it again at the new
    pin, changefile first. If anything fails the release is left FAILED/ROLLED_BACK and is picked up as an ordinary
    pending release on the next run.
    """
    results = []
    labels, log = _labels(spark, p), _log(spark, p)
    for label in pending_amendments(labels, log):
        version = label["release_version"]
        # No deferral here: after the rollback the table is missing this release, so use the changefile if it is ready
        # right now and otherwise run the MERGE immediately instead of waiting days for the changefile.
        pp = replace(p, release_version=version, changefile_wait_days=0)
        rolled = rollback_release(spark, pp)
        out = run_release_auto(spark, pp)
        out["amended"] = {"from_pin": log[version]["source_delta_version"], "to_pin": label["source_delta_version"],
                          "rollback": rolled}
        results.append(out)
    return results


def run_pending(spark, p: Params, require_confirmed: bool = False) -> dict:
    """Apply every pending release in order, one at a time. Stops (raises) on the first failure,
    or quietly when a release is deferred while its changefile has not arrived.

    A pending release whose pinned version has expired (time-travel window) while a LATER release is pending is
    marked SUPERSEDED: the later release diffs against our current state, so no change is lost, it is just
    attributed to the later release_version.
    """
    results = run_amendments(spark, p)
    if results and results[-1]["result"] == "deferred":  # later releases wait for it
        return {"applied": results}
    pending = pending_releases(_labels(spark, p), _states(spark, p))
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
        out = run_release_auto(spark, replace(p, release_version=version))
        results.append(out)
        if out["result"] == "deferred":  # later releases must wait for this one
            break
    return {"applied": results}
