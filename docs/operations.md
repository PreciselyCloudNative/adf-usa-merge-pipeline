# Operations

Back to the [README](../README.md).

## Run a release

```bash
databricks bundle run adf_usa_release -t prod --profile <profile>
```

`adf-detect` finds vintages newer than the baseline and seeds PROPOSED labels. `adf-release` applies every pending
release in order and refreshes the gold views. The task output is JSON; look for `"method": "CHANGEFILE"` and `"audit": "PASSED"`.
The job runs daily at 06:30 UTC (`resources/adf_usa_release.job.yml`). To pause it, set `pause_status: PAUSED`, push
through a PR and redeploy.

## Verify

```sql
SELECT release_version, release_seq, state, method, audit, fallback_reason, added, updated, deleted
FROM address_book.ops.release_log ORDER BY release_seq;                         -- what happened, and how

SELECT status, count(*) FROM address_book.bronze.adf_usa_current GROUP BY 1;    -- live rows + tombstones
SELECT * FROM address_book.gold.adf_usa_versions ORDER BY release_seq;          -- release views, newest is_latest
```

Per release expect `method = CHANGEFILE`, `audit = PASSED`, counts equal to the Precisely document, and live rows in
`silver.adf_usa_current` equal to the release's row count. `MERGE` with a `fallback_reason` means the changefile was rejected.

After the first run, drop the baseline table (only the bootstrap needs it):

```bash
databricks experimental aitools tools query --profile <profile> "DROP TABLE address_book.bronze.adf_usa_baseline"
```

## Parameters

Job `adf-usa-release`, defaults from the bundle variables in `databricks.yml`:

| Parameter | Default | Meaning |
|---|---|---|
| `catalog` | `address_book` | Target catalog (schemas `bronze`, `ops`, `silver`, `gold`) |
| `source_table` | `precisely_product_share.precisely_db_property_graph.adf_usa` | Main table Delta Share |
| `changefile_table` | `precisely_product_share.precisely_db_property_graph.adf_usa_changefile` | Changefile Delta Share |

* **One run:** `databricks bundle run adf_usa_release -t prod --profile <profile> --params catalog=...,source_table=...,changefile_table=...`
  (or "Run now with different parameters" in the UI).
* **Scheduled defaults:** edit the variables in `databricks.yml`, or `--var catalog=...` at deploy.
* **New catalog:** repeat setup steps 4 to 6 (DDL, baseline, bootstrap) with that catalog name.
* **New `source_table` on an existing install:** re-pin `ops.release_label`; Delta version numbers differ between shares.
* **Task options** (`resources/adf_usa_release.job.yml`): `use-changefile`, `audit`, `changefile-wait-days` (default 3; `0` applies
  by MERGE immediately), `require-confirmed`, `allow-large-change`.

## Routine tasks

* **Confirm release counts** (ideally before the changefile apply):
  `UPDATE ops.release_label SET status='CONFIRMED', vendor_added=..., vendor_updated=..., vendor_deleted=..., confirmed_by=..., confirmed_at=current_timestamp() WHERE release_version='...'`
* **Roll back the newest release:** `adf_merge.release.rollback_release` restores `adf_usa_current` to the previous release's
  Delta version and clears that release's change log and history rows; the next run drops its gold view. Works only inside the 7-day Delta retention.
* **Failed run:** the release stays FAILED in `ops.release_log`. Fix the cause and re-run (reruns are idempotent).
* **Maintenance** runs inside `adf-release`: `REORG ... PURGE` after releases with deletes, `VACUUM` (7-day retention).

## Alerts

The job emails `alert_email` (default: the deploying user) on failure and also when the changefile is unusable:

* **Waiting:** the changefile has not arrived (deferred up to `changefile-wait-days`). The alert repeats daily until it lands.
* **Fallback:** the changefile was not ready or not consistent, so the MERGE applied the release. The reason is in the email and in `ops.release_log.fallback_reason`.

In both cases `adf-release` finishes its work and then exits non-zero with `CHANGEFILE ALERT:`, so the run shows as failed
although no data is wrong.

## Re-marked releases

The vendor can set the same vintage again after its release (the Thursday NJ_DELETION job deletes rows and re-sets
`precisely.data.vintage` with a new count). `adf-detect` compares the newest marker of each vintage with the pin we applied.

* **Newest applied release:** the label is re-pinned (`"remarked"` in the output) and `adf-release` rolls it back and
  re-applies it at the new pin, changefile first. It never defers: it uses the changefile if ready now, otherwise the MERGE.
* **Any other re-mark** (older release, in flight or CONFIRMED): detect fails for a human to resolve.
* **Not re-published yet:** a normal release defers while the changefile's marker is more than 10 minutes older than the main table's.
* If the rollback itself fails (e.g. the previous table version was vacuumed), the job fails daily until someone resolves it.
