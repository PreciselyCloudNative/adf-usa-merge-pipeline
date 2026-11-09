# adf-usa-merge-pipeline

Incremental release pipeline for **Address Fabric USA** (`adf_usa`, Delta Sharing). Every Precisely release
(weekly `2026.10.1`, `2026.10.2`, ... and the monthly `X.0`) is applied to one table, `bronze.adf_usa_current`, so only
records that were **ADDED / UPDATED / DELETED** carry the new release version. Unchanged rows are never rewritten, so
downstream jobs only reprocess the change set.

## Architecture

![Architecture: changefile as the primary path, hash-diff MERGE as the fallback](docs/architecture.png)

Source: `docs/architecture.excalidraw` (open it at excalidraw.com to edit, then export the PNG).

* **Primary path: the changefile** (`precisely_product_share.precisely_db_property_graph.adf_usa_changefile`, rows with
  `CHANGE` = A/U/D). Precisely now publishes it the same day as the main table's release marker; it holds only the latest release.
  LATITUDE/LONGITUDE are `decimal(11,6)` or `double` (the vendor changed it with 2026.10.3) and are cast to double before hashing (hash-identical to the main table).
* **Fallback and audit: the hash MERGE** against the main table pinned at the vendor's release commit. After a changefile
  apply, a content fingerprint is compared to the pinned main table (audit PASSED; SKIPPED if the 7-day pin has expired).
  If any gate, validation or audit fails, the release is rolled back and the MERGE runs. The reason is stored in
  `ops.release_log.fallback_reason`.
* A release missed for any reason is always applied by the MERGE (the changefile only holds the latest one).
* Deletes are tombstones: attributes and `row_hash` are NULLed, PBKEY + status + release stay (NJ privacy removals are permanent).

### Tables (catalog `address_book`)

| Table / view | Purpose |
|---|---|
| `bronze.adf_usa_current` | Current state, one row per PBKEY, with `row_hash`, `status` (BASELINE/ADDED/UPDATED/DELETED), `release_version`, `release_seq`, `reappeared` |
| `bronze.adf_usa_change_log` | Slim per-release history (key, change type, hashes) |
| `ops.release_label` | Authority for release labels: source pin, vendor counts from the release document, PROPOSED/CONFIRMED |
| `ops.release_log` | One row per release run: state, counts, `method`, `fallback_reason`, `audit`, rollback pointer |
| `silver.adf_usa_current` | Consumer view: live rows only, no hash columns |
| `silver.adf_usa_change_feed` | Consumer view: the rows changed after the baseline, including tombstones |
| `silver.adf_usa_change_log`, `silver.adf_usa_releases` | Consumer views of the change history and the release list |

Consumers get `USE CATALOG` on the catalog and `USE SCHEMA`, `SELECT` on `silver` only (`sql/grants.template.sql`).

## Setup

Step-by-step guides to push the bundle to Databricks and run it end to end (deploy, create tables, freeze the baseline,
bootstrap, grants, first run):

* **[macOS / Linux setup guide](docs/setup-macos.md)**
* **[Windows (PowerShell) setup guide](docs/setup-windows.md)**

You need Databricks CLI >= 1.18, an authenticated profile, Python 3.10+, permission to create schemas in the catalog and
read `precisely_product_share.*`, and serverless compute. Each guide has the details. Once set up, the job runs daily at
06:30 UTC (`resources/adf_usa_release.job.yml`, `pause_status: UNPAUSED`). To pause it, set `PAUSED`, push through a PR and
redeploy. Failures email `alert_email` (`databricks.yml`), which defaults to the user who deployed the bundle
(`${workspace.current_user.userName}`); override with `--var alert_email=...`.

## Running a release

```bash
databricks bundle run adf_usa_release -t prod --profile <profile>
```

`adf-detect` finds vintages newer than the baseline and seeds PROPOSED labels. `adf-release` applies every pending release
in order. The task output is JSON; look for `"method": "CHANGEFILE"` and `"audit": "PASSED"`.

**Changefile alerts.** The job emails `alert_email` not only when it fails, but also when the changefile is not usable:
the release is **waiting** for it (deferred, up to `changefile-wait-days`), or it was **not ready / not consistent** and the
release was applied by the MERGE instead (the reason is in the email's task error and in `ops.release_log.fallback_reason`).
In both cases `adf-release` finishes its work and then exits non-zero with a `CHANGEFILE ALERT:` message, so the run shows
as failed in the UI even though no data is wrong. A deferred release repeats the alert on each daily run until the
changefile lands or the wait expires.

**Re-marked releases.** The vendor can set the same vintage again after its release (the Thursday NJ_DELETION job
deletes rows and sets `precisely.data.vintage` once more, with a new count, hours after the weekly marker). `adf-detect`
compares the newest marker version of each vintage with the pin we applied. If it moved up and that release is the newest
one applied, the label is re-pinned (`"remarked"` in the output, note appended to `release_label.notes`) and `adf-release`
rolls the release back and re-applies it at the new pin, changefile first. Any other re-mark (an older release, or one
in flight or CONFIRMED) fails detect for a human to resolve. A normal release defers while the changefile's marker is more than
10 minutes older than the main table's marker (not re-published yet); a re-applied release never defers, because the
table is missing it after the rollback, so it uses the changefile only if it is ready now and otherwise the MERGE. If the rollback itself fails
(e.g. the previous table version was already vacuumed) the job fails each day until someone resolves it.

## Verify and finish setup

```sql
-- what happened, and how
SELECT release_version, release_seq, state, method, audit, fallback_reason, added, updated, deleted
FROM address_book.ops.release_log ORDER BY release_seq;

-- current table: live rows + tombstones
SELECT status, count(*) FROM address_book.bronze.adf_usa_current GROUP BY 1;

-- consumers
SELECT * FROM address_book.silver.adf_usa_releases;
SELECT status, release_version, count(*) FROM address_book.silver.adf_usa_change_feed GROUP BY 1, 2;
```

Per applied release expect `method = CHANGEFILE`, `audit = PASSED`, added/updated/deleted equal to the Precisely document, and
live rows in `silver.adf_usa_current` equal to the release's row count. A `MERGE` method with a `fallback_reason` means the
changefile was rejected and the MERGE took over.

Then drop the baseline table (it is only needed for the bootstrap):

```bash
databricks experimental aitools tools query --profile <profile> "DROP TABLE address_book.bronze.adf_usa_baseline"
```

## Operating it

* **Job parameters** (job `adf-usa-release`, defaults from the bundle variables in `databricks.yml`):

  | Parameter | Default | Meaning |
  |---|---|---|
  | `catalog` | `address_book` | Target catalog (schemas `bronze`, `ops`, `silver`) |
  | `source_table` | `precisely_product_share.precisely_db_property_graph.adf_usa` | Main table Delta Share |
  | `changefile_table` | `precisely_product_share.precisely_db_property_graph.adf_usa_changefile` | Changefile Delta Share |

  Override for one run: `databricks bundle run adf_usa_release -t prod --profile <profile> --params catalog=my_catalog,source_table=...,changefile_table=...`
  (or "Run now with different parameters" in the UI). To change the scheduled defaults, edit the variables in `databricks.yml`
  (or `--var catalog=...` at deploy). A new catalog also needs steps 4 to 6 of the setup guide (DDL, baseline, bootstrap) run against it,
  using that catalog name. The pins in `ops.release_label` are Delta versions of the source table, so switching
  `source_table` on an existing install also means re-pinning (version numbers differ between shares).
* **Configuration**: job parameters in `resources/adf_usa_release.job.yml` (`use-changefile`, `audit`,
  `changefile-wait-days` (default 3), `require-confirmed`, `allow-large-change`).
  `changefile-wait-days: 0` applies by MERGE right away instead of waiting for the changefile.
* **Confirm release counts**: the labels carry the numbers from the Precisely release document. For a new release,
  `UPDATE ops.release_label SET status='CONFIRMED', vendor_added=..., vendor_updated=..., vendor_deleted=...,
  confirmed_by=..., confirmed_at=current_timestamp() WHERE release_version='...'`, ideally before the changefile apply.
* **Roll back the newest release**: `adf_merge.release.rollback_release` restores `adf_usa_current` to the previous release's
  Delta version and clears the change log. It only works inside the 7-day Delta retention.
* **Maintenance** runs inside `adf-release`: `REORG ... PURGE` after releases with deletes, `VACUUM` (7-day retention).
* **Failed run**: the release stays FAILED in `ops.release_log`; fix the cause and re-run the job (reruns are idempotent).

## Tests

* Local: `.venv/bin/python -m pytest tests -q` on macOS/Linux, `.venv\Scripts\python.exe -m pytest tests -q` on Windows (hashing, release helpers, detection).
* Synthetic scenarios on Databricks (26: normal, resurrection, rebuild, schema drift, rollback, missed releases, changefile
  fallbacks, ...). They use throwaway `zz_syn_*` schemas that are dropped afterwards:

  ```bash
  databricks workspace import $ROOT/scripts/run_synthetic --file scripts/run_synthetic.py --format SOURCE --language PYTHON --overwrite --profile <profile>
  # then submit it like step 6 of the setup guide with base_parameters {"root": "$ROOT", "catalog": "address_book"}
  ```

## Repo layout

```
src/adf_merge/   hashing, release (MERGE, gates, validate, rollback), changefile, detect, maintenance, cli
sql/ddl/         schemas, tables, silver views (${catalog} placeholder)
sql/migrations/  one-offs for tables created before a change
scripts/         apply_ddl.py, bootstrap_load_current.py (notebook), run_synthetic.py, run_release.py, run_maintenance.py
resources/       the DAB job; databricks.yml = bundle, variables, target
tests/           unit tests and tests/synthetic/scenarios.py
design/          the agent design rounds (history)
CLAUDE.md        project log and decisions
```
