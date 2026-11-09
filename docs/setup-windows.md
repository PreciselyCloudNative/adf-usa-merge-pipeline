# Setup guide: Windows

Push the pipeline to Databricks and run it end to end. Mac/Linux users: see [setup-macos.md](setup-macos.md).
Back to the [README](../README.md).

All commands are for **Windows PowerShell** (not `cmd.exe`) and run from the repo root. Replace `<profile>` with your
profile name (list yours with `databricks auth profiles`) and `address_book` if your catalog differs.

## Prerequisites

* Databricks CLI >= 1.18 (v0.295.0 fails deploys with `openpgp: key expired`)
* An authenticated profile for your workspace (see "Log in" below)
* Python 3.10+ with `build`, installed **inside `.venv`** (not the global Python), for the wheel that the bundle builds
* `gh` and `git`
* Permission to create schemas in the catalog and read the shared catalogs `precisely_product_share.*`
* Serverless compute enabled in the workspace

### Install or update the Databricks CLI

```powershell
winget install Databricks.DatabricksCLI      # install
winget upgrade Databricks.DatabricksCLI      # update
databricks --version                         # open a new terminal first
```

If the upgrade fails with `Access is denied`, close other terminals/editors and retry from an elevated PowerShell, or
`winget uninstall` then `winget install`.

### Log in to the workspace

Opens a browser for SSO. Use your workspace URL (no trailing path such as `/browse`):

```powershell
databricks auth login --host https://<your-workspace>.cloud.databricks.com --profile <profile>
databricks auth profiles    # the profile should show Valid: YES
```

The bundle does not pin a workspace host. It deploys to whichever workspace `--profile` points at, so always pass
`--profile <profile>` (or set `DATABRICKS_HOST`). Logins expire; on `Refresh token is invalid`, log in again.

## 1. Get the code

```powershell
git clone https://github.com/PreciselyCloudNative/adf-usa-merge-pipeline.git
cd adf-usa-merge-pipeline
```

## 2. Python environment and local tests (no Databricks needed)

```powershell
python -m venv .venv
.venv\Scripts\python.exe -m pip install -e ".[dev]" build
.venv\Scripts\python.exe -m pytest tests -q
.venv\Scripts\Activate.ps1     # keep it active for the rest of this guide
```

`build` is needed in step 3. Install it in the venv: a uv- or system-managed Python refuses global installs
(`externally-managed-environment`). If activation is blocked by the execution policy, run
`Set-ExecutionPolicy -Scope CurrentUser RemoteSigned` once.

## 3. Deploy the bundle (builds the wheel, uploads code, creates the job)

The venv must be active so the bundle's `python -m build --wheel` finds `build`. If the deploy fails with
`python: command not found` or `No module named build`, activate the venv and retry.

```powershell
databricks bundle validate --strict -t prod --profile <profile>
databricks bundle deploy -t prod --profile <profile>
```

This creates the job `adf-usa-release` (tasks `adf-detect` -> `adf-release`, serverless, daily schedule) and syncs the
repo to `/Workspace/Users/<you>/.bundle/adf-usa-merge-pipeline/files`. Catalog and schema names are bundle variables
in `databricks.yml` (`--var catalog=...` to override).

## 4. Create the schemas and tables

```powershell
python scripts/apply_ddl.py --profile <profile> --catalog address_book
```

Creates `bronze`, `ops` and `silver` plus all tables and views from `sql/ddl/`. Safe to re-run.

## 5. Pick the baseline release and freeze it

A fresh install starts from whatever release Precisely has published **now**. `scripts/latest_release.py` asks the share
which one that is and prints JSON (`release_version`, `source_version`, `row_count`, `committed_at`). The commands below
read those values and copy that version of the main table into `adf_usa_baseline`. The share keeps only about 7 days of
history, so do this right away.

> **If `latest_release.py` stops with "older than the share's time-travel window":** the newest release is still
> the latest, but Precisely has already vacuumed its data files, so it can no longer be copied. Do not continue and do
> not pick a newer version by hand (the row count would not match the release label). Wait for the next release, or ask
> Precisely to extend the share's `delta.deletedFileRetentionDuration`.

```powershell
$r = python scripts/latest_release.py --profile <profile> | ConvertFrom-Json

databricks experimental aitools tools query --profile <profile> "CREATE TABLE address_book.bronze.adf_usa_baseline TBLPROPERTIES ('release_version' = '$($r.release_version)') AS SELECT * FROM precisely_product_share.precisely_db_property_graph.adf_usa VERSION AS OF $($r.source_version)"
"release_version=$($r.release_version) source_version=$($r.source_version) expected_rows=$($r.row_count)"
```

The row count must equal `row_count` (the bootstrap checks it):

```powershell
databricks experimental aitools tools query --profile <profile> "SELECT count(*) FROM address_book.bronze.adf_usa_baseline"
```

## 6. Load `adf_usa_current` from the baseline and seed the labels

`scripts/bootstrap_load_current.py` is a notebook. It hashes the baseline into `adf_usa_current` (status BASELINE,
seq 1), seeds `ops.release_label` with the baseline release and writes the baseline row of `ops.release_log`. It refuses
to run twice.

**Run this in the same terminal as step 5**, which set `$r`. If you opened a new terminal, do not re-run
`latest_release.py` (the share may have published a newer release since the baseline was frozen). Set the variable by
hand from the values step 5 printed:
`$r = @{release_version="2026.10.2"; source_version=213; row_count=194475857}`.

Replace `<you>` and `<profile>`. The closing `"@` must stay at the start of its line. The job file is written without a
BOM to your temp folder, so nothing is left in the repo. The `@` path is inside double quotes because PowerShell treats
a bare `@` as splatting.

```powershell
$ROOT = "/Workspace/Users/<you>/.bundle/adf-usa-merge-pipeline/files"
databricks workspace import "$ROOT/scripts/bootstrap_load_current" --file scripts/bootstrap_load_current.py --format SOURCE --language PYTHON --overwrite --profile <profile>
$json = @"
{"run_name": "adf-bootstrap",
 "tasks": [{"task_key": "main",
   "notebook_task": {"notebook_path": "$ROOT/scripts/bootstrap_load_current",
                     "base_parameters": {"catalog": "address_book", "src_path": "$ROOT/src",
                                         "baseline_table": "adf_usa_baseline",
                                         "release_version": "$($r.release_version)",
                                         "source_version": "$($r.source_version)",
                                         "expected_rows": "$($r.row_count)"}},
   "environment_key": "e"}],
 "environments": [{"environment_key": "e", "spec": {"client": "4"}}]}
"@
[System.IO.File]::WriteAllText("$env:TEMP\bootstrap.json", $json)
databricks jobs submit --json "@$env:TEMP\bootstrap.json" --profile <profile>
```

If the run fails with `Workload failed`, read the real error: `databricks jobs get-run <run_id> --profile <profile>`,
take the `run_id` of the `main` task, then `databricks jobs get-run-output <task_run_id> --profile <profile>`
(see `error` and `error_trace`).

Check: `SELECT status, count(*) FROM address_book.bronze.adf_usa_current GROUP BY 1` gives `row_count` rows, all BASELINE.

## 7. Create the consumer grants (once)

Use single quotes, so the backticks around the group name are literal:

```powershell
databricks experimental aitools tools query --profile <profile> 'GRANT USE CATALOG ON CATALOG address_book TO `account users`'
databricks experimental aitools tools query --profile <profile> 'GRANT USE SCHEMA, SELECT ON SCHEMA address_book.silver TO `account users`'
```

Use a narrower group when you have one (see `sql/grants.template.sql`).

## 8. Run the pipeline

```powershell
databricks bundle run adf_usa_release -t prod --profile <profile>
```

`adf-detect` finds vintages newer than the baseline and seeds PROPOSED labels. `adf-release` applies every pending
release in order. The task output is JSON; look for `"method": "CHANGEFILE"` and `"audit": "PASSED"`.

If the baseline is already the newest release, the run reports nothing pending: that is normal. Run it again after the
next Precisely release (weekly), once its changefile has landed (normally the same day).

## Next

[Verify the run and drop the baseline table](../README.md#verify-and-finish-setup).
