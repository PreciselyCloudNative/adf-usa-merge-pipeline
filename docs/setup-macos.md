# Setup guide: macOS (and Linux)

Push the pipeline to Databricks and run it end to end. Windows users: see [setup-windows.md](setup-windows.md).
Back to the [README](../README.md).

All commands run from the repo root in bash or zsh. Replace `<profile>` with your profile name (list yours with
`databricks auth profiles`) and `address_book` if your catalog differs.

## Prerequisites

* Databricks CLI >= 1.18 (v0.295.0 fails deploys with `openpgp: key expired`)
* An authenticated profile for your workspace (see "Log in" below)
* Python 3.10+ with `build`, installed **inside `.venv`** (not the global Python), for the wheel that the bundle builds
* `gh` and `git`
* Permission to create schemas in the catalog and read the shared catalogs `precisely_product_share.*`
* Serverless compute enabled in the workspace

### Install or update the Databricks CLI

```bash
brew tap databricks/tap && brew install databricks    # install
brew upgrade databricks                               # update
databricks --version                                  # open a new terminal first
```

### Log in to the workspace

Opens a browser for SSO. Use your workspace URL (no trailing path such as `/browse`):

```bash
databricks auth login --host https://<your-workspace>.cloud.databricks.com --profile <profile>
databricks auth profiles    # the profile should show Valid: YES
```

The bundle does not pin a workspace host. It deploys to whichever workspace `--profile` points at, so always pass
`--profile <profile>` (or set `DATABRICKS_HOST`). Logins expire; on `Refresh token is invalid`, log in again.

## 1. Get the code

```bash
git clone https://github.com/PreciselyCloudNative/adf-usa-merge-pipeline.git
cd adf-usa-merge-pipeline
```

## 2. Python environment and local tests (no Databricks needed)

```bash
python3 -m venv .venv
.venv/bin/python -m pip install -e '.[dev]' build
.venv/bin/python -m pytest tests -q
source .venv/bin/activate     # keep it active for the rest of this guide
```

`build` is needed in step 3. Install it in the venv: a uv- or system-managed Python refuses global installs
(`externally-managed-environment`).

## 3. Deploy the bundle (builds the wheel, uploads code, creates the job)

The venv must be active so the bundle's `python -m build --wheel` finds `build`. If the deploy fails with
`python: command not found` (macOS only ships `python3`) or `No module named build`, activate the venv and retry.

```bash
databricks bundle validate --strict -t prod --profile <profile>
databricks bundle deploy -t prod --profile <profile>
```

This creates the job `adf-usa-release` (tasks `adf-detect` -> `adf-release`, serverless, daily schedule) and syncs the
repo to `/Workspace/Users/<you>/.bundle/adf-usa-merge-pipeline/files`. Catalog and schema names are bundle variables
in `databricks.yml` (`--var catalog=...` to override).

## 4. Create the schemas and tables

```bash
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

```bash
J=$(python scripts/latest_release.py --profile <profile>)
REL=$(python -c "import sys,json; print(json.loads(sys.argv[1])['release_version'])" "$J")
VER=$(python -c "import sys,json; print(json.loads(sys.argv[1])['source_version'])" "$J")
ROWS=$(python -c "import sys,json; print(json.loads(sys.argv[1])['row_count'])" "$J")

databricks experimental aitools tools query --profile <profile> "CREATE TABLE address_book.bronze.adf_usa_baseline TBLPROPERTIES ('release_version' = '$REL') AS SELECT * FROM precisely_product_share.precisely_db_property_graph.adf_usa VERSION AS OF $VER"
echo "release_version=$REL source_version=$VER expected_rows=$ROWS"
```

The row count must equal `ROWS` (the bootstrap checks it):

```bash
databricks experimental aitools tools query --profile <profile> "SELECT count(*) FROM address_book.bronze.adf_usa_baseline"
```

## 6. Load `adf_usa_current` from the baseline and seed the labels

`scripts/bootstrap_load_current.py` is a notebook. It hashes the baseline into `adf_usa_current` (status BASELINE,
seq 1), seeds `ops.release_label` with the baseline release and writes the baseline row of `ops.release_log`. It refuses
to run twice.

**Run this in the same terminal as step 5**, which set `$REL`, `$VER` and `$ROWS`. If you opened a new terminal, do not
re-run `latest_release.py` (the share may have published a newer release since the baseline was frozen). Set the
variables by hand from the values step 5 printed, e.g. `REL=2026.10.2 VER=213 ROWS=194475857`.

```bash
ROOT=/Workspace/Users/<you>/.bundle/adf-usa-merge-pipeline/files
databricks workspace import $ROOT/scripts/bootstrap_load_current --file scripts/bootstrap_load_current.py \
  --format SOURCE --language PYTHON --overwrite --profile <profile>
cat > /tmp/bootstrap.json <<JSON
{"run_name": "adf-bootstrap",
 "tasks": [{"task_key": "main",
   "notebook_task": {"notebook_path": "$ROOT/scripts/bootstrap_load_current",
                     "base_parameters": {"catalog": "address_book", "src_path": "$ROOT/src",
                                         "baseline_table": "adf_usa_baseline",
                                         "release_version": "$REL",
                                         "source_version": "$VER",
                                         "expected_rows": "$ROWS"}},
   "environment_key": "e"}],
 "environments": [{"environment_key": "e", "spec": {"client": "4"}}]}
JSON
databricks jobs submit --json @/tmp/bootstrap.json --profile <profile>
```

If the run fails with `Workload failed`, read the real error: `databricks jobs get-run <run_id> --profile <profile>`,
take the `run_id` of the `main` task, then `databricks jobs get-run-output <task_run_id> --profile <profile>`
(see `error` and `error_trace`).

Check: `SELECT status, count(*) FROM address_book.bronze.adf_usa_current GROUP BY 1` gives `row_count` rows, all BASELINE.

## 7. Create the consumer grants (once)

```bash
databricks experimental aitools tools query --profile <profile> "GRANT USE CATALOG ON CATALOG address_book TO \`account users\`"
databricks experimental aitools tools query --profile <profile> "GRANT USE SCHEMA, SELECT ON SCHEMA address_book.silver TO \`account users\`"
```

Use a narrower group when you have one (see `sql/grants.template.sql`).

## 8. Run the pipeline

```bash
databricks bundle run adf_usa_release -t prod --profile <profile>
```

`adf-detect` finds vintages newer than the baseline and seeds PROPOSED labels. `adf-release` applies every pending
release in order. The task output is JSON; look for `"method": "CHANGEFILE"` and `"audit": "PASSED"`.

If the baseline is already the newest release, the run reports nothing pending: that is normal. Run it again after the
next Precisely release (weekly), once its changefile has landed (normally the same day).

## Next

[Verify the run and drop the baseline table](../README.md#verify-and-finish-setup).
