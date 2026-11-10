# adf-usa-merge-pipeline

Incremental release pipeline for **Address Fabric USA** (`adf_usa`, Delta Sharing). Each Precisely release is applied to
one table, so only the records that were added, updated or deleted carry the new release version. Every past release
stays queryable as a view.

## How it works

![Architecture: changefile as the primary path, hash-diff MERGE as the fallback](docs/architecture.png)

1. **Detect** a new release in the vendor share (a daily job polls it).
2. **Apply** it from the vendor changefile. If the changefile is missing or inconsistent, fall back to a hash-diff MERGE.
3. **Audit** the result against the vendor table, then **publish** the change log and the release views.

Deletes are tombstones. Superseded versions are archived, so release N can be rebuilt exactly.
Details: [operations](docs/operations.md), [data model](docs/data-model.md).

## What consumers query

Catalog `address_book`. Consumers need access to `silver` and `gold` only ([grants](sql/grants.template.sql)).

| View | Contents |
|---|---|
| `silver.adf_usa_current` | Live records, latest state |
| `silver.adf_usa_change_feed` | Records changed since the baseline, including tombstones |
| `gold.adf_usa_<release>` | The table **as of** that release, e.g. `gold.adf_usa_2026_10_3` |
| `gold.adf_usa_latest` | Alias of the newest release |
| `gold.adf_usa_versions` | Index of the release views |

All tables and views: [data model](docs/data-model.md). How release views work: [release views](docs/release-views.md).

## Set up

Prerequisites: Databricks CLI >= 1.18 with an authenticated profile, Python 3.10+, permission to create schemas in the
catalog and read `precisely_product_share.*`, and serverless compute.

* [macOS / Linux guide](docs/setup-macos.md)
* [Windows (PowerShell) guide](docs/setup-windows.md)

Once set up, the job runs daily at 06:30 UTC and emails `alert_email` on failure or when the changefile is unusable.

## Run and operate

```bash
databricks bundle run adf_usa_release -t prod --profile <profile>
```

Verify a run, change parameters, confirm counts, roll back, handle alerts and re-marked releases: [operations](docs/operations.md).

## Test

```bash
.venv/bin/python -m pytest tests -q    # Windows: .venv\Scripts\python.exe
```

Local unit tests plus 30 synthetic scenarios on Databricks: [testing](docs/testing.md).

## Repo layout

```
src/adf_merge/   hashing, release, changefile, detect, gold, maintenance, cli
sql/ddl/         schemas, tables and views (${catalog} placeholder)
scripts/         DDL apply, bootstrap notebook, synthetic runner
resources/       the bundle job; databricks.yml = bundle, variables, target
tests/           unit tests and synthetic scenarios
docs/            guides and architecture diagram
design/          design rounds (history)
```
