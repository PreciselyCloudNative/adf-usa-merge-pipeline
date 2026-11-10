# Testing

Back to the [README](../README.md).

## Local

```bash
.venv/bin/python -m pytest tests -q          # Windows: .venv\Scripts\python.exe -m pytest tests -q
```

Covers hashing, release and gold helpers, SQL generation and detection. No Databricks needed.

## Synthetic scenarios (Databricks)

30 end-to-end scenarios: normal release, resurrection, rebuild, schema drift, rollback, missed releases, changefile
fallbacks, and release-view exactness. Each runs on a throwaway source table in `zz_syn_*` schemas that are dropped afterwards.

```bash
databricks workspace import $ROOT/scripts/run_synthetic --file scripts/run_synthetic.py --format SOURCE --language PYTHON --overwrite --profile <profile>
```

Then submit it as in step 6 of the setup guide, with `base_parameters` `{"root": "$ROOT", "catalog": "address_book"}`.
Add `"only": "g01,g02"` to run a subset. Upload `src/`, `sql/ddl/` and `tests/synthetic/` to `$ROOT` first.
