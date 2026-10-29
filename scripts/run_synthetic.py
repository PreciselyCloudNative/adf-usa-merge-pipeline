# Databricks notebook source
# Run the synthetic scenario suite on Databricks. Parameters: root (workspace dir holding src/, sql/, tests/),
# catalog, only (optional comma list of scenario names). Throwaway schemas are created and dropped per scenario.

# COMMAND ----------

import json
import sys
import time
import traceback

for name, default in [("root", ""), ("catalog", "address_book"), ("only", "")]:
    dbutils.widgets.text(name, default)
get = dbutils.widgets.get
root = get("root")
sys.path.insert(0, f"{root}/src")
sys.path.insert(0, f"{root}/tests/synthetic")

import scenarios

only = [s.strip() for s in get("only").split(",") if s.strip()]
todo = [f for f in scenarios.SCENARIOS if not only or f.__name__ in only or f.__name__.split("_")[0] in only]

# COMMAND ----------

results = []
for fn in todo:
    t0 = time.time()
    env = scenarios.Env(spark, get("catalog"), f"{root}/sql/ddl")
    try:
        env.load_baseline()
        fn(env)
        results.append({"scenario": fn.__name__, "ok": True, "secs": round(time.time() - t0, 1)})
    except Exception as e:  # noqa: BLE001
        results.append({"scenario": fn.__name__, "ok": False, "error": f"{type(e).__name__}: {e}"[:800],
                        "trace": traceback.format_exc()[-1200:], "secs": round(time.time() - t0, 1)})
    finally:
        env.drop()

failed = [r["scenario"] for r in results if not r["ok"]]
dbutils.notebook.exit(json.dumps({"passed": len(results) - len(failed), "failed": failed, "results": results}))
