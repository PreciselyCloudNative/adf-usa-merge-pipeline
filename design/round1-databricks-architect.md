# Round 1 - Databricks Principal SA proposal (ADF USA release-diff pipeline)

Scope: Databricks-side architecture. Design only; nothing created. `${catalog}`/`${schema}` are bundle variables throughout.

## 0. Facts I re-checked (read-only) and two corrections to CLAUDE.md

1. **A release label DOES exist on the source.** `DESCRIBE DETAIL precisely_product_share.addressing.adf_usa` returns table properties `precisely.data.vintage` (= `2026.10.2`) and `precisely.data.count` (= 194,475,857). History shows the vendor runs `SET TBLPROPERTIES` 1-2 minutes after each release MERGE (v217 -> 2026.10.0, v219 -> 2026.10.1, v227 -> 2026.10.2). `count` is also updated after Thursday DELETEs without touching `vintage`. Caveat: labels are imperfect (the 10-07 rebuild was first tagged `2026.9.3`, then `2026.9`), so the seed table stays authoritative; the property is the *trigger and cross-check*.
2. **The 2026.10.1 baseline is still reachable, and for longer than assumed.** `VERSION AS OF 219` reads fine: 194,466,619 rows, distinct PBKEY = 194,466,619 = the vintage-property count. Files removed by the 10-27 MERGE (v226) stay vacuum-eligible only after ~2026-11-03. (CLAUDE.md's version numbers v204/v212 are off; the real ones are v219 (10.1) and v226/227 (10.2).) **v219 is the clean 10.1 state; v220 (Thursday DELETE, 12 rows) is correctly counted into 10.2.** Reconciliation check: 194,466,619 + 9,467 - 217 - 12 = 194,475,857 = `precisely.data.count` at v227. The user's decisions tie out exactly.
3. NULLs are pervasive (UNIT null on 140.6M rows, empty string 0 rows) -> NULL handling in the hash is a real correctness issue (section 3).
4. The vendor MERGE rewrites ~85 files / 9.6 GB per release (no clustering), takes ~93 s on their side. This is our best proxy for our cost.
5. Source also reports `appendOnly` in table features, yet it is MERGEd; irrelevant to us but do not rely on source feature flags.

## 1. Where it runs

**Recommendation: one serverless Lakeflow Job, 4 tasks, deployed by a DAB, `performance_target: STANDARD`.**

Tasks: `detect` (cheap gate) -> `merge` (condition task: only if detect says NEW) -> `validate` -> `publish` (writes change-log + marks release SUCCESS). Notebook/SQL tasks, Python for orchestration logic, MERGE in SQL.

- **Detect new release**: `detect` reads `DESCRIBE DETAIL` + `DESCRIBE HISTORY LIMIT 20` on the source. NEW iff `vintage` property is non-null, maps to a seed-table row, is not `SUCCESS` in `release_log`, AND the property-set commit is >=30 min old (settle time) AND source `count` property == `count(*)` of the pinned version. Otherwise the run ends green with status SKIPPED (no merge compute).
- **Trigger**: scheduled poll, daily 18:00 UTC (cron, UTC pinned), not weekly: releases arrived Fri 10-16, Thu 10-22, Tue 10-27, i.e. no fixed weekday. A no-op poll costs a few seconds of serverless. Table-update triggers are not a safe assumption on Delta Sharing recipient catalogs - **I have not verified this; treat as open question**. Add `max_concurrent_runs: 1` and a manual `release_version` override parameter for re-runs/backfills.
- **Compute: serverless STANDARD.** Weekly batch, no SLA; a few minutes of start latency is irrelevant. PERFORMANCE_OPTIMIZED buys nothing; classic is only for R/Scala/Docker/native libs - none apply, and it adds cluster-sizing and idle-cost ownership.
- **Pin the source**: `detect` records `source_version`; all later tasks read `precisely_product_share.addressing.adf_usa VERSION AS OF <n>`, so a Thursday DELETE landing mid-run cannot produce a torn read.

**Rejected:**
- *Lakeflow Spark Declarative Pipeline with `AUTO CDC ... FROM SNAPSHOT`*: closest alternative (it diffs full snapshots for you). Rejected because: SCD1/2 output shapes do not give the explicit BASELINE/ADDED/UPDATED/DELETED + release label semantics; no natural place for pre-flight guards, release gating, idempotency keys or seed-table lookup; exact-hash control is hidden; a pipeline full-refresh can destroy the state table (our table is the only record of history once the 7-day window passes); debugging a failed update is harder than a failed SQL task. SDP earns its keep in multi-hop streaming, not a single weekly keyed diff. Revisit for Phase 2 if the changelog arrives as a stream.
- *Classic job / interactive cluster / DBSQL scheduled query*: more ops, no guard branching (a scheduled query cannot do the gate/branch cleanly).
- *Weekly fixed cron*: misses off-day releases by up to a week.

## 2. Physical model (Unity Catalog, `${catalog}.${schema}`)

All managed Delta tables, UC-governed.

**`adf_usa_current`** - one row per PBKEY ever seen within the tombstone retention window.
- PBKEY (PK, NOT NULL) + the 31 source attribute columns (same types) + `row_hash BIGINT` (section 3).
- `status STRING` CHECK in (BASELINE, ADDED, UPDATED, DELETED); `release_version STRING` (e.g. `2026.11.0`); `release_seq INT` (our monotonic integer, the sort key; labels like `2026.9` vs `2026.10.0` do not sort lexicographically); `changed_at TIMESTAMP`.
- Semantics: status/release describe the **last change to that row**. Unchanged rows keep their old status; a row ADDED in 2026.10.2 reads (ADDED, 2026.10.2) until it changes. Baseline rows = (BASELINE, 2026.10.1).
- Companion view `adf_usa_live` = `WHERE status <> 'DELETED'` for anyone who wants "the address table".

**Deletes: tombstone, with attributes NULLed.** The DELETED row keeps only PBKEY, `row_hash` (last-known), status, release_version/seq, changed_at; all 30 attribute columns are set to NULL in the MERGE. Why: downstream must be told "PBKEY X is gone" and a physical delete leaves nothing to find; but NJ privacy removals mean we must not retain the address. Implications I want handled explicitly:
- PBKEY is an opaque ID, but whether PBKEY (or GOVID/GEOID if you kept them) is itself linkable personal data is a **legal question**; I keep only PBKEY.
- **Delta itself retains the PII**: old file versions (time travel) and any CDF pre-images contain the row until VACUUM. Mitigations: `delta.deletedFileRetentionDuration = 7 days`, `delta.logRetentionDuration = 7 days`, weekly `VACUUM`, deletion vectors OFF on this table (or follow with `REORG ... APPLY (PURGE)`), and **do not enable CDF** (its `_change_data` files carry pre-images of deleted rows). The same 7-day bound the vendor uses; document it as the maximum PII residency.
- Tombstones are purged after 13 releases (~90 days) by the job; a consumer further behind than that must resync from `adf_usa_live`. PBKEY re-ADDED later simply overwrites the tombstone (ADDED).
- Rejected: hard delete only (consumers cannot learn of deletes, and the status requirement is lost); full-row tombstones (retains PII for the privacy removals).

**`adf_usa_change_log`** - append-only, **slim, no attributes**: (release_seq, release_version, PBKEY, change_type, row_hash_new). Needed? Yes, but small: `adf_usa_current` only shows the *latest* change per key, so "what changed in release X" is lost once a row changes again. It is derived from `adf_usa_current WHERE release_seq = X` right after the MERGE, so it is rebuildable and written idempotently (`MERGE ... ON (release_seq, PBKEY)` or `INSERT ... REPLACE WHERE release_seq = X`). Carries no PII, so it can be kept indefinitely. Size: ~10K-2.2M rows per release.

**`adf_usa_release_log`** - one row per release attempt-set: release_seq (PK), release_version, mode (BASELINE|MERGE|REBUILD), source_vintage_prop, source_version, source_commit_ts, source_row_count, vendor_count_prop, added/updated/deleted counts, expected_* from the doc (nullable), reconcile_ok, rows_after, status (STARTED|SUCCESS|FAILED|SKIPPED|BLOCKED), run_id, started/ended. **A release is real only when this row is SUCCESS** - the commit marker (Delta has no cross-table transaction).

**`adf_usa_release_label`** - the seed table: vintage/commit-date window -> release_version, release_seq, release_date, doc counts (A/U/D), doc URL. Maintained by hand from the Precisely release doc, in a version-controlled CSV in the bundle that is MERGEd on deploy. The job *fails closed* on an unknown vintage ("add a seed row"); never invents a label.

## 3. MERGE design

Source = pinned source snapshot with computed `row_hash`. Target = `adf_usa_current`.

```
MERGE INTO adf_usa_current t USING src s ON t.PBKEY = s.PBKEY
WHEN MATCHED AND t.status = 'DELETED'                      THEN UPDATE SET <all cols from s>, status='ADDED', release...  -- resurrection
WHEN MATCHED AND t.row_hash <> s.row_hash                  THEN UPDATE SET <all cols from s>, status='UPDATED', release...
WHEN NOT MATCHED BY TARGET                                 THEN INSERT (..., status='ADDED', release...)
WHEN NOT MATCHED BY SOURCE AND t.status <> 'DELETED'       THEN UPDATE SET <30 attrs>=NULL, status='DELETED', release...
```
Unchanged rows match no clause -> not rewritten (apart from file-level collateral, below).

- **Hash**: `xxhash64` over all 31 non-key columns in fixed order, typed values (LATITUDE/LONGITUDE hashed as stored, no rounding per decision 6) **plus a NULL bitmap** `(c1 IS NULL)<<0 | ...` as an extra argument. Reason: Spark's hash skips NULLs, so (NULL,'x') and ('x',NULL) can collide - with 140M NULL UNITs this is not theoretical. Empty-string vs NULL are different inputs and hash differently (no empty strings today, but the rule is deterministic). Collision risk: the comparison is per-PBKEY, so the relevant risk is a changed row hashing identically to its old value: 2^-64 per changed row; at 5M changed rows (a large X.0 release) ~3e-13 per release. The "birthday over 194M rows" figure is irrelevant because rows are never compared to each other. If the team wants zero argument: `sha2(...,256)` costs a few extra CPU-minutes and 32 bytes/row (~6 GB) - I do not recommend it. Hash column list is generated from the schema, ordered, and a `hash_spec_version` is stored in table properties; any change to the column list forces a controlled re-hash (all rows would otherwise read UPDATED).
- **Source schema drift guard**: fail if source columns != expected 32 (new column = hash definition change = human decision).
- **NOT MATCHED BY SOURCE** is the highest-risk clause (an empty/partial source deletes everything) -> only executed after the guards in section 4. Expressed in the same single MERGE, so adds/updates/deletes commit atomically in one Delta commit.
- **File rewrite avoidance**: a full-table scan/join is unavoidable and cheap (9-11 GB); the cost is *rewriting* files. Changed PBKEYs are scattered, so unclustered MERGE touches nearly every file (vendor rewrites 85 files/9.6 GB each time). Recommendation: **`CLUSTER BY (PBKEY)` (liquid clustering)** + **deletion vectors ON** (default for new managed tables in current DBR; `MERGE` then writes DVs plus new small files, not full rewrites) + **predictive optimization ON** for the schema (auto OPTIMIZE/VACUUM; verify it is enabled in this workspace) with a post-merge `OPTIMIZE` fallback. Small weekly releases (15-30K changed rows) should rewrite a handful of files; X.0 releases (2.2M changed) will touch most files regardless. Note: our DV/PURGE requirement for PII residency (section 2) means weekly `REORG ... APPLY (PURGE)` after releases with deletes - include it in `publish`.
- Rejected: partitioning (no natural low-cardinality column; PBKEY is the only stable key); Z-order (superseded by liquid); `overwrite` of the whole table each release (loses status semantics and destroys time travel); anti-join + manual three-step writes (non-atomic).
- **Runtime/cost**: vendor does the same-sized MERGE in ~93 s on their warehouse. Expect 5-12 min end-to-end on serverless STANDARD for a small release, 15-25 min for an X.0, plus 1-3 min for hash/validate/publish. Estimated ~1-3 DBU-hours per run -> low single-digit dollars; ~$5-20/month with the daily no-op poll. These are estimates; the 10.1->10.2 dry run gives the real number.

## 4. Safety guards (all in `detect`/`validate`; any failure => release_log BLOCKED/FAILED, current table untouched)

| Risk | Guard |
|---|---|
| Partial/bad source | `count(*)` of pinned version must equal `precisely.data.count` property; `abs(count - rows_current_live)/rows_current_live` <= 1% for X.N, <= 3% for X.0 (observed deltas: 0.01%-0.3%); also expected NOT MATCHED BY SOURCE count <= max(1,000, 0.5% rows) unless override. Unique PBKEY and NOT NULL PBKEY asserted on the source snapshot. |
| Source unchanged | `source_version`/vintage already SUCCESS in log -> SKIPPED. |
| Rebuild release (CREATE OR REPLACE, ~every 1-3 months) | **Nothing special needed**: we diff content, not vendor commits - a rebuild is just another snapshot. Mode recorded as REBUILD if history shows CTAS since last release. Large A/U/D after a rebuild trips the thresholds -> requires an explicit `--approve` override, which is the desired human check. |
| Idempotency / re-run | Release is keyed by release_seq. A re-run of an already-SUCCESS release is a no-op. A MERGE re-run after a mid-way failure is naturally idempotent (same result), because `release_seq` already applied is detected via `t.release_seq = :seq` rows; `change_log` insert uses replace-where. |
| Atomicity | MERGE is one Delta commit. change_log + release_log are written after; `release_log.status=SUCCESS` is written last; readers/consumers must only trust releases with SUCCESS (expose via `adf_usa_release_log` and a view `adf_usa_current_release`). A crash between steps is repaired by re-running (derived artefacts are idempotent). |
| Out-of-order | Releases applied only if `release_seq = max(SUCCESS)+1`. Source older than applied (vintage maps to lower seq) => BLOCKED. Because the diff is snapshot-vs-state, applying release N+2 directly is valid. |
| Missed release (N+1 never seen, N+2 arrives) | Allowed and expected (we cannot go back >7 days anyway): the merge folds N+1 and N+2 into one diff labelled N+2; release_log notes `skipped_seq = [N+1]` and the seed table must still contain it. Consumers see net changes, correct for state. Alert, because per-release counts will not reconcile to the doc. |
| Manual DELETE/privacy removal between releases | Folded into next release by decision 3; the diff catches it naturally. |
| First-run baseline | One-time `mode=BASELINE` run reading **`VERSION AS OF 219`** (2026.10.1; must run before ~2026-11-03, earlier than the original 10-29 is safe too). Insert all rows as (BASELINE, 2026.10.1, seq 1), hash computed; log row, row count must equal 194,466,619. Then the normal run reads latest (v227+, 2026.10.2) as seq 2: expected 9,467 ADDED / 3,389 UPDATED / 229 DELETED (217 + the 12 from Thursday). That is the acceptance test of the whole pipeline - **do the baseline load before 11-03 or the 10.2 test is lost**. If it expires, baseline = 2026.10.2 and the first MERGE tests only on release 11.0. |
| Merge-condition cardinality | Source PBKEY unique (asserted); target PBKEY PK (informational constraint + test). |

## 5. Downstream consumption

- Primary interface: **`SELECT * FROM adf_usa_current WHERE release_seq > :last_processed_seq`**. Because status+seq describe the latest change, this returns the *net* change set since the consumer's watermark - correct whether it is 1 release behind or 5. Per-release exact slice: `WHERE release_seq = :n`. Consumers store their own watermark and must only read up to `max(release_seq) WHERE status='SUCCESS'`. Clustering on PBKEY does not help that filter; at 194M rows a `release_seq > n` scan is a ~9 GB read (seconds-minutes); if consumers hammer it, add `release_seq` as a second clustering key or serve from `adf_usa_change_log` joined by PBKEY (slim, small).
- DELETED rows come with PBKEY only; consumers must remove the key (and any derived data) - this is the NJ compliance contract; put it in the table comment and release_log.
- **CDF/row tracking on our table: OFF** (diverges from CLAUDE.md). The release_seq column gives the same feed with no extra machinery, and CDF pre-images are a PII retention channel for deleted rows. Row tracking is harmless but unused; skip until needed. If a future consumer demands streaming, enable CDF with the 7-day retention in section 2 and re-evaluate.
- **Sharing/grants**: UC GRANTs to consuming groups/service principals: SELECT on `adf_usa_current`, `adf_usa_live`, `adf_usa_change_log`, `adf_usa_release_log`; no access to seed/staging. Delta Sharing out only if external consumers exist (share the views/tables without history; it would otherwise expose tombstones/history). Pipeline runs as a dedicated service principal (not a user) that owns the schema.

## 6. Observability, testing, cost, rollout

- **Observability**: every run writes metrics to `release_log`; job failure notifications to the team email/Slack; a SQL alert on (a) no SUCCESS in > 10 days, (b) reconcile_ok = false vs seed doc counts (A/U exact; D tolerance because the vendor's own D count is 2-36 lower than their doc), (c) BLOCKED status. A small AI/BI dashboard is optional - not needed for v1. System tables (`system.billing.usage`) were not readable by my principal, so cost tracking needs a granted role or tag-based reporting (tag the job).
- **Testing**: (1) unit: hash function on crafted rows, incl. NULL position swap, ''/NULL, -0.0/0.0, NaN, lat/lon last-digit change; (2) in a dev schema (separate catalog var), synthetic 1M-row source with injected adds/updates/deletes/resurrections/rebuild-with-reordering; (3) guard tests: empty source, 50% source, duplicate PBKEY, unknown vintage, out-of-order, double run, kill the job between MERGE and `publish` then re-run; (4) real data acceptance: baseline @v219 then 10.2 reconcile (9,467/3,389/229). Optional extra read-only check before building: diff v219 vs v227 with the same hash SQL in a notebook to see how many "updates" are real value changes (vendor's count includes rows whose only change may be a LAT/LON ULP).
- **Cost**: section 3; storage ~11 GB + tombstones + 7-day retention overhead (~2x for one release's rewritten files) = <50 GB total.
- **Rollout**: (1) DAB skeleton + dev target, seed table, schema/permissions; (2) dev run with synthetic data + guard tests; (3) prod baseline@v219 (before 11-03) -> 10.2 MERGE manual run, reconcile; (4) enable the daily schedule in paused state, one week shadow; (5) unpause; first real release 2026.11.0 monitored by hand; (6) add consumer onboarding. Rollback = Delta `RESTORE` of `adf_usa_current` to the pre-run version (recorded in release_log) + mark release FAILED, within the 7-day retention.

## 7. Open questions / risks for the Lead Architect to challenge

1. Is relying on `precisely.data.vintage` safe as the release trigger, given the observed mislabel on the rebuild? I gate on it but fail closed to the seed table; is a pure date-window mapping safer?
2. Net-change feed via `release_seq > n` on a latest-state table vs. an immutable per-release delta table with full values: I chose the former to limit PII retention; does downstream need previous values or deleted-row values (decision 4 is unresolved)? If yes, the PII design changes.
3. Tombstone retention (13 releases) and what a consumer behind that must do - acceptable?
4. Is PBKEY retention on a tombstone acceptable to privacy/legal for NJ removals?
5. Hash design: xxhash64+null bitmap (my pick) vs sha2-256; any reason to take the extra cost?
6. Consumer-side atomicity: release_log SUCCESS as the commit marker vs a single-table design with the release row inside `adf_usa_current` - is a cross-table protocol too fragile for a Java/Python consumer?
7. Missed-release policy (fold and alert) vs hard-fail and require manual approval.
8. Table-update triggers on Delta Sharing recipient tables: unverified. Daily poll is the fallback; is that fine?
9. Does the shared catalog allow us to run `DESCRIBE HISTORY` reliably on the share long term (vendor could change sharing mode and drop properties/history)? Detection must degrade gracefully to commit-timestamp + seed table.
