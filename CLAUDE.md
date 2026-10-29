# ADF USA Incremental Release Pipeline

## CURRENT STATE (read this first; updated 2026-10-29)
- **Built, deployed, running.** Everything below the "HISTORY" marker is the design log and partly superseded; trust this section and `README.md`.
- **What it does**: applies each Precisely `adf_usa` release (weekly X.Y.1.., monthly X.0) to `address_book.bronze.adf_usa_current` so only ADDED/UPDATED/DELETED rows carry the new release. Consumers read `address_book.silver.*` views (grants: `account users`, USE CATALOG + USE SCHEMA/SELECT on silver).
- **Primary path = Precisely's changefile** (`precisely_product_share.precisely_db_property_graph.adf_usa_changefile`, CHANGE A/U/D, decimal(11,6) coords cast to double, hash-identical). **Fallback + audit = hash-diff MERGE** against the main table pinned at the vendor's release commit. Changefile lands ~2 days after the main release; the job waits up to 3 days (`changefile-wait-days`), then MERGEs. A missed release always takes the MERGE. Any validation/audit failure -> `rollback_release` -> MERGE. See `src/adf_merge/changefile.py`, `release.py`, `detect.py`.
- **Run**: DAB bundle `adf-usa-merge-pipeline`, target `prod`, job `adf-usa-release` (tasks `detect` -> `release`, serverless, runs as mayank.kasturia@precisely.com), daily 06:30 UTC, **UNPAUSED** (user chose to enable on 2026-10-29; failures email the user). The job does NOT pull from GitHub: it runs the wheel last deployed with `databricks bundle deploy -t prod --profile DEFAULT`. No CI deploy workflow exists (user said not needed): after merging to main, someone must redeploy by hand.
- **Verified end to end 2026-10-29** from a fresh clone of main: bootstrap from the 2026.10.1 baseline, then the job applied 2026.10.2 via CHANGEFILE, audit PASSED, 9,467 / 3,389 / 229, 194,475,857 live rows. Local tests 25 pass; 26 synthetic scenarios pass on Databricks.
- **Next real test**: the release around 2026-11-05 is the first one that sees a changefile not already applied; it also applies the 2026-10-29 Thursday NJ delete (v230, 71 rows). Check `ops.release_log` (method CHANGEFILE, audit PASSED, else method MERGE + fallback_reason).
- **Repo**: https://github.com/PreciselyCloudNative/adf-usa-merge-pipeline (main has everything; PRs #1-#3 merged). Workflow: branch -> PR -> user merges (merging is blocked for the agent) -> redeploy.
- **Gotchas**: the share only keeps ~7 days of history, so a baseline must be taken from the CURRENT release (`scripts/latest_release.py`; README steps 5-6). Rollback works only inside 7 days. Bundle files live at `/Workspace/Users/mayank.kasturia@precisely.com/.bundle/adf-usa-merge-pipeline/files`. Synthetic tests use throwaway `zz_syn_*` schemas. Leftover table `bronze.adf_usa_baseline_2026_10_1` is unused and can be dropped.
- **Open items**: vendor delete-count gap of 2-36 in older releases is unexplained; optional asks to Precisely (append-only changefile history >= 90 days, release_version/date columns, PBKEY_ALT semantics, changefile landing with the release); `require-confirmed` is false (PROPOSED labels apply automatically, gated by counts/schema/fingerprint).

## HISTORY (design log, 2026-10-26..28; partly superseded)

## Goal
Consume Precisely's Address Fabric USA (`adf_usa`) shared via Delta Sharing and identify, per release, only the
records that were **added / updated / deleted**, so the downstream pipeline never reprocesses unchanged rows.

Phase 1 (this project): **MERGE-based pipeline is the primary flow** (hash diff against our own copy).
Phase 2 (later, when Precisely resumes publishing a proper changelog): redesign so the changelog is primary and
the MERGE becomes the fallback / periodic audit.

## Databricks access
- CLI profile: `DEFAULT` -> https://partner-precisely-presales-us-east-2.cloud.databricks.com (only profile; user chose it explicitly — still pass `--profile DEFAULT`, never auto-select).
- Source tables (identical copies in two schemas):
  - `precisely_product_share.precisely_db_property_graph.adf_usa`
  - `precisely_product_share.addressing.adf_usa` (same pattern as the changelog; assumed same)
- Skills to load for Databricks work: `databricks-core`, then `databricks-jobs`, `databricks-pipelines` / `databricks-dabs`, `databricks-serverless`.

## Source table facts (measured 2026-10-26..28)
- ~194.4M rows, ~9 GB, Delta. 32 columns: PBKEY, PARENT, GOVID, GEOID, FORMATTEDADDRESS, ADDRLINE1-4, ADDNUMBER, ADDNUMBER2,
  STREETNAME, STREETPREDIR, STREET, STREETSUFFIX, STREETPOSTDIR, UNITTYPE, UNIT, LEVEL, BUILDINGNAME, CITY, POSTALCODE,
  POSTALCODEEXT, ADMIN2, ADMIN1, COUNTRY, LOCCODE, LATITUDE, LONGITUDE, PROPTYPE, LANGCODE, FIPS.
- `PBKEY` is unique (distinct count == row count) -> the merge key.
- No vintage / release / date column in the table.
- **Row tracking is NOT enabled** -> `table_changes()` fails (`DELTA_MISSING_ROW_TRACKING_FOR_CDC`). No CDF available from the share.
- **Time travel limited to 7 days** (`delta.deletedFileRetentionDuration` = 168h); VACUUM runs weekly. Older versions are unreadable.
- No release-version label anywhere in the share. Release version must be mapped from Delta commit timestamp -> Precisely's release document.
- `PBKEY_ALT` (consolidation/duplicate replacement ID) exists ONLY in the dead changelog, not in `adf_usa`: a consolidation looks like delete + add.

## Release cadence (from Delta history, validated against Precisely "Address Fabric USA Weekly" doc)
- Releases are **weekly**: `YYYY.MM.0` = large monthly drop, `.1`-`.4` = small weekly increments.
- Applied by the vendor as Delta `MERGE`; inserted + updated counts match the doc EXACTLY for every release checked (2026.07.0 -> 2026.10.1).
  Deleted counts in the MERGE are 2-36 lower than the doc (unexplained; weekly Thursday DELETE statements don't account for it).
- Exception: **2026.09.4 was applied as a full rebuild** (`CREATE OR REPLACE`, 2026-10-07, v190), not a MERGE. Full rebuilds happened on
  2026-04-06, 05-03, 06-04, 07-03, 10-07. So any approach that depends on vendor commit history must tolerate rebuilds.
- Small weekly DELETE (usually <100 rows, once on 09-24 = 1,109) runs every Thursday ~13:15 UTC.

| Release | Doc date | Delta commit (UTC) | Version | Added | Updated | Deleted (doc) |
|---|---|---|---|---|---|---|
| 2026.10.0 | 10-16 | 10-16 15:50 | v202 | 499,410 | 1,735,907 | 246,132 |
| 2026.10.1 | 10-23 | 10-22 06:57 | v204 | 11,738 | 5,200 | 23 |
| 2026.10.2 (probable, UNCONFIRMED vs doc) | ? | 10-27 16:17 | v212 | 9,467 | 3,389 | 217 (delta) |

## Dead end: adf_usa_changelog
Precisely's `adf_usa_changelog` (both schemas) is a monthly full-replace snapshot with NO release column, last rebuilt 2026-03-03, and the
vendor no longer updates it. **Do not use it.** Phase 2 depends on Precisely re-publishing it with the spec below.

## Chosen design (Phase 1): customer-owned copy + hash MERGE
1. **Base table** (our catalog): Delta copy of `adf_usa`, plus `row_hash` (hash of all attribute columns, excluding PBKEY), `first_release`, `last_release`.
   Enable row tracking + CDF on OUR copy (we own it, so no vendor dependency).
2. **Per release run**: `MERGE` source -> base on `PBKEY`:
   - `WHEN NOT MATCHED` -> insert (A)
   - `WHEN MATCHED AND t.row_hash <> s.row_hash` -> update (U)  (unchanged rows are not rewritten, so they never appear in the feed)
   - `WHEN NOT MATCHED BY SOURCE` -> delete (D)
   - Tag the commit (userMetadata / property) with `release_version`.
3. **Downstream input**: `table_changes()` on our base table for that release version (A/U/D with pre/post images). Pipeline processes only these rows.
4. **Release log table**: release_version, release_date, source Delta version/timestamp, added/updated/deleted counts, run status; reconcile vs the vendor doc.
5. Cost: full compare of ~194M rows (~9 GB) per run on serverless compute; weekly.
6. Baseline: first run loads current source state as the baseline (full first pass for the downstream pipeline). **2026.10.0 itself is NOT recoverable** (v202 is outside the 7-day window).

## Phase 2 spec to send Precisely (changelog return)
- `release_version` + `release_date` on every row; append-only, kept >= 90 days (not a monthly replace).
- Complete: every add/update/delete reaching `adf_usa`, including Thursday DELETEs and rebuild releases; new values for A/U; PBKEY for D.
- Include `PBKEY_ALT` for consolidations/duplicates.
- Lands atomically with the `adf_usa` release; per-release counts reconcile with their release document.
- Then: changelog = primary flow; hash MERGE = fallback (run on every X.0 release, any rebuild release, any missed release, or count mismatch).

## Precisely's CDF answer (context)
Precisely can enable CDF but worries about performance and that the table is shared directly. Our view: row IDs are stable (new only for inserts), CDF cost
scales with changed rows (10K-5M vs 194M) so it is small; real risks are `CREATE OR REPLACE` rebuilds breaking the feed, and `WITH HISTORY` sharing exposing
history/deleted records and needing longer retention (~11 GB removed per weekly VACUUM). User judged CDF "too much work" -> pursue the changelog instead.

## Open decisions (NEED USER APPROVAL before building)
1. Baseline = today's state (>= 2026.10.2) instead of exactly 2026.10.0?  (recommended: yes, it is the only option)
2. Release-label mapping: manual lookup table (commit date -> doc version) vs ask Precisely to tag? Proposed: a small seed table we maintain from the Precisely doc.
3. Thursday DELETEs: count as part of the next release (proposed) or track separately?
4. Per-record needs of the downstream pipeline: new values only, or also previous values for updates / deleted-row values?
5. Where does it run: target catalog/schema, serverless job vs Lakeflow pipeline, DAB-based deployment, schedule (weekly, after Thursday?).
6. Hash scope: all 32 columns excluding PBKEY? (LATITUDE/LONGITUDE precision and nullable-column handling must be normalised.)
7. Which source schema is canonical (`precisely_db_property_graph` vs `addressing`)?

## DECISIONS from the user (2026-10-28) — these supersede the "Open decisions" above
1. **Baseline**: use whatever is available; user votes for the **2026.10.1** state as baseline (v204/v206 on 2026-10-22, still inside the 7-day window only until ~2026-10-29),
   then apply the freshly released **2026.10.2** (v212, 10-27) as the first real MERGE to test the pipeline.
2. **Release labels**: seed/mapping table maintained from the Precisely release document (commit date -> release version). YES.
3. **Thursday deletes**: count toward the NEXT release. These are the **NJ lawyers data-removal job** (privacy removals) — treat deletes as legitimate and permanent; downstream must honour them.
4. **Downstream output**: capture everything. Each row carries a status of `BASELINE`, `ADDED`, `UPDATED`, `DELETED` plus the release version, e.g. an added row = (`ADDED`, `2026.11.0`).
   Exact physical layout (current-state status columns vs separate history table, tombstones for deletes) is for the architects to propose.
5. **Where it runs**: catalog/schema must be parameterised variables. Compute/orchestration choice (serverless job vs Lakeflow SDP vs DAB) is **undecided** — decided after the agent feedback loop.
6. **Hash scope**: ALL columns except PBKEY, EXACT values — **no rounding of LATITUDE/LONGITUDE**. Any minor column change qualifies as UPDATED.
7. **Source schema**: `precisely_product_share.addressing.adf_usa`.

## AGREED DESIGN after agent rounds 1-2 (files in `design/`; user answers 2026-10-28) — still NOTHING created in Databricks
- **Catalog**: `address_book` (user-created, MANAGED_CATALOG, owner mayank.kasturia@precisely.com, currently only `default` + `information_schema` schemas).
  User asked for a catalog-level managed location, but `storage_root` currently shows null (= metastore default root) -> verify/set before creating tables (cannot change later).
  Catalog/schema names stay bundle variables. Dev copy = separate catalog later.
- **Schemas (FINAL, user decision 2026-10-28; supersedes the agents' bronze/silver/gold split)**: `bronze` (`adf_usa_current`, slim `adf_usa_change_log`, temporary baseline `adf_usa_baseline_2026_10_1`),
  `silver` (consumer views of the current table and the change feed — the ONLY schema consumers get grants on), `ops` (`release_log`, `release_label` seed). **No `gold` schema for now.**
  Reason: nothing here transforms the data, so `adf_usa_current` is bronze data; the baseline is dropped after acceptance, so bronze must not be left empty.
  Storage: metastore default root (user confirmed; catalog-level managed location not used).
- **Baseline**: `address_book.bronze.adf_usa_baseline_2026_10_1` = CTAS of `precisely_product_share.addressing.adf_usa VERSION AS OF 219` (expect exactly 194,466,619 rows, unique PBKEY).
  Readable only until ~2026-11-03. Kept in bronze; dropped after the 2026.10.2 acceptance test, hard cap 14 days (Lead architect's version). NJ removals must be propagated into it until dropped.
- **No per-release full copies.** One `adf_usa_current` table (~194M rows) updated in place by MERGE; slim change log (keys + hash + change type + release) is the only per-release history. Only the one-off baseline is a full extra copy.
- **Hash**: SHA-256 truncated to 128 bits over a length-prefixed canonical encoding of all columns except PBKEY (NULL != empty string; LATITUDE/LONGITUDE as exact IEEE bits / hex-float, no rounding); store `hash_spec_version`; golden-vector tests + run-start canary; fail on source schema drift.
- **Apply**: single atomic MERGE on PBKEY into `adf_usa_current` (insert / update when hash differs / tombstone on NOT MATCHED BY SOURCE / resurrection flagged `reappeared`). Status per row = BASELINE|ADDED|UPDATED|DELETED + `release_version` + monotonic `release_seq`.
- **Pinning**: pin each release to the vendor's release-commit version (the commit where `precisely.data.vintage` was set), NOT "latest at poll time" — so Thursday NJ deletes (e.g. v220, 12 rows) fall into the NEXT release.
  Trigger = daily poll of the vendor `precisely.data.vintage/count` properties; `release_label` seed table is the authority (auto-create PROPOSED row, human confirms).
  Source mapping: 2026.10.1 = v219, 2026.10.2 = v227; expected first test merge = 9,467 added / 3,389 updated / 229 deleted (217 + 12 Thursday).
- **Tombstones (NJ)**: attributes AND row_hash NULLed; keep PBKEY + status + release fields only; kept indefinitely (NJ requirement is permanent); retention 7 days, no CDF on our tables, purge + VACUUM after any release with deletes. Same removal handling for baseline/test fixtures.
- **Run**: serverless Lakeflow Job via DAB, `performance_target: STANDARD`, 3-4 tasks (detect -> merge -> validate -> publish), SQL files for MERGE/DDL + Python wheel for orchestration/hash/gates; no notebooks in prod; run as a dedicated service principal, group owns catalog.
- **Guards**: count gate vs vendor `precisely.data.count`, row-drop and %UPDATED/%DELETED thresholds, label validation, sequential releases, idempotent re-run, dry-run mode, post-apply checksum.

## User answers to the build-plan questions (2026-10-28)
- Release 2026.10.2 doc numbers are published on **2026-10-30**: seed it as PROPOSED until then; confirm counts vs the doc on/after 10-30 (source v227 stays readable until ~2026-11-04 — keep Phase 4 inside that window).
- **Run identity**: "use the one we used for other jobs". Finding: the user's existing jobs (geomaze_batch_pipeline, tmo_fiber_ingest, tmo_fiber_validation) run as the USER (mayank.kasturia@precisely.com), not a service principal.
  Workspace has service principals incl. `svc-geocode-project` (app id dcce1746-a58a-4cb9-b0bd-fcb4def70d2a). DECISION PENDING: run as user vs a service principal.
- **No dev catalog.** Single env (`address_book`); catalog/schema still bundle variables. Testing uses the baseline + 2026.10.2 on `address_book` itself plus local unit tests.
- **Workflow**: develop locally -> git push -> PR -> main -> deploy to Databricks (DAB). DDL/jobs should come from the repo, not ad-hoc. GitHub org/repo name PENDING.

## Process: agent design loop (final decision is the USER's)
- Agent 1: Databricks Principal Solution Architect (data engineering + warehouse). Agent 2: Lead Technical Architect, 20 yrs prod (Java/Python/pipeline design).
- Round 1: each writes an independent proposal to `design/`. Round 2: each critiques the other's. Round 3: joint final design + plan. User decides. NO building, NO Databricks writes by agents.

## Planned validation before building (read-only, not yet run)
Diff v204 (10-22) vs v212 (10-27) using the hash approach — both still inside the 7-day window — and compare to Precisely's counts (9,467 / 3,389 / 217). Also shows how many "updates" are real value changes.
Note: v204 time-travel window closes ~2026-10-29; run it soon or skip.

## Process agreed with the user
1. Design only until approvals. 2. Review the final design with agents (proposed: one design-review agent for correctness/edge cases, one for Databricks implementation
fit via the databricks skills). 3. Only then build.

## Phase 5 DONE 2026-10-28
- Added `detect.py` (poll vendor `SET TBLPROPERTIES` release commits via DESCRIBE HISTORY, seed PROPOSED labels, apply pending releases in order, SUPERSEDE expired pins), `cli.py` entry points `adf-detect` / `adf-release`, `rollback_release()` in release.py (RESTORE to previous release pointer, clears change log, state ROLLED_BACK), Params `bronze_schema`/`ops_schema`.
- Trigger = scheduled poll (Delta Sharing cannot push). Vendor sets vintage property after its MERGE, so a new vintage = finished release; pin = that commit. History before the newest tracked pin is ignored (older vintages are unpadded/out of order, e.g. 2026.9.0, 2026.6.25).
- DAB: `databricks.yml` + `resources/adf_usa_release.job.yml`, deployed to `/Workspace/Users/mayank.kasturia@precisely.com/.bundle/adf-usa-merge-pipeline`; job `adf-usa-release` (detect -> release), serverless STANDARD, schedule 06:30 UTC **PAUSED**, failure email. `require-confirmed=false` (PROPOSED labels apply automatically, gated by row count / schema / fingerprint).
- Synthetic suite `tests/synthetic/scenarios.py` (run via `scripts/run_synthetic.py` on Databricks, throwaway schemas): 15 scenarios pass (normal, resurrection, hash sensitivity, rebuild, large-change gate, schema drift, bad data, vendor count, validation failure + rollback, idempotent rerun, crash recovery, detect + Thursday delete, missed releases, expired pin, detect anomalies). Local tests: 25 pass.
- Shadow run of the deployed job on the real share: detect seeded nothing, release applied nothing (2026.10.2 already COMPLETE).
- Remaining: shadow-run through the next real release (2026.11.0 / next weekly), then unpause the schedule; Phase 6 (silver views, grants, drop baseline by 2026-11-11, purge/VACUUM); confirm 2026.10.2 label vs Precisely doc on 10-30; push/PR only after Phase 6.

## Phase 6 DONE 2026-10-29
- 2026.10.2 CONFIRMED vs the Precisely weekly doc (9,467 / 3,389 / 229, release date 2026-10-29). 2026.10.1 doc: 11,738 added / 5,200 updated / 23 deleted. Vendor changefile (`precisely_product_share.precisely_db_property_graph.adf_usa_changefile`) matches our MERGE-derived change set key-for-key for 2026.10.2.
- silver views created (sql/ddl/03_silver_views.sql): `adf_usa_current` (live rows, no hash cols), `adf_usa_change_feed` (non-baseline rows incl. tombstones), `adf_usa_change_log`, `adf_usa_releases`. Grants NOT issued: `sql/grants.template.sql` (consumer group still to be named). Views run with owner rights, consumers need silver only.
- maintenance.py: REORG ... APPLY (PURGE) after any release with deletes, VACUUM after any applied release (in the `release` task). Run once manually on 2026-10-29 (229 tombstones).
- Baseline table `bronze.adf_usa_baseline_2026_10_1` DROPPED 2026-10-29 (UC UNDROP window 7 days). Rollback of 2026.10.2 (RESTORE current to v1) only works until ~2026-11-04 (7-day retention).
- Synthetic suite now 17 scenarios (adds maintenance, silver views), all pass.
- Not pushed. User wants push/PR only after Phase 6 -> awaiting go-ahead.
## Changefile (found 2026-10-29)
`adf_usa_changefile`: full-replace per release (only the latest release's changes, v0 CREATE, v1 CTAS, v2 SET TBLPROPERTIES vintage/count=13085). Columns = adf cols + CHANGE (A/U/D), CHG_CODE (NEW/BAD/DUPLICATE/''), PBKEY_ALT (set for DUPLICATE). LATITUDE/LONGITUDE decimal(11,6) (not double). D rows carry full attributes. Plan: changefile primary, MERGE fallback + audit (design pending user approval).

## Changefile primary (branch feat/changefile-primary, 2026-10-29)
- User decisions: grants to `account users` (done: USE CATALOG on address_book, USE SCHEMA + SELECT on silver); push + PR #1 (done, base main = empty initial commit); changefile primary with MERGE fallback + audit; cast decimal(11,6) coordinates to double (verified hash-identical for 12,856/12,856 rows of 2026.10.2).
- `changefile.py`: per release -> changefile at the release vintage? gates (schema, unique keys, A/U/D, row count == its count property) + preconditions vs current (A not live, U/D live, live rows after == vendor_row_count) -> one MERGE by key (D tombstone, A insert/resurrect, U only when hash differs) -> change log -> validate (counts == changefile counts, vendor doc counts, live rows) + audit (content fingerprint vs the pinned main table when the pin is readable, else audit SKIPPED).
- Any failure after apply -> `rollback_release` -> hash-diff MERGE (method MERGE, fallback_reason recorded). Changefile missing/older than the release: DEFER (no error, later releases wait) for up to `changefile_wait_days` (3) after label release_date, then MERGE. The changefile lands ~2 days after the main-table marker.
- release_log gained `method` (BASELINE/MERGE/CHANGEFILE), `fallback_reason`, `audit`; migration in sql/migrations/001 applied live. Job params: use-changefile, audit, changefile-wait-days.
- Synthetic suite: 26 scenarios (9 new c01-c09) all pass. The changefile keeps only the latest release, so a missed release must use MERGE (automatic).
