# Round 1 - Lead Technical Architect proposal (pipeline engineering and correctness)

Verified read-only on 2026-10-28 (`addressing.adf_usa`): 194,475,857 rows; PBKEY never NULL; all 31 non-key columns are STRING except LATITUDE/LONGITUDE (DOUBLE); ADDRLINE2 is 100% NULL; UNIT is 72% NULL with zero empty strings (so NULL vs '' is cleanly separable today).

**Time-critical:** v204 (2026.10.1 baseline) leaves the 7-day window at about 2026-10-29 06:57 UTC. Whatever we decide, the baseline copy must be materialised before then.

## Headline recommendation
Do NOT make "MERGE + CDF on our table" the change feed. Instead: **pin a source version, compute an explicit state diff (hash full-outer-join), persist it as an append-only `change_log` keyed by release, then apply that change set to a `current` table.** The change_log is the source of truth for a release; `current` is derivable from it. Reasons: (a) labels are first-class data, not commit metadata; (b) the diff is gated before anything is applied; (c) we control purging of deleted data (CDF files and table history would retain removed addresses); (d) vendor `CREATE OR REPLACE` rebuilds are irrelevant because we diff state, not history.

## 1. Stages and contracts
Tables (catalog/schema parameterised): `release_map` (seed), `release_log` (state machine), `adf_usa_current`, `adf_usa_change_log`, view `adf_usa_live` (current minus tombstones).

| # | Stage | Input -> Output | Contract |
|---|---|---|---|
| 1 | Detect | `DESCRIBE HISTORY` of source + `release_map` -> next release to process | Daily poll. Newest source commit with no `release_map` row -> alert "unmapped commit" (human adds the row). Process only the next unprocessed `release_map` row, strictly in sequence. |
| 2 | Validate source | source @ pinned version -> pass/fail | Schema == contract; PBKEY non-null and unique; label checks (section 4). Nothing written except `release_log` status. |
| 3 | Stage/hash | source @ pin -> `(PBKEY, row_hash)`; join to `current` -> classified diff | Output: `stage_diff_<release>` (A/U with full row; D with PBKEY only). Re-runnable (overwrite by name). |
| 4 | Gate | diff counts vs thresholds and vendor-doc counts | Hard stop unless an operator approval flag is set on the release_map row. Dry-run stops here. |
| 5 | Publish change_log | stage_diff -> `change_log` | `replaceWhere release_version = X` (idempotent). |
| 6 | Apply | change_log(X) -> `current` | One Delta MERGE keyed on PBKEY from the change set (about 10K-2M rows, not 194M). Guarded by `current.last_release` property so it cannot apply twice. |
| 7 | Reconcile and close | `current` vs source@pin | count(live) == count(source); aggregate checksum `sum(xxhash64(PBKEY,row_hash))` equal on both sides. Only then `release_log.status = COMPLETE`. |

**Transaction boundaries:** Delta only gives single-table atomicity, so there are three commits (5, 6, 7) and we do not pretend otherwise. State is `release_log.status` in {STARTED, DIFFED, PUBLISHED, APPLIED, COMPLETE, FAILED} plus the pinned `source_version`. Each stage reads the status, skips if already past it, and is safe to re-run. Downstream may read a release only when status = COMPLETE (consumers join `release_log`, or read a view that filters on it). A crash between 5 and 6 leaves change_log(X) present but invisible; the retry resumes at 6.

**Idempotency key = release_version.** Re-running a COMPLETE release is a no-op. Re-running a failed one resumes. A different diff for the same release (source changed under the pin) cannot happen because the pin is a Delta version.

Rejected: (i) single MERGE with `WHEN NOT MATCHED BY SOURCE` doing everything (no gate between detection and mutation; no change_log); (ii) MERGE+CDF as feed (above); (iii) Lakeflow `AUTO CDC FROM SNAPSHOT` for steps 3+6 (does the diff for us, but hides the hash, offers no pre-apply gate, no pinned-version/label control, and weak control over purging history). Worth the other architect challenging, see section 7.

## 2. Change semantics
Definitions, always relative to our last COMPLETE release (state diff, never commit-by-commit):
- **BASELINE**: row present in the first load; `change_type` BASELINE, label = baseline release (2026.10.1).
- **ADDED**: PBKEY in source@pin, absent from `current` or present as tombstone.
- **UPDATED**: in both, live, `row_hash` differs.
- **DELETED**: live in `current`, absent from source@pin.
- Unchanged: no row written anywhere.

`current` carries `row_status` and `release_version` = the **last change** (a row ADDED in 2026.11.0 stays (ADDED, 2026.11.0) until it changes again). Consumers needing "what changed in release X" read `change_log WHERE release_version = X`; `current` answers "what is the state". The change_log also carries `prev_status`, `prev_hash`, `reappeared` and `release_date`.

Edge cases:
- **Updated twice before consumption / multiple releases between runs:** state diff collapses automatically. Net effect wins: add+update = ADDED (final values); add+delete = nothing; update+revert-to-same = nothing; delete+re-add same values inside the gap = nothing (and cannot be seen; accepted).
- **Missed release:** policy is strictly sequential. If the pins for the skipped releases are still readable, process them in order. If not, run one collapsed release labelled with the **latest** release and record `covers_releases` (the skipped labels, from `release_map`) in `release_log` and as an array column in change_log. Rows then say (ADDED, latest) which is honest about our observation, not about when the vendor added them.
- **Deleted PBKEY returns:** tombstone exists -> emit ADDED with `reappeared = true`, plus `tombstone_release`; alert (never auto-ignore). We cannot tell NJ-removed keys from ordinary deletes, so every reappearance is surfaced for a human; the rule that wins is "source is truth, but loudly".
- **Consolidation:** looks like DELETED(old) + ADDED(new) with no link. v1 stores a nullable `linked_pbkey` (NULL always from hash diff). An optional heuristic (same GOVID/GEOID) may populate a separate `link_hint` column, never `linked_pbkey`.
- **Thursday deletes count toward the next release:** achieved structurally, not by convention. We pin to the **release commit version** (stored in `release_map.source_version`), so deletes landed after it are invisible to release X and appear in X+1's diff. The 2026.10.1/10.2 test pair shows this: baseline = v204, and v206 (Thursday delete) falls into 2026.10.2.
- **NJ privacy removals, permanent:** requires a purge design.
  - Tombstone keeps only PBKEY, status DELETED, release_version, row_hash NULL, all attributes NULL. Legal should confirm PBKEY alone is not itself personal data; if it is, tombstones move to a hashed-key suppression table.
  - No previous values stored for D (the answer to "deleted-row values" is: do not keep them).
  - Apply step also scrubs attribute columns of all earlier change_log rows for deleted PBKEYs.
  - Disable CDF on our tables; set `deletedFileRetentionDuration` to 7 days; after each release that has D rows, run `REORG TABLE ... APPLY (PURGE)` (deletion vectors keep bytes otherwise) then `VACUUM`. Target SLA: physical removal within 8 days; assert it with a test query on history.
  - Dry-run/stage tables are dropped on COMPLETE; no clones or backups of `current` outside this policy.
  - Downstream: DELETED events must be retained in change_log at least as long as the slowest consumer lags and consumed events must be acknowledged (`consumer_ack` table or watermark) so a missed delete is detectable. Downstream copies are the customer's own compliance problem, but our contract says "DELETED means purge".

## 3. Hashing
- **Algorithm:** SHA-256 over a canonical byte string, truncated to 128 bits, stored `BINARY(16)` (about 3 GB). Rejected: `hash()` (32-bit Murmur, unacceptable), `xxhash64` as the row hash (64-bit is statistically fine per-key, but SHA costs little here and removes the argument), MD5 (policy noise).
- **Collision risk:** we compare old vs new hash per PBKEY, not across rows. False-equal probability per changed row is 2^-128; even with 64 bits it would be about 10^-13 per release. Whole-table birthday math is irrelevant.
- **Canonical serialization** (explicit ordered column list from config, never `SELECT *`, never `concat_ws`): for each column in contract order, NULL -> `0x00`; non-NULL -> `0x01 || utf8 byte length (decimal) || ':' || utf8 bytes`. Length-prefixing removes delimiter ambiguity. NULL and '' hash differently (kept distinct per "exact"; counter `null_empty_flips` reported as a DQ metric). No trim, no case folding, no Unicode normalisation.
- **LATITUDE/LONGITUDE:** exact IEEE-754 bits. Serialise with the hex-float form (`format_string('%a', x)`), not decimal casting (decimal text is runtime-dependent and may round). -0.0 and NaN are distinct values by design. A golden-vector unit test pins the encoding.
- **hash_version:** stored as a table property and column; changing the contract bumps it. Because `current` holds full rows, a `rehash` mode can recompute stored hashes without emitting events.
- **Schema drift:** stage 2 compares source schema to the versioned contract (names, order-insensitive, types). Any added/removed/retyped column FAILS the release; no auto-adapt. An operator runs `schema-migrate` (alter `current`, bump contract, rehash, emit no row events unless chosen) because an added populated column would otherwise flag 194M rows UPDATED.

## 4. Safety guards
Hard-stop gates (thresholds in config, override only by approval flag on the release_map row):
- Source row count vs previous: |delta| > 1% stop. Observed monthly X.0 drop: +0.26% net.
- UPDATED > 5% (weekly) or > 10% (X.0) stop. Observed X.0: about 0.9%.
- DELETED > 1% stop; absolute D > 10,000 on a weekly release warns. This also covers the empty/partial source (`NOT MATCHED BY SOURCE` becoming a mass delete).
- Vendor-count reconciliation from `release_map.expected_*`: ADDED and UPDATED must equal exactly (they matched in every release checked); DELETED within a tolerance (history shows doc is 2-36 higher), outside it warn, grossly outside stop.
- Label validation: pinned commit timestamp must fall in the interval of that `release_map` row; label strictly greater than last COMPLETE; no gaps unless `covers_releases` is explicit; source_version must still be readable.
- **Rebuild vs merge:** the vendor's method does not matter to us. Our own "rebuild" is a separate, explicit `rebaseline` mode (all rows BASELINE, no events, own label), never an automatic fallback.
- **Dry-run:** runs stages 1-4, writes only to a `_dryrun` stage table and a report; mandatory for the first run of each release when `approval_required = true`.

Ways to silently corrupt, and detection:
1. Hash contract bug (column order/NULL handling) -> mass false UPDATED or missed updates. Golden-vector tests; update-% gate; sampled audit (compare raw columns of 1M hash-equal rows, expect 0 differences).
2. Wrong release label/pin. Interval validation plus expected-count reconcile; the 2026.10.2 test is the proof.
3. Double apply/retry. `last_release` guard plus stage 7 checksum.
4. Out-of-band writes to `current`. Stage 7 checksum compares against source every run; restrict write grants to the job principal.
5. Duplicate/NULL PBKEY in source -> ambiguous merge or dropped rows. Validate uniqueness in stage 2.
6. Source read at "latest" instead of the pin (Thursday deletes leak into the wrong release). Pin is a mandatory parameter; test asserts it.
7. Time-travel/VACUUM erasing the pin mid-run. Check readability up front; on failure of a retry, abort rather than fall back to latest.
8. Privacy residue in history/log files. Scheduled purge assertion test.

## 5. Code, tests, operations
- **Language:** Python 3.11 with the PySpark DataFrame API plus plain SQL files for DDL and the apply MERGE. Java/Scala not needed (no UDFs: the hash is built from built-in functions). Notebooks only for ad-hoc investigation, never in the job.
- **Layout:**
  ```
  databricks.yml              # DAB; variables: catalog, schema, source_table, thresholds
  src/adf_merge/{config,contract,hashing,diff,gates,apply,reconcile,release_log,cli}.py
  sql/{ddl,apply_merge}.sql
  conf/{contract_v1.yml,thresholds.yml}
  tests/{unit,integration,dq}
  runbook.md
  ```
  Packaged as a wheel; job = one task per stage (so retry granularity matches the state machine). Compute (serverless job vs pipeline) stays open because the stages are plain idempotent functions.
- **Tests:** unit on local Spark: golden hash vectors, canonicalisation property tests (injectivity on NULL/''/delimiters/unicode), a scenario matrix for every case in section 2 (twice-updated, add+delete, reappear, missed release, Thursday delete pin). Integration in a dev schema: baseline v204 -> apply v212 and require ADDED 9,467 / UPDATED 3,389 / DELETED 217 (the A and U must be exact; D is the known-fuzzy one). DQ: uniqueness, non-null key, live count == source, no tombstone with attributes.
- **Test data:** because the share keeps only 7 days, freeze fixtures immediately: the baseline copy (v204), and a v212 snapshot or (cheaper) just the three diff key sets plus a 100K-key sample of both versions into a `fixtures` schema. Add a synthetic generator for mutation cases.
- **CI/CD:** GitHub Actions: ruff + pytest + `bundle validate` on PR; deploy to dev on merge; manual promote to prod with a tag; integration test on demand only (it costs a full scan).
- **Observability:** `release_log` holds counts, durations, gate results, source_version, code version, hash_version. Alerts (Slack/email): job failure, any gate stop, unmapped source commit, reappeared tombstone, "release in release_map not COMPLETE after N days", purge SLA breach. Metrics are table rows; a small dashboard can read them.
- **Runbook:** one page per scenario: gate stop (approve or reject), failed stage (re-run, resume is automatic), source schema change, missed release, rebaseline, privacy purge verification, restoring `current` from change_log.

## 6. Phase-2 compatibility (cheap if we do these now)
- `release_version` string plus sortable `release_seq` int on every table; `release_date` too.
- change_log schema = the spec we are sending Precisely (release_version, release_date, PBKEY, change_type, PBKEY_ALT/`linked_pbkey`, attributes) plus a `source_kind` column ('HASH_DIFF' | 'VENDOR_CHANGELOG').
- Stage 3 behind a `ChangeSource` interface; gates, publish, apply and reconcile are source-agnostic. Phase 2 swaps the implementation and keeps the diff as an audit task that compares key sets per release against the vendor changelog.
- Downstream only reads `change_log`/`adf_usa_live` and `release_log`, never diff internals.
- Pinning by `release_map.source_version` already matches how a vendor-published changelog would be reconciled to a release.

## 7. Open questions and risks for the Databricks architect
1. Is `AUTO CDC FROM SNAPSHOT` good enough, given pre-apply gating, pinned-version reads and purge control? What would it look like with a gate task before it?
2. Cost of the diff: I assume a hash-only shuffle (about 7 GB) then re-fetch full rows for A/U. Is that right on serverless, and are liquid clustering on PBKEY plus deletion vectors + Photon the right tuning? Expected run time?
3. Does `REORG ... APPLY (PURGE)` + `VACUUM` truly remove removed rows from managed storage, including the history retained for pinned snapshots? Any Unity Catalog audit-log implication?
4. Can a Delta Sharing recipient reliably read `VERSION AS OF` the release commit, and does the version number stay stable across vendor `CREATE OR REPLACE`/restore? (I treat version numbers as pins; a rebuild-then-restore could break that.)
5. Does the vendor ever restate a published release (re-commit under the same label)? That would break "strictly increasing label" and my idempotency key.
6. Are NJ-removed PBKEYs themselves personal data (affects tombstone design)? Legal question for Mayank.
7. Is `current` at 194M rows worth keeping versus only change_log + a daily source snapshot, given the source is available to us at any time? I keep it because it is the diff base and it survives the 7-day window.
8. Should the first production gate be mandatory-approval for every release for the first 3 months?
