# Round 2 - Lead Technical Architect: critique and catalog management

Read-only checks run: `geomazerunner` (MANAGED, owner Mayank, predictive optimization inherited ENABLE, metastore-level storage, no own storage root) already has schemas `bronze`, `silver`, `ops`, `default`, holding GeoMazeRunner customer data (charter, fedex, john_deere...) and `ops.run_audit`. Catalog grants: `account users` BROWSE, and `tanha.talavia@precisely.com` ALL_PRIVILEGES. CREATE CATALOG on the metastore sits with a workspace-admin group; Mayank is in `admins` and created `geomazerunner` himself, so he can probably create one (not verified).

## Part 1 - Critique of the Databricks proposal

**(a) Single atomic MERGE + release_seq watermark vs my staged diff + change_log: I concede most of it.** Their design is simpler and gives the same consumer feed without a table carrying attributes. I drop: my attribute-bearing change_log, the separate "publish" and "apply" commits, and the staged diff. I keep four things, and ask them to adopt them:
1. **Pre-MERGE gate.** Their guards (e.g. expected NOT MATCHED BY SOURCE count) need the diff before the MERGE. Compute A/U/D counts with a hash-only anti-join (about 1 min), gate, then MERGE. Nothing mutates `current` until gates pass. RESTORE stays as the rollback of last resort, not the primary guard.
2. **Counts for `release_log` come from `current WHERE release_seq = X` (and the MERGE metrics only as a cross-check).** If the job dies after the MERGE and is re-run, the MERGE is a no-op and would report zero changes.
3. **Consumer consistency:** record `current_table_version` in `release_log` at COMPLETE. Consumers who need a snapshot read `adf_usa_current VERSION AS OF v`. A filtered view on `release_seq <= max(SUCCESS)` would be wrong: it hides updated rows entirely instead of showing their old values.
4. Slim `change_log` is fine and rebuildable; it holds PBKEY and keys only. Keep it.

**Pin defect to fix (important).** Their `detect` records "source_version" without saying which one. If the pin is the latest version at poll time, a Thursday delete that lands after a release but before the 18:00 poll is folded into that release. This exactly happened on 10-22 (release v219 at 06:57, 12-row delete v220 at about 13:15). Decision 3 (Thursday deletes count toward the next release) requires: **pin = the vendor's release-commit version** (the last MERGE/REPLACE commit before the `vintage` property-set commit), stored in the seed/release_log. Same for the cross-check: `precisely.data.count` must be read as of the property-set commit, not from latest, since `count` also moves after Thursday deletes.

**(b) Hash: I concede xxhash64 + NULL bitmap, with three conditions.** Per-key false-equal at 2^-64 is negligible. Conditions:
- Spark's `xxhash64` normalises -0.0 to 0.0 and NaN, so an exact-bits change would be invisible, contradicting "exact values". Add LATITUDE/LONGITUDE as hex-float strings (`format_string('%a', x)`) as additional arguments; the cost is trivial.
- A persisted hash is only valid while the runtime computes it identically, and xxhash64's output is not a contractual guarantee. Mitigation: `hash_spec_version`, a golden-vector unit test in CI, and a run-start canary (hash 20 constant rows, compare to stored expected values, fail closed). `current` holds full rows, so a controlled rehash is possible.
- Tombstones: see (c).

**(c) Tombstones:** agree with attributes NULLed and no CDF. Disagreements:
- **Set `row_hash` to NULL on tombstones.** A last-known hash is a fingerprint of the removed address (confirmable by anyone who guesses it). Resurrection does not need it (status DELETED always becomes ADDED).
- **Do not purge tombstones after 13 releases.** The requirement is "honoured permanently". PBKEY-only tombstones cost almost nothing (about 250K deletes per month, tens of MB). Keep them indefinitely, or at minimum 2 years and then move to a hashed suppression table. A consumer behind the horizon must otherwise do a full resync, and a reappearance of a purged key would no longer be flagged.
- Overwriting the tombstone on resurrection loses the "reappeared" signal. Add `reappeared_count` to `release_log` (and a `reappeared` flag column) and alert.
- Retention settings are fine but weekly VACUUM is not enough alone: deletion vectors OFF or `REORG ... APPLY (PURGE)` after any release with D rows, and do not rely on predictive optimization for the timing. Assert physical removal with a test.

**(d) Trigger/label: agree** with vendor property as trigger and cross-check, seed table authoritative, daily poll, fail closed on unknown vintage, 30-minute settle. Add: mislabels (`2026.9.3` then `2026.9`) are expected, so the property-set commit with the final value after the settle window is the one used; a pure date-window mapping is the fallback if the property disappears.

**(e) Language: a split, not either/or.** MERGE and DDL in `.sql` files (agree with them), but orchestration, hash-expression generation from the contract, gates and the state machine are Python in a wheel (`python_wheel_task`), unit-testable offline. Notebook tasks in prod are untestable and diffable only with pain; avoid them. Not SQL-only, because gating logic and the hash contract would otherwise live in strings.

**(f) Is multi-commit worth it?** No, not as I proposed it. After the concessions above the remaining multi-table protocol is: MERGE (atomic) then `change_log` (idempotent replace-where) then `release_log` COMPLETE. That is the minimum honest state machine. Their runtime estimate (5-12 min small, 15-25 min X.0) is plausible: the vendor's 93 s includes no hash computation, but xxhash64 over 31 string columns is cheap. Treat it as an estimate; the 10.1 -> 10.2 run gives the real figure. I also accept "do not enable CDF" and the daily poll, and I accept their rollout order.

**Still open between us:** (i) pin semantics above; (ii) tombstone retention and hash NULLing; (iii) whether the pre-MERGE count gate is acceptable cost; (iv) whether `SDP AUTO CDC FROM SNAPSHOT` stays rejected (both of us reject it).

## Part 2 - Catalog management (decide now)

**Decision: a new dedicated catalog, not `geomazerunner`.**
- `geomazerunner` is project-named for another product, contains unrelated customer data, and already has a broad grant (`tanha.talavia` ALL_PRIVILEGES) that would inherit to every new schema and table, including data subject to NJ removals. Grants, ownership, predictive-optimization config and storage all sit at catalog level, so a dedicated catalog gives a clean blast radius and a one-statement `DROP CATALOG ... CASCADE` for dev.
- Name: `${catalog}`, default prod `address_fabric` (neutral and product-named, not tied to this pipeline), dev `address_fabric_dev`. If CREATE CATALOG turns out not to be available to Mayank, request it from the admin group (a ticket, not a blocker for design). **Fallback only if refused:** use `geomazerunner` with new schemas `adf_*` and an explicit `REVOKE` review; do not reuse its existing `bronze/silver/ops`.
- Dev vs prod: one catalog per environment, selected by DAB target variables. Prod holds the full 194M rows; dev holds a ~1M-key sample plus synthetic mutations, never a full copy.

**Schemas and objects** (schema names also variables, defaults shown; table names keep the `adf_usa_` prefix so they stay unambiguous if a catalog is ever shared):

| Schema | Object | Notes |
|---|---|---|
| `bronze` | `adf_usa_baseline_2026_10_1` | One-off CTAS of source `VERSION AS OF 219`, immutable, 194,466,619 rows, no hash. Property `delete_after` set. |
| `silver` | `adf_usa_current` | State table: 31 attributes, `row_hash`, status, release_version, release_seq, changed_at. Liquid-clustered on PBKEY, no CDF. |
| `silver` | `adf_usa_change_log` | Slim, keys only, append-only, no attributes. |
| `ops` | `adf_usa_release_log`, `adf_usa_release_label` (seed), `adf_usa_run_metrics`, `_dryrun` scratch | Seed CSV lives in the repo, MERGEd on deploy. |
| `gold` | `adf_usa_live` (view, non-tombstone), `adf_usa_changes` (view over `current` joined to `release_log` SUCCESS) | Only schema consumers get grants on. |

No persisted per-release staged copy of the source: stages read the share directly at the pinned version and keep hash-only data in memory/temp. Rejected: weekly full bronze snapshots (194M rows x weekly; retains removed NJ rows forever; no benefit since the source is readable at any pin within the window).

**NJ retention per layer.**
- `bronze` baseline is the awkward one: it permanently contains addresses that later NJ removals delete. It must not be a permanent archive. Keep it only until the 10.2 acceptance test passes, then drop it and VACUUM (suggest a 14-day cap from creation). If anyone needs it longer, the job must purge later-deleted keys from it each release; I recommend not doing that.
- `silver.adf_usa_current`: tombstones with NULL attributes and NULL hash, 7-day file retention, purge after delete releases.
- `change_log`, `release_log`, seed: PBKEY-level only, retained indefinitely.
- Dev fixtures: sample keys may later be deleted in prod, so give dev data a 30-day TTL and rebuild it, never copy prod rows into long-lived tests (use synthetic for regression cases).

**Ownership and grants.**
- Catalog owner: a group (create `adf-owners`, Mayank plus one deputy), not a single user, so the data is not orphaned. If groups cannot be created, Mayank owns it now and transfers later.
- Pipeline runs as a dedicated service principal (existing SPs belong to other products; do not reuse), granted USE CATALOG, ALL on `bronze/silver/ops`, nothing on `gold` beyond view ownership. The SP needs SELECT on `precisely_product_share.addressing.adf_usa`.
- Consumers: USE CATALOG + USE SCHEMA + SELECT on `gold` only. Nobody except the SP gets MODIFY. Not shared via Delta Sharing out until a use is defined (sharing would expose tombstones and history).
- Predictive optimization ENABLE at the catalog; the job still runs explicit OPTIMIZE/PURGE/VACUUM when a release has deletes.

**Needed now vs can wait.**
- **Now (margin: VERSION AS OF 219 readable to about 2026-11-03):** (1) confirm CREATE CATALOG, or file the ticket today; (2) create the catalog plus `bronze` (and `ops` empty); (3) run the one-off CTAS of v219 into `bronze.adf_usa_baseline_2026_10_1`, with a row-count assertion of 194,466,619 and a table comment stating the delete_after date and why. Nothing else is needed to protect the test. Aim to finish with at least 3 days of margin.
- **Soon (before the first MERGE test):** `silver` and `ops` objects, seed rows for 2026.10.1/10.2, SP and grants, golden-vector tests.
- **Can wait:** `gold` views, consumer grants, dev catalog, dashboards, alerting, Phase-2 hooks, tombstone-archive policy.

**Questions for the Databricks architect:** does `address_fabric` need its own managed storage location (separate S3 prefix) for isolation and clean deletion, or is the metastore root acceptable? Is a service principal creatable in this workspace without account-admin help?
