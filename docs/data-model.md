# Data model

Back to the [README](../README.md). Catalog `address_book`.

| Table / view | Purpose |
|---|---|
| `bronze.adf_usa_current` | Current state, one row per PBKEY, with `row_hash`, `status` (BASELINE/ADDED/UPDATED/DELETED), `release_version`, `release_seq`, `reappeared` |
| `bronze.adf_usa_change_log` | Slim per-release history (key, change type, hashes) |
| `bronze.adf_usa_history` | Superseded versions: every live version that a later release updated or deleted, valid from `release_seq` (inclusive) to `valid_to_seq` (exclusive) |
| `ops.release_label` | Release labels: source pin, vendor counts from the release document, PROPOSED/CONFIRMED |
| `ops.release_log` | One row per release run: state, counts, `method`, `fallback_reason`, `audit`, rollback pointer |
| `silver.adf_usa_current` | Consumer view: live rows only, no hash columns |
| `silver.adf_usa_change_feed` | Consumer view: rows changed after the baseline, including tombstones |
| `silver.adf_usa_change_log`, `silver.adf_usa_releases` | Consumer views of the change history and the release list |
| `silver.adf_usa_history` | Consumer view of `bronze.adf_usa_history` |
| `gold.adf_usa_<release>` | The table as of that release. A view, no data copy ([how](release-views.md)) |
| `gold.adf_usa_latest` | Alias of the newest release (same rows as `silver.adf_usa_current`) |
| `gold.adf_usa_versions` | Index of the release views: `release_version`, `view_name`, counts, `is_latest` |

## Apply paths

* **Changefile (primary):** `precisely_product_share.precisely_db_property_graph.adf_usa_changefile`, rows with `CHANGE` = A/U/D.
  Precisely publishes it the same day as the main table's release marker. It holds only the latest release.
  LATITUDE/LONGITUDE arrive as `decimal(11,6)` or `double` and are cast to double before hashing (hash-identical to the main table).
* **Hash MERGE (fallback and audit):** against the main table pinned at the vendor's release commit. After a changefile
  apply, a content fingerprint is compared to the pinned table (audit PASSED, or SKIPPED if the 7-day pin has expired).
  If any gate, validation or audit fails, the release is rolled back and the MERGE runs. The reason is stored in
  `ops.release_log.fallback_reason`.
* A release missed for any reason is always applied by the MERGE (the changefile only holds the latest one).
* Deletes are tombstones: attributes and `row_hash` are NULLed, PBKEY, status and release stay.
