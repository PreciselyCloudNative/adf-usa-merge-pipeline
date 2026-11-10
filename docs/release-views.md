# Release views (gold)

Back to the [README](../README.md).

`adf_usa_current` holds each record only at its last change, so on its own it cannot show an older release. Before a
release is applied, the live versions it is about to update or delete are copied to `bronze.adf_usa_history` (changed
records only, never a full copy). Release N is then a view:

```sql
SELECT ... FROM silver.adf_usa_current WHERE release_seq <= N                       -- unchanged since N
UNION ALL
SELECT ... FROM silver.adf_usa_history WHERE release_seq <= N AND valid_to_seq > N  -- changed or deleted later
```

After every run, `adf-release` (`adf_merge.gold.refresh`):

* creates the view of each new release, `gold.adf_usa_<release>` (dots become underscores);
* points `gold.adf_usa_latest` at the newest release;
* tags the newest view `release_status = latest` and older ones `archived`;
* drops the view of a rolled-back release.

## Guarantees and limits

* **Exact:** the synthetic suite proves every release view equals the vendor table at that release's pin. Each release's
  archive is also checked at run time (rows archived = updated + deleted).
* **History starts at the baseline:** earlier releases were never captured, so the first gold view is the baseline release.
* **Privacy:** the history keeps the old attributes of records a release removed, including NJ privacy removals.
  Sign-off to retain them was obtained. `adf_usa_current` itself holds only tombstones. Grant `silver` and `gold` with this in mind.
