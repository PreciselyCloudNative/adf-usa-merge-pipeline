-- Release views. ${catalog} is substituted at deploy time. The per-release views (gold.adf_usa_<release>) and
-- gold.adf_usa_latest are created by adf_merge.gold after every release, so only the schema and the index are static here.
-- Views run with the owner's rights, so consumers need USE CATALOG + USE SCHEMA/SELECT on gold only.

CREATE SCHEMA IF NOT EXISTS ${catalog}.gold COMMENT 'Address Fabric USA as of every applied release: views only, no data copies.';

CREATE OR REPLACE VIEW ${catalog}.gold.adf_usa_versions
COMMENT 'One row per available release view. view_name is the gold view holding Address Fabric USA as of that release.'
AS SELECT release_version, release_seq, release_date, row_count, added, updated, deleted,
  concat('adf_usa_', replace(release_version, '.', '_')) AS view_name,
  release_seq = max(release_seq) OVER () AS is_latest
FROM ${catalog}.silver.adf_usa_releases;
