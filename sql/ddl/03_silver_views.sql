-- Consumer surface. ${catalog} is substituted at deploy time. Views run with the owner's rights, so consumers
-- need USE CATALOG + USE SCHEMA/SELECT on silver only, never on bronze or ops.

CREATE SCHEMA IF NOT EXISTS ${catalog}.silver COMMENT 'Consumer views of Address Fabric USA. The only schema consumers are granted.';

CREATE OR REPLACE VIEW ${catalog}.silver.adf_usa_current
COMMENT 'Address Fabric USA, live records only (deleted records excluded). status/release_version tell when each record last changed.'
AS SELECT PBKEY, PARENT, GOVID, GEOID, FORMATTEDADDRESS, ADDRLINE1, ADDRLINE2, ADDRLINE3, ADDRLINE4, ADDNUMBER, ADDNUMBER2,
  STREETNAME, STREETPREDIR, STREET, STREETSUFFIX, STREETPOSTDIR, UNITTYPE, UNIT, LEVEL, BUILDINGNAME, CITY, POSTALCODE,
  POSTALCODEEXT, ADMIN2, ADMIN1, COUNTRY, LOCCODE, LATITUDE, LONGITUDE, PROPTYPE, LANGCODE, FIPS,
  status, release_version, release_seq, changed_at, reappeared
FROM ${catalog}.bronze.adf_usa_current WHERE status <> 'DELETED';

CREATE OR REPLACE VIEW ${catalog}.silver.adf_usa_change_feed
COMMENT 'Records whose last change is after the baseline: ADDED, UPDATED, DELETED (DELETED rows carry only PBKEY and release fields). Filter release_seq > your last processed release_seq.'
AS SELECT * EXCEPT (hash_spec_version) FROM ${catalog}.bronze.adf_usa_current WHERE status <> 'BASELINE';

CREATE OR REPLACE VIEW ${catalog}.silver.adf_usa_change_log
COMMENT 'Slim append-only history of every change per release: keys, new hash and change type. No address attributes.'
AS SELECT release_seq, release_version, PBKEY, change_type, row_hash, reappeared, changed_at
FROM ${catalog}.bronze.adf_usa_change_log;

CREATE OR REPLACE VIEW ${catalog}.silver.adf_usa_releases
COMMENT 'Applied releases (vintages) with their change counts.'
AS SELECT l.release_seq, l.release_version, b.release_date, coalesce(l.added, b.vendor_added) AS added, coalesce(l.updated, b.vendor_updated) AS updated, coalesce(l.deleted, b.vendor_deleted) AS deleted, l.source_row_count AS row_count, l.finished_at
FROM ${catalog}.ops.release_log l LEFT JOIN ${catalog}.ops.release_label b USING (release_version)
WHERE l.state = 'COMPLETE';
