-- Source-faithful data tables. ${catalog} is substituted at deploy time (bundle variable).
-- Retention is 7 days on purpose: tombstoned (NJ privacy-removed) rows must not stay recoverable via time travel.
-- No change data feed on our tables: pre-images would retain removed addresses.

CREATE TABLE IF NOT EXISTS ${catalog}.bronze.adf_usa_current (
  PBKEY            STRING NOT NULL,
  PARENT           STRING,
  GOVID            STRING,
  GEOID            STRING,
  FORMATTEDADDRESS STRING,
  ADDRLINE1        STRING,
  ADDRLINE2        STRING,
  ADDRLINE3        STRING,
  ADDRLINE4        STRING,
  ADDNUMBER        STRING,
  ADDNUMBER2       STRING,
  STREETNAME       STRING,
  STREETPREDIR     STRING,
  STREET           STRING,
  STREETSUFFIX     STRING,
  STREETPOSTDIR    STRING,
  UNITTYPE         STRING,
  UNIT             STRING,
  LEVEL            STRING,
  BUILDINGNAME     STRING,
  CITY             STRING,
  POSTALCODE       STRING,
  POSTALCODEEXT    STRING,
  ADMIN2           STRING,
  ADMIN1           STRING,
  COUNTRY          STRING,
  LOCCODE          STRING,
  LATITUDE         DOUBLE,
  LONGITUDE        DOUBLE,
  PROPTYPE         STRING,
  LANGCODE         STRING,
  FIPS             STRING,
  row_hash          BINARY  COMMENT 'SHA-256 (first 16 bytes) over all non-key columns, NULL on tombstones',
  hash_spec_version INT     COMMENT 'Version of the canonical encoding used for row_hash',
  status            STRING  NOT NULL COMMENT 'BASELINE, ADDED, UPDATED or DELETED: the LAST change to this row',
  release_version   STRING  NOT NULL COMMENT 'Release that last changed this row, e.g. 2026.10.2',
  release_seq       BIGINT  NOT NULL COMMENT 'Monotonic release sequence of that change (consumer watermark)',
  changed_at        TIMESTAMP,
  reappeared        BOOLEAN COMMENT 'TRUE when a previously DELETED key came back',
  CONSTRAINT adf_usa_current_pk PRIMARY KEY (PBKEY)
)
CLUSTER BY (PBKEY)
COMMENT 'Current state of Address Fabric USA, one row per PBKEY. DELETED rows are tombstones: attributes and row_hash are NULL.'
TBLPROPERTIES (
  'delta.enableDeletionVectors' = 'true',
  'delta.deletedFileRetentionDuration' = 'interval 7 days',
  'delta.logRetentionDuration' = 'interval 7 days',
  'contains_pii_removal_candidates' = 'true'
);

CREATE TABLE IF NOT EXISTS ${catalog}.bronze.adf_usa_change_log (
  release_seq     BIGINT NOT NULL,
  release_version STRING NOT NULL,
  PBKEY           STRING NOT NULL,
  change_type     STRING NOT NULL COMMENT 'ADDED, UPDATED or DELETED',
  row_hash        BINARY COMMENT 'New hash after the change, NULL for DELETED (a last-known hash would fingerprint a removed address)',
  reappeared      BOOLEAN,
  changed_at      TIMESTAMP
)
CLUSTER BY (release_seq)
COMMENT 'Append-only slim change feed: keys, hash and change type per release. No address attributes.'
TBLPROPERTIES (
  'delta.enableDeletionVectors' = 'true',
  'delta.deletedFileRetentionDuration' = 'interval 7 days',
  'delta.logRetentionDuration' = 'interval 7 days'
);
