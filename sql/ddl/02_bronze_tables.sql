-- Source-faithful data tables. ${catalog} is substituted at deploy time (bundle variable).
-- adf_usa_current keeps a 7-day Delta retention and no change data feed: its tombstones carry no attributes.
-- Superseded versions of changed or deleted records are kept on purpose in adf_usa_history (privacy sign-off
-- obtained), which is what makes every past release reproducible (see the gold schema).

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

CREATE TABLE IF NOT EXISTS ${catalog}.bronze.adf_usa_history (
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
  row_hash          BINARY,
  hash_spec_version INT,
  status            STRING  NOT NULL COMMENT 'Status the row had while it was current: BASELINE, ADDED or UPDATED',
  release_version   STRING  NOT NULL COMMENT 'Release that created this version (valid from, inclusive)',
  release_seq       BIGINT  NOT NULL COMMENT 'Sequence of that release (valid from, inclusive)',
  changed_at        TIMESTAMP,
  reappeared        BOOLEAN,
  valid_to_seq      BIGINT  NOT NULL COMMENT 'Sequence of the release that changed or deleted it (valid until, exclusive)',
  valid_to_version  STRING  NOT NULL COMMENT 'Release label of valid_to_seq'
)
CLUSTER BY (valid_to_seq, PBKEY)
COMMENT 'Superseded versions of live records: one row per version that a later release updated or deleted. Together with adf_usa_current it reproduces the table as of any release.'
TBLPROPERTIES ('delta.enableDeletionVectors' = 'true');
