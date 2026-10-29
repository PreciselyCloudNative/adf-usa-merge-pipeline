-- Control tables. ${catalog} is substituted at deploy time (bundle variable).

CREATE TABLE IF NOT EXISTS ${catalog}.ops.release_label (
  release_version      STRING  NOT NULL COMMENT 'Precisely release label, e.g. 2026.10.2',
  release_date         DATE             COMMENT 'Release date from the Precisely release document',
  source_delta_version BIGINT           COMMENT 'Version of the shared source table that carries this release (pin)',
  status               STRING  NOT NULL COMMENT 'PROPOSED (auto-detected) or CONFIRMED (human, vs release doc)',
  vendor_added         BIGINT           COMMENT 'Added count from the release document',
  vendor_updated       BIGINT           COMMENT 'Updated count from the release document',
  vendor_deleted       BIGINT           COMMENT 'Deleted count from the release document',
  vendor_row_count     BIGINT           COMMENT 'Expected table row count after this release (precisely.data.count)',
  confirmed_by         STRING,
  confirmed_at         TIMESTAMP,
  notes                STRING
)
COMMENT 'Authority for release labels and their source-version pins, seeded by hand / auto-proposed by detect'
TBLPROPERTIES ('delta.enableDeletionVectors' = 'true');

CREATE TABLE IF NOT EXISTS ${catalog}.ops.release_log (
  release_version       STRING  NOT NULL,
  release_seq           BIGINT  NOT NULL COMMENT 'Monotonic sequence assigned by us, 1 = baseline 2026.10.1',
  run_id                STRING,
  state                 STRING  NOT NULL COMMENT 'STARTED, GATED, APPLIED, COMPLETE, FAILED',
  source_delta_version  BIGINT,
  source_row_count      BIGINT,
  added                 BIGINT,
  updated               BIGINT,
  deleted               BIGINT,
  current_table_version BIGINT           COMMENT 'Version of adf_usa_current produced by the apply (rollback target = previous)',
  hash_spec_version     INT,
  started_at            TIMESTAMP,
  finished_at           TIMESTAMP,
  error                 STRING
)
COMMENT 'One row per release run, commit marker and rollback pointer'
TBLPROPERTIES ('delta.enableDeletionVectors' = 'true');
