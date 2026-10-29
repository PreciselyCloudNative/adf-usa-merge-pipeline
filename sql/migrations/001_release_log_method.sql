-- One-off for tables created before the changefile path. New installs get these columns from 01_ops_tables.sql.
ALTER TABLE ${catalog}.ops.release_log ADD COLUMNS (
  method          STRING COMMENT 'CHANGEFILE or MERGE: how the release was applied',
  fallback_reason STRING COMMENT 'Why the MERGE ran instead of the changefile, NULL otherwise',
  audit           STRING COMMENT 'PASSED (content fingerprint matched the pinned source) or SKIPPED'
);
