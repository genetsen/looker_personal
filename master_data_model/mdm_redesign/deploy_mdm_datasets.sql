-- ============================================================
-- deploy_mdm_datasets.sql
--
-- Creates the mdm_* dataset zones for the master_data_model
-- versioned redesign.  These datasets are sibling/versioned
-- spaces that replace nothing in production.  All new
-- candidate views and tables are built inside these zones.
--
-- Safe to re-run (CREATE SCHEMA IF NOT EXISTS).
-- Run from looker-studio-pro-452620.
-- ============================================================

-- mdm_raw: raw source declarations (mirrored vendor tables, etc.)
CREATE SCHEMA IF NOT EXISTS `looker-studio-pro-452620.mdm_raw`
OPTIONS(
  location = 'US',
  description = 'Raw source declarations for the master data model redesign. Mirrored vendor/ingested tables before any transformation. Created by deploy_mdm_datasets.sql.'
);

-- mdm_config: mapping configuration tables
CREATE SCHEMA IF NOT EXISTS `looker-studio-pro-452620.mdm_config`
OPTIONS(
  location = 'US',
  description = 'Mapping configuration zone for the master data model redesign. Stores versioned source-field mappings, dimension-placeholder rules, metric rules, and inference rules. Created by deploy_mdm_datasets.sql.'
);

-- mdm_stg: staging layer (cleaned / typed / joined intermediates)
CREATE SCHEMA IF NOT EXISTS `looker-studio-pro-452620.mdm_stg`
OPTIONS(
  location = 'US',
  description = 'Staging zone for the master data model redesign. Cleaned, typed, and lightly joined source data before final aggregation or universal output. Created by deploy_mdm_datasets.sql.'
);

-- mdm_int: intermediate logic (business-rule application, grain bridging)
CREATE SCHEMA IF NOT EXISTS `looker-studio-pro-452620.mdm_int`
OPTIONS(
  location = 'US',
  description = 'Intermediate logic zone for the master data model redesign. Applies business rules, grain bridges, placeholder filling, and metadata inference before the universal final table. Created by deploy_mdm_datasets.sql.'
);

-- mdm_qa: QA and validation artifacts
CREATE SCHEMA IF NOT EXISTS `looker-studio-pro-452620.mdm_qa`
OPTIONS(
  location = 'US',
  description = 'QA and validation zone for the master data model redesign. Contains parity-check results, schema-coverage snapshots, reconciliation totals, and proof reports. Not used for reporting. Created by deploy_mdm_datasets.sql.'
);

-- mdm_mart: shortcut marts (package, creative, DMA, etc.)
CREATE SCHEMA IF NOT EXISTS `looker-studio-pro-452620.mdm_mart`
OPTIONS(
  location = 'US',
  description = 'Shortcut-mart zone for the master data model redesign. Derived from the universal final evidence table for common reporting grains (package/date, creative/date, DMA/date). These are shortcuts, not separate sources of truth. Created by deploy_mdm_datasets.sql.'
);

-- mdm_publish: final published / BI-facing views
CREATE SCHEMA IF NOT EXISTS `looker-studio-pro-452620.mdm_publish`
OPTIONS(
  location = 'US',
  description = 'Publish zone for the master data model redesign. Stable BI-facing views and final published output objects, ready for dashboard consumption after approval. Created by deploy_mdm_datasets.sql.'
);

-- mdm_sandbox: temporary / experimental redesign space
CREATE SCHEMA IF NOT EXISTS `looker-studio-pro-452620.mdm_sandbox`
OPTIONS(
  location = 'US',
  description = 'Sandbox zone for the master data model redesign. Temporary / experimental workspace for ad-hoc development, testing, and prototyping before promotion to other mdm_* zones. Created by deploy_mdm_datasets.sql.'
);

-- summary
SELECT
  schema_name,
  schema_owner,
  creation_time
FROM `looker-studio-pro-452620.INFORMATION_SCHEMA.SCHEMATA`
WHERE STARTS_WITH(schema_name, 'mdm_')
ORDER BY schema_name;
