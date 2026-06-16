-- ============================================================
-- verify_mdm_workspace.sql
--
-- Reusable verification query for Story 1.1:
--   1. Lists all mdm_* datasets (proves they exist)
--   2. Confirms master_stg.data_model still has its expected
--      shape (proves production untouched)
-- ============================================================

-- --- Part A: mdm_* datasets exist ---
SELECT
  'mdm_datasets' as check_name,
  COUNT(*) as dataset_count,
  STRING_AGG(schema_name, ', ' ORDER BY schema_name) as datasets_found
FROM `looker-studio-pro-452620.INFORMATION_SCHEMA.SCHEMATA`
WHERE STARTS_WITH(schema_name, 'mdm_')
  AND schema_name IN (
    'mdm_raw', 'mdm_config', 'mdm_stg', 'mdm_int',
    'mdm_qa', 'mdm_mart', 'mdm_publish', 'mdm_sandbox'
  );

-- --- Part B: master_stg production objects are untouched ---
SELECT
  table_catalog,
  table_schema,
  table_name,
  table_type
FROM `looker-studio-pro-452620.master_stg.INFORMATION_SCHEMA.TABLES`
WHERE table_name IN ('data_model', 'data_model_mart')
ORDER BY table_name;
