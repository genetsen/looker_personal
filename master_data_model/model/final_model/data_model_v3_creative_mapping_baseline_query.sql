-- Reconstructed pre-mapping V3 baseline for SQL Change Guard.
-- The live table already contains the approved mapping result, so this query
-- restores the preserved raw name as the displayed name without changing any
-- row, key, metric, or other field.
SELECT
  -- QA OBJECT NOTE: Isolated reconstructed baseline for creative mapping QA.
  -- Safe to delete after Guard passes and production is verified.
  baseline.* REPLACE (baseline._creative_name_raw AS _creative_name)
FROM `looker-studio-pro-452620.master_stg.data_model_v3` AS baseline
