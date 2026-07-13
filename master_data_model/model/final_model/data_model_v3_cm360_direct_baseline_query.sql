-- Read-only SQL Change Guard baseline for the direct-CM360 V3 candidate.
-- It gives current V3 rows the stable record key used by the candidate without
-- altering the live table.

SELECT
  v3.*,
  TO_HEX(SHA256(CONCAT('base|', TO_JSON_STRING(v3)))) AS qa_cm360_record_key
FROM `looker-studio-pro-452620.master_stg.data_model_v3` AS v3;
