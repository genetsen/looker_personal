-- Compatibility wrapper for the consolidated package/date summary model.
--
-- Purpose:
--   Keep existing data_model_v2 consumers pointed at the same package/date
--   shape now owned directly by data_model, including initiative.

CREATE OR REPLACE VIEW `looker-studio-pro-452620.master_stg.data_model_v2` AS
SELECT
  -- LIVE VIEW NOTE: Compatibility wrapper exposing the canonical package/date
  -- model through the maintained data_model_v2 object.
  *
FROM `looker-studio-pro-452620.master_stg.data_model`;
