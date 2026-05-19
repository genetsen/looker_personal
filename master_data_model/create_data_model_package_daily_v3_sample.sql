-- Sample package/date summary model for the creative-grain recovery.
--
-- Purpose:
--   Keep package budgets and package/date rollups at the package/date grain.
--   Adds the ADIF-style `initiative` field from Prisma's source column
--   `initative`, without adding lower-grain creative fields to this summary.

CREATE OR REPLACE VIEW `looker-studio-pro-452620.master_stg.data_model_package_daily_v3_sample` AS
WITH
prisma_meta AS (
  SELECT
    package_id,
    ARRAY_AGG(initative IGNORE NULLS ORDER BY report_date DESC NULLS LAST LIMIT 1)[SAFE_OFFSET(0)] AS initiative
  FROM `looker-studio-pro-452620.20250327_data_model.prisma_expanded_full`
  WHERE package_type != 'Child'
    AND start_date >= DATE '2025-01-01'
  GROUP BY package_id
)

SELECT
  -- Sample view for comparing package/date summary against detail-grain outputs.
  dm.*,
  COALESCE(pm.initiative, dm.`_package_name`) AS initiative
FROM `looker-studio-pro-452620.master_stg.data_model` AS dm
LEFT JOIN prisma_meta AS pm
  ON dm.`_package_id` = pm.package_id;
