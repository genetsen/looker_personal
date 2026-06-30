-- Build reporting mart over the generalized package/date data model.
--
-- Purpose:
--   Keep the master model as the evidence layer, then apply reporting-only row
--   exclusions and recalculate package rollups after those exclusions.

CREATE OR REPLACE VIEW `looker-studio-pro-452620.master_stg.data_model_mart_v2` AS
WITH
filtered_rows AS (
  SELECT *
  FROM `looker-studio-pro-452620.master_stg.data_model_v2`
  WHERE NOT CONTAINS_SUBSTR(`qa_data_issues`, 'low_signal_dcm')
),

with_rollups AS (
  SELECT
    * EXCEPT(
      `qa_pkg_est_spend_doNotSum`,
      `qa_pkg_est_impressions_doNotSum`,
      `qa_pkg_act_spend_doNotSum`,
      `qa_pkg_act_impressions_doNotSum`,
      `qa_pkg_act_clicks_doNotSum`,
      `qa_pkg_fpd_impressions_doNotSum`,
      `qa_pkg_fpd_spend_doNotSum`,
      `qa_package_spend_over_plan_flag`
    ),
    SUM(COALESCE(`_planned_spend`, 0)) OVER (PARTITION BY `_package_id`) AS `qa_pkg_est_spend_doNotSum`,
    SUM(COALESCE(`_planned_impressions`, 0)) OVER (PARTITION BY `_package_id`) AS `qa_pkg_est_impressions_doNotSum`,
    SUM(COALESCE(`_spend`, 0)) OVER (PARTITION BY `_package_id`) AS `qa_pkg_act_spend_doNotSum`,
    SUM(COALESCE(`_impressions`, 0)) OVER (PARTITION BY `_package_id`) AS `qa_pkg_act_impressions_doNotSum`,
    SUM(COALESCE(`_clicks`, 0)) OVER (PARTITION BY `_package_id`) AS `qa_pkg_act_clicks_doNotSum`,
    SUM(COALESCE(`fpd_impressions`, 0)) OVER (PARTITION BY `_package_id`) AS `qa_pkg_fpd_impressions_doNotSum`,
    SUM(COALESCE(`fpd_spend`, 0)) OVER (PARTITION BY `_package_id`) AS `qa_pkg_fpd_spend_doNotSum`
  FROM filtered_rows
)

SELECT
  -- LIVE VIEW NOTE: Versioned reporting mart over data_model_v2 with the same
  -- reporting exclusions and package-rollup rules as the main mart.
  *,
  CASE
    WHEN `qa_pkg_est_spend_doNotSum` = 0 THEN NULL
    ELSE `qa_pkg_act_spend_doNotSum` > `qa_pkg_est_spend_doNotSum`
  END AS `qa_package_spend_over_plan_flag`
FROM with_rollups;
