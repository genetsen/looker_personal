-- Build reporting mart over the generalized package/date data model.
--
-- Purpose:
--   Keep the master model as the evidence layer, then apply reporting-only row
--   exclusions and recalculate package rollups after those exclusions.

CREATE OR REPLACE VIEW `looker-studio-pro-452620.master_stg.data_model_mart` AS
WITH
filtered_rows AS (
  SELECT *
  FROM `looker-studio-pro-452620.master_stg.data_model`
  WHERE NOT CONTAINS_SUBSTR(`qa_row_data_issue_category`, 'low_signal_dcm')
),

with_rollups AS (
  SELECT
    * EXCEPT(
      `qa_pkg_est_spend_doNotSum`,
      `qa_pkg_est_impressions_doNotSum`,
      `qa_pkg_act_spend_doNotSum`,
      `qa_pkg_act_impressions_doNotSum`,
      `qa_pkg_act_clicks_doNotSum`,
      `qa_pkg_fpd_orig_impressions_doNotSum`,
      `qa_pkg_fpd_orig_spend_doNotSum`,
      `qa_pkg_fpd_updated_impressions_doNotSum`,
      `qa_pkg_fpd_updated_spend_doNotSum`,
      `qa_pkg_fpd_combined_impressions_doNotSum`,
      `qa_pkg_fpd_combined_spend_doNotSum`,
      `qa_pkg_over_bool`,
      `qa_pkg_over_flag`,
      `qa_model_view_runtime_timestamp`
    ),
    SUM(COALESCE(`_planned_spend`, 0)) OVER (PARTITION BY `_package_id`) AS `qa_pkg_est_spend_doNotSum`,
    SUM(COALESCE(`_planned_impressions`, 0)) OVER (PARTITION BY `_package_id`) AS `qa_pkg_est_impressions_doNotSum`,
    SUM(COALESCE(`_spend`, 0)) OVER (PARTITION BY `_package_id`) AS `qa_pkg_act_spend_doNotSum`,
    SUM(COALESCE(`_impressions`, 0)) OVER (PARTITION BY `_package_id`) AS `qa_pkg_act_impressions_doNotSum`,
    SUM(COALESCE(`_clicks`, 0)) OVER (PARTITION BY `_package_id`) AS `qa_pkg_act_clicks_doNotSum`,
    SUM(COALESCE(`fpd_orig_impressions`, 0)) OVER (PARTITION BY `_package_id`) AS `qa_pkg_fpd_orig_impressions_doNotSum`,
    SUM(COALESCE(`fpd_orig_spend`, 0)) OVER (PARTITION BY `_package_id`) AS `qa_pkg_fpd_orig_spend_doNotSum`,
    SUM(COALESCE(`fpd_updated_impressions`, 0)) OVER (PARTITION BY `_package_id`) AS `qa_pkg_fpd_updated_impressions_doNotSum`,
    SUM(COALESCE(`fpd_updated_spend`, 0)) OVER (PARTITION BY `_package_id`) AS `qa_pkg_fpd_updated_spend_doNotSum`,
    SUM(COALESCE(`fpd_impressions`, 0)) OVER (PARTITION BY `_package_id`) AS `qa_pkg_fpd_combined_impressions_doNotSum`,
    SUM(COALESCE(`fpd_spend`, 0)) OVER (PARTITION BY `_package_id`) AS `qa_pkg_fpd_combined_spend_doNotSum`
  FROM filtered_rows
)

SELECT
  *,
  CASE
    WHEN `qa_pkg_est_spend_doNotSum` = 0 THEN NULL
    ELSE `qa_pkg_act_spend_doNotSum` > `qa_pkg_est_spend_doNotSum`
  END AS `qa_pkg_over_bool`,
  CASE
    WHEN `qa_pkg_est_spend_doNotSum` = 0 THEN NULL
    WHEN `qa_pkg_act_spend_doNotSum` > `qa_pkg_est_spend_doNotSum` THEN 1
    ELSE 0
  END AS `qa_pkg_over_flag`,
  CURRENT_TIMESTAMP() AS `qa_model_view_runtime_timestamp`
FROM with_rollups;
