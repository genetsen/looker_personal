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
  WHERE NOT CONTAINS_SUBSTR(row_data_issue_category, 'low_signal_dcm')
),

with_rollups AS (
  SELECT
    * EXCEPT(
      pkg_est_spend,
      pkg_est_impressions,
      pkg_act_spend,
      pkg_act_impressions,
      pkg_act_clicks,
      pkg_fpd_orig_impressions,
      pkg_fpd_orig_spend,
      pkg_fpd_updated_impressions,
      pkg_fpd_updated_spend,
      pkg_fpd_combined_impressions,
      pkg_fpd_combined_spend,
      pkg_over_bool,
      pkg_over_flag,
      model_view_runtime_timestamp
    ),
    SUM(COALESCE(planned_daily_spend_pk, 0)) OVER (PARTITION BY package_id_joined) AS pkg_est_spend,
    SUM(COALESCE(planned_daily_impressions_pk, 0)) OVER (PARTITION BY package_id_joined) AS pkg_est_impressions,
    SUM(COALESCE(final_spend, 0)) OVER (PARTITION BY package_id_joined) AS pkg_act_spend,
    SUM(COALESCE(final_impressions, 0)) OVER (PARTITION BY package_id_joined) AS pkg_act_impressions,
    SUM(COALESCE(final_clicks, 0)) OVER (PARTITION BY package_id_joined) AS pkg_act_clicks,
    SUM(COALESCE(fpd_orig_impressions, 0)) OVER (PARTITION BY package_id_joined) AS pkg_fpd_orig_impressions,
    SUM(COALESCE(fpd_orig_spend, 0)) OVER (PARTITION BY package_id_joined) AS pkg_fpd_orig_spend,
    SUM(COALESCE(fpd_updated_impressions, 0)) OVER (PARTITION BY package_id_joined) AS pkg_fpd_updated_impressions,
    SUM(COALESCE(fpd_updated_spend, 0)) OVER (PARTITION BY package_id_joined) AS pkg_fpd_updated_spend,
    SUM(COALESCE(fpd_impressions, 0)) OVER (PARTITION BY package_id_joined) AS pkg_fpd_combined_impressions,
    SUM(COALESCE(fpd_spend, 0)) OVER (PARTITION BY package_id_joined) AS pkg_fpd_combined_spend
  FROM filtered_rows
)

SELECT
  *,
  CASE
    WHEN pkg_est_spend = 0 THEN NULL
    ELSE pkg_act_spend > pkg_est_spend
  END AS pkg_over_bool,
  CASE
    WHEN pkg_est_spend = 0 THEN NULL
    WHEN pkg_act_spend > pkg_est_spend THEN 1
    ELSE 0
  END AS pkg_over_flag,
  CURRENT_TIMESTAMP() AS model_view_runtime_timestamp
FROM with_rollups;
