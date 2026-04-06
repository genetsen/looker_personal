CREATE OR REPLACE VIEW `looker-studio-pro-452620.Prisma.prisma_processed_plusDCMFPD` AS
WITH data AS (
  SELECT
    p.* EXCEPT(click_through_url, external_entity_id, provider_name, ad_server_placement_id),
    d.total_dcm_impressions,
    d.dcm_max_date,
    d.dcm_min_date,
    f.total_fpd_impressions,
    f.total_fpd_spend,
    f.total_fpd_clicks,
    f.fpd_max_date,
    f.fpd_min_date
  FROM `looker-studio-pro-452620.20250327_data_model.prisma_porcessed` AS p
  LEFT JOIN (
    SELECT
      package_id,
      SUM(impressions) AS total_dcm_impressions,
      MAX(date) AS dcm_max_date,
      MIN(date) AS dcm_min_date
    FROM `looker-studio-pro-452620.DCM.20250505_costModel_v5`
    WHERE package_id IS NOT NULL
    GROUP BY package_id
  ) AS d
    ON p.package_id = d.package_id
  LEFT JOIN (
    SELECT
      package_id,
      SUM(impressions) AS total_fpd_impressions,
      SUM(spend) AS total_fpd_spend,
      SUM(clicks) AS total_fpd_clicks,
      MAX(date_final) AS fpd_max_date,
      MIN(date_final) AS fpd_min_date
    FROM `looker-studio-pro-452620.landing.fpd_data_ranged_shortcutsFolder`
    WHERE package_id IS NOT NULL
    GROUP BY package_id
  ) AS f
    ON p.package_id = f.package_id
)
SELECT
  *,
  CASE
    WHEN total_dcm_impressions IS NOT NULL
      AND (
        total_fpd_impressions IS NOT NULL
        OR total_fpd_spend IS NOT NULL
        OR total_fpd_clicks IS NOT NULL
        OR fpd_min_date IS NOT NULL
        OR fpd_max_date IS NOT NULL
      ) THEN 'DCM+FPD'
    WHEN total_dcm_impressions IS NOT NULL THEN 'DCM'
    WHEN (
      total_fpd_impressions IS NOT NULL
      OR total_fpd_spend IS NOT NULL
      OR total_fpd_clicks IS NOT NULL
      OR fpd_min_date IS NOT NULL
      OR fpd_max_date IS NOT NULL
    ) THEN 'FPD'
    ELSE NULL
  END AS tracking_delivery_source,
  CASE
    WHEN total_dcm_impressions IS NULL THEN
      CASE
        WHEN (
          total_fpd_impressions IS NOT NULL
          OR total_fpd_spend IS NOT NULL
          OR total_fpd_clicks IS NOT NULL
          OR fpd_min_date IS NOT NULL
          OR fpd_max_date IS NOT NULL
        ) THEN 0
        ELSE 1
      END
    WHEN total_dcm_impressions < 1000 THEN
      CASE
        WHEN DATE_DIFF(CAST(script_run_date AS DATE), start_date, DAY) > 7 THEN 1
        ELSE 0
      END
    ELSE 0
  END AS untracked_flag,
  CASE
    WHEN report_date <= start_date THEN 'Pre-flight'
    WHEN total_dcm_impressions IS NULL THEN
      CASE
        WHEN (
          total_fpd_impressions IS NOT NULL
          OR total_fpd_spend IS NOT NULL
          OR total_fpd_clicks IS NOT NULL
          OR fpd_min_date IS NOT NULL
          OR fpd_max_date IS NOT NULL
        ) THEN 'Tracking Delivery'
        ELSE 'Untagged - Needs FPD'
      END
    WHEN total_dcm_impressions < 1000 THEN
      CASE
        WHEN DATE_DIFF(CAST(script_run_date AS DATE), start_date, DAY) > 7 THEN 'Tagged but not tracking delivery'
        ELSE 'delayed'
      END
    ELSE 'Tracking Delivery'
  END AS tracking_status,
  DATE_DIFF(SAFE_CAST(script_run_date AS DATE), start_date, DAY) AS days_since_start_date,
  DATE_DIFF(SAFE_CAST(dcm_max_date AS DATE), dcm_min_date, DAY) AS dcm_days_live,
  CASE
    WHEN DATE(script_run_date) > end_date THEN planned_imps_pk
    WHEN DATE(script_run_date) < start_date THEN 0
    ELSE (
      DATE_DIFF(
        LEAST(DATE(script_run_date), end_date),
        start_date,
        DAY
      ) + 1
    ) * planned_daily_impressions_pk
  END AS expected_impressions_to_date,
  SAFE_DIVIDE(
    total_dcm_impressions,
    DATE_DIFF(SAFE_CAST(dcm_max_date AS DATE), dcm_min_date, DAY)
  ) AS dcm_avg_impressions_per_day,
  planned_daily_impressions_pk AS p_planned_imps_per_day
FROM data;
