-- Normalize the legacy-schema RTL compatibility table for historical checks.
--
-- Purpose:
--   Parse package IDs and keep conversion activity and creative detail at the
--   compatibility table's source grain. The table now mirrors direct CM360
--   history; legacy delivery and Sheet-lineage columns remain NULL. Production
--   v3 uses the direct CM360 history table instead of this reference query.

SELECT
  REGEXP_EXTRACT(package_roadblock, r'\|([^|_]+)_') AS `_package_id`,
  DATE(date) AS `_date`,
  CAST(new_site AS STRING) AS conv_site,
  CAST(site_cm360 AS STRING) AS conv_site_cm360,
  CAST(campaign AS STRING) AS conv_campaign,
  CAST(campaign_id AS STRING) AS conv_campaign_id,
  CAST(package_roadblock AS STRING) AS conv_package_roadblock,
  CAST(creative AS STRING) AS conv_creative,
  CAST(activity AS STRING) AS conv_activity,
  SUM(CAST(total_conversions AS INT64)) AS conv_total_conversions,
  SUM(CAST(impressions AS INT64)) AS conv_source_impressions,
  SUM(CAST(clicks AS INT64)) AS conv_source_clicks,
  COUNT(*) AS conv_source_row_count,
  MAX(loaded_at) AS conv_loaded_at,
  ARRAY_AGG(CAST(source_sheet_id AS STRING) IGNORE NULLS ORDER BY loaded_at DESC LIMIT 1)[SAFE_OFFSET(0)] AS conv_source_sheet_id,
  ARRAY_AGG(CAST(source_sheet_tab AS STRING) IGNORE NULLS ORDER BY loaded_at DESC LIMIT 1)[SAFE_OFFSET(0)] AS conv_source_sheet_tab,
  ARRAY_AGG(CAST(source_sheet_gid AS STRING) IGNORE NULLS ORDER BY loaded_at DESC LIMIT 1)[SAFE_OFFSET(0)] AS conv_source_sheet_gid
FROM `looker-studio-pro-452620.landing.rtl_conv_report`
WHERE DATE(date) >= DATE '2025-01-01'
  AND REGEXP_EXTRACT(package_roadblock, r'\|([^|_]+)_') IS NOT NULL
GROUP BY
  `_package_id`,
  `_date`,
  conv_site,
  conv_site_cm360,
  conv_campaign,
  conv_campaign_id,
  conv_package_roadblock,
  conv_creative,
  conv_activity;
