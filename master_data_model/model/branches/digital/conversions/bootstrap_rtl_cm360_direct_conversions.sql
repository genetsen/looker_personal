-- One-time production bootstrap for direct CM360 RTL conversion history.
--
-- Purpose:
--   Create the persistent direct-CM360 staging table and MERGE the approved
--   enriched historical-backfill and current-seed exports into it. This does
--   not rebuild master_stg.data_model_v3; run the v3 builder separately only
--   after its integration candidate has passed QA.
--
-- Safe use:
--   Run once for the direct-history cutover. The routine rolling-window loader
--   is in merge_rtl_cm360_direct_conversions_latest.sql.

CREATE TABLE IF NOT EXISTS `looker-studio-pro-452620.master_stg.rtl_cm360_direct_conversions` (
  date DATE,
  advertiser STRING,
  campaign STRING,
  site STRING,
  activity_group STRING,
  activity STRING,
  creative STRING,
  placement STRING,
  package_roadblock STRING,
  total_conversions FLOAT64,
  activity_click_through_conversions FLOAT64,
  activity_view_through_conversions FLOAT64,
  total_conversions_revenue FLOAT64,
  activity_click_through_revenue FLOAT64,
  activity_view_through_revenue FLOAT64,
  source_table_name STRING,
  source_exported_at TIMESTAMP,
  source_load_purpose STRING,
  package_id STRING,
  placement_id STRING,
  conversion_row_key STRING,
  model_detail_key STRING,
  data_refresh_date DATE,
  staged_at TIMESTAMP
)
OPTIONS (
  description = 'Persistent direct CM360 RTL conversion history at source activity grain. Built by model/branches/digital/conversions/bootstrap_rtl_cm360_direct_conversions.sql and updated by merge_rtl_cm360_direct_conversions_latest.sql. Each enriched rolling export updates only matching logical conversion records; prior history remains. Source fields, parsed detail keys, export timestamps, and refresh metadata are retained. Cleanup owner: Master Data Model migration review.'
);

MERGE `looker-studio-pro-452620.master_stg.rtl_cm360_direct_conversions` AS target
USING (
  WITH source_rows AS (
    SELECT
      DATE(date) AS date,
      CAST(advertiser AS STRING) AS advertiser,
      CAST(campaign AS STRING) AS campaign,
      CAST(site AS STRING) AS site,
      CAST(activity_group AS STRING) AS activity_group,
      CAST(activity AS STRING) AS activity,
      CAST(creative AS STRING) AS creative,
      CAST(placement AS STRING) AS placement,
      CAST(package_roadblock AS STRING) AS package_roadblock,
      CAST(total_conversions AS FLOAT64) AS total_conversions,
      CAST(activity_click_through_conversions AS FLOAT64) AS activity_click_through_conversions,
      CAST(activity_view_through_conversions AS FLOAT64) AS activity_view_through_conversions,
      CAST(total_conversions_revenue AS FLOAT64) AS total_conversions_revenue,
      CAST(activity_click_through_revenue AS FLOAT64) AS activity_click_through_revenue,
      CAST(activity_view_through_revenue AS FLOAT64) AS activity_view_through_revenue,
      'Ritual_conversions_last14_cm360_1123381_1665558564_20260314_20260513_20260713_121837' AS source_table_name,
      TIMESTAMP('2026-07-13 19:18:41+00:00') AS source_exported_at,
      'one_time_history_backfill' AS source_load_purpose
    FROM `giant-spoon-299605.ALL_DCM_adswerve.Ritual_conversions_last14_cm360_1123381_1665558564_20260314_20260513_20260713_121837`

    UNION ALL

    SELECT
      DATE(date), CAST(advertiser AS STRING), CAST(campaign AS STRING),
      CAST(site AS STRING), CAST(activity_group AS STRING), CAST(activity AS STRING),
      CAST(creative AS STRING), CAST(placement AS STRING), CAST(package_roadblock AS STRING),
      CAST(total_conversions AS FLOAT64), CAST(activity_click_through_conversions AS FLOAT64),
      CAST(activity_view_through_conversions AS FLOAT64), CAST(total_conversions_revenue AS FLOAT64),
      CAST(activity_click_through_revenue AS FLOAT64), CAST(activity_view_through_revenue AS FLOAT64),
      'Ritual_conversions_last14_cm360_1123381_1665558564_20260514_20260712_20260713_085339',
      TIMESTAMP('2026-07-13 15:53:42+00:00'), 'current_enriched_seed'
    FROM `giant-spoon-299605.ALL_DCM_adswerve.Ritual_conversions_last14_cm360_1123381_1665558564_20260514_20260712_20260713_085339`
  ),
  keyed_rows AS (
    SELECT
      source_rows.*,
      REGEXP_EXTRACT(package_roadblock, r'\|([^|_]+)_') AS package_id,
      REGEXP_EXTRACT(placement, r'\|([^|_]+)_') AS placement_id,
      TO_HEX(SHA256(TO_JSON_STRING(STRUCT(date, advertiser, campaign, site, activity_group, activity, creative, placement, package_roadblock)))) AS conversion_row_key
    FROM source_rows
  )
  SELECT
    * EXCEPT (source_row_rank),
    CONCAT(LOWER(TRIM(package_id)), '|', FORMAT_DATE('%F', date), '|', LOWER(TRIM(placement_id)), '|', LOWER(TRIM(creative))) AS model_detail_key,
    CURRENT_DATE('America/New_York') AS data_refresh_date,
    CURRENT_TIMESTAMP() AS staged_at
  FROM (
    SELECT
      keyed_rows.*,
      ROW_NUMBER() OVER (PARTITION BY conversion_row_key ORDER BY source_exported_at DESC, source_table_name DESC) AS source_row_rank
    FROM keyed_rows
  )
  WHERE source_row_rank = 1
) AS source
ON target.conversion_row_key = source.conversion_row_key
WHEN MATCHED THEN UPDATE SET
  date = source.date, advertiser = source.advertiser, campaign = source.campaign,
  site = source.site, activity_group = source.activity_group, activity = source.activity,
  creative = source.creative, placement = source.placement, package_roadblock = source.package_roadblock,
  total_conversions = source.total_conversions,
  activity_click_through_conversions = source.activity_click_through_conversions,
  activity_view_through_conversions = source.activity_view_through_conversions,
  total_conversions_revenue = source.total_conversions_revenue,
  activity_click_through_revenue = source.activity_click_through_revenue,
  activity_view_through_revenue = source.activity_view_through_revenue,
  source_table_name = source.source_table_name, source_exported_at = source.source_exported_at,
  source_load_purpose = source.source_load_purpose, package_id = source.package_id,
  placement_id = source.placement_id, model_detail_key = source.model_detail_key,
  data_refresh_date = source.data_refresh_date, staged_at = source.staged_at
WHEN NOT MATCHED THEN INSERT ROW;
