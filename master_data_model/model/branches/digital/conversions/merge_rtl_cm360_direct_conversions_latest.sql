-- Routine production MERGE for enriched rolling direct CM360 RTL exports.
--
-- Purpose:
--   Select one newest enriched report with a true rolling window of at most
--   14 days, then update only matching source-activity records in persistent
--   direct CM360 history. It intentionally rejects exports without package and
--   placement fields instead of allowing a broader or ambiguous model join.
--
-- Safe use:
--   Run only after bootstrap_rtl_cm360_direct_conversions.sql has created the
--   target table. A missing eligible source is a safe failure, not an empty
--   refresh or an instruction to overwrite history.

DECLARE source_table_name STRING;
DECLARE source_exported_at TIMESTAMP;
DECLARE source_has_whole_number_conversions BOOL;

SET (source_table_name, source_exported_at) = (
  SELECT AS STRUCT
    t.table_name,
    t.creation_time
  FROM `giant-spoon-299605.ALL_DCM_adswerve.INFORMATION_SCHEMA.TABLES` AS t
  JOIN (
    SELECT table_name
    FROM `giant-spoon-299605.ALL_DCM_adswerve.INFORMATION_SCHEMA.COLUMNS`
    WHERE column_name IN ('package_roadblock', 'placement')
    GROUP BY table_name
    HAVING COUNT(DISTINCT column_name) = 2
  ) AS enriched
    USING (table_name)
  WHERE STARTS_WITH(t.table_name, 'Ritual_conversions_last14_cm360_1123381_1665558564_')
    AND DATE_DIFF(
      PARSE_DATE('%Y%m%d', REGEXP_EXTRACT(t.table_name, r'_\d{8}_(\d{8})_\d{8}_\d{6}$')),
      PARSE_DATE('%Y%m%d', REGEXP_EXTRACT(t.table_name, r'_(\d{8})_\d{8}_\d{8}_\d{6}$')),
      DAY
    ) BETWEEN 0 AND 14
  ORDER BY t.creation_time DESC
  LIMIT 1
);

ASSERT source_table_name IS NOT NULL AS 'No enriched rolling CM360 export with package and placement fields is available.';

EXECUTE IMMEDIATE FORMAT(
  """
  SELECT COUNTIF(total_conversions != FLOOR(total_conversions)) = 0
  FROM `giant-spoon-299605.ALL_DCM_adswerve.%s`
  """,
  source_table_name
) INTO source_has_whole_number_conversions;

ASSERT source_has_whole_number_conversions AS 'CM360 total_conversions contains fractional values; preserve the V3 integer conversion-field contract before merging.';

EXECUTE IMMEDIATE FORMAT(
  """
  MERGE `looker-studio-pro-452620.master_stg.rtl_cm360_direct_conversions` AS target
  USING (
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
      '%s' AS source_table_name,
      TIMESTAMP('%s') AS source_exported_at,
      'rolling_enriched_export' AS source_load_purpose,
      REGEXP_EXTRACT(package_roadblock, r'\\|([^|_]+)_') AS package_id,
      REGEXP_EXTRACT(placement, r'\\|([^|_]+)_') AS placement_id,
      TO_HEX(SHA256(TO_JSON_STRING(STRUCT(DATE(date), advertiser, campaign, site, activity_group, activity, creative, placement, package_roadblock)))) AS conversion_row_key,
      CONCAT(LOWER(TRIM(REGEXP_EXTRACT(package_roadblock, r'\\|([^|_]+)_'))), '|', FORMAT_DATE('%%F', DATE(date)), '|', LOWER(TRIM(REGEXP_EXTRACT(placement, r'\\|([^|_]+)_'))), '|', LOWER(TRIM(creative))) AS model_detail_key,
      CURRENT_DATE('America/New_York') AS data_refresh_date,
      CURRENT_TIMESTAMP() AS staged_at
    FROM `giant-spoon-299605.ALL_DCM_adswerve.%s`
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
  WHEN NOT MATCHED THEN INSERT ROW
  """,
  source_table_name,
  FORMAT_TIMESTAMP('%F %T+00:00', source_exported_at),
  source_table_name
);
