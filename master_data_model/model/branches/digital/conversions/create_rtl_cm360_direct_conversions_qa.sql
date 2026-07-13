-- QA-only direct CM360 conversion candidate.
--
-- Purpose: Select the newest rolling CM360 export, retain every raw source
-- field with stable staging metadata, and create a delivery-detail metrics
-- sidecar. It does not modify the current Sheet path or production v3 table.
--
-- Safe use: Run this script only for the direct-CM360 migration QA. Both
-- output tables are disposable after the migration decision is complete.

DECLARE source_table_name STRING;
DECLARE source_exported_at TIMESTAMP;

SET (source_table_name, source_exported_at) = (
  SELECT AS STRUCT
    table_name,
    creation_time
  FROM `giant-spoon-299605.ALL_DCM_adswerve.INFORMATION_SCHEMA.TABLES`
  WHERE STARTS_WITH(
    table_name,
    'Ritual_conversions_last14_cm360_1123381_1665558564_'
  )
  ORDER BY PARSE_DATETIME(
    '%Y%m%d_%H%M%S',
    REGEXP_EXTRACT(table_name, r'_(\d{8}_\d{6})$')
  ) DESC
  LIMIT 1
);

EXECUTE IMMEDIATE FORMAT(
  """
  CREATE OR REPLACE TABLE
    `looker-studio-pro-452620.master_stg.rtl_cm360_direct_conversions_qa`
  OPTIONS (
    description = 'QA-only snapshot for the planned direct CM360 RTL conversion source. Created by model/branches/digital/conversions/create_rtl_cm360_direct_conversions_qa.sql; differs from the current Sheet branch by preserving all direct CM360 source and revenue fields plus parsed package/placement keys. Safe to delete after direct-CM360 migration QA is approved or abandoned. Cleanup owner: Master Data Model migration review.'
  ) AS
  SELECT
    -- QA-only raw staging: every direct CM360 source field plus auditable keys.
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
    REGEXP_EXTRACT(package_roadblock, r'\\|([^|_]+)_') AS package_id,
    REGEXP_EXTRACT(placement, r'\\|([^|_]+)_') AS placement_id,
    TO_HEX(SHA256(TO_JSON_STRING(STRUCT(
      DATE(date) AS conversion_date,
      CAST(advertiser AS STRING) AS advertiser,
      CAST(campaign AS STRING) AS campaign,
      CAST(site AS STRING) AS site,
      CAST(activity_group AS STRING) AS activity_group,
      CAST(activity AS STRING) AS activity,
      CAST(creative AS STRING) AS creative,
      CAST(placement AS STRING) AS placement,
      CAST(package_roadblock AS STRING) AS package_roadblock
    )))) AS conversion_row_key,
    CONCAT(
      LOWER(TRIM(REGEXP_EXTRACT(package_roadblock, r'\\|([^|_]+)_'))), '|',
      FORMAT_DATE('%%F', DATE(date)), '|',
      LOWER(TRIM(REGEXP_EXTRACT(placement, r'\\|([^|_]+)_'))), '|',
      LOWER(TRIM(CAST(creative AS STRING)))
    ) AS model_detail_key,
    '%s' AS source_table_name,
    TIMESTAMP('%s') AS source_exported_at,
    CURRENT_DATE('America/New_York') AS data_refresh_date,
    CURRENT_TIMESTAMP() AS staged_at
  FROM `giant-spoon-299605.ALL_DCM_adswerve.%s`
  """,
  source_table_name,
  FORMAT_TIMESTAMP('%F %T+00:00', source_exported_at),
  source_table_name
);

CREATE OR REPLACE TABLE
  `looker-studio-pro-452620.master_stg.rtl_cm360_conversion_metrics_detail_qa`
OPTIONS (
  description = 'QA-only direct CM360 conversion metrics sidecar at package/date/parsed-placement-ID/creative grain. Created by model/branches/digital/conversions/create_rtl_cm360_direct_conversions_qa.sql; differs from the current Sheet outcome-row branch by attaching metrics through a logical detail key without adding a UNION branch. Unmatched source keys remain visible with a join status and are never attached at a broader grain. Safe to delete after direct-CM360 migration QA is approved or abandoned. Cleanup owner: Master Data Model migration review.'
) AS
WITH conversion_metrics AS (
  SELECT
    package_id,
    date AS conversion_date,
    placement_id,
    creative,
    model_detail_key,
    COUNT(*) AS source_activity_record_count,
    SUM(total_conversions) AS conv_total_conversions,
    SUM(activity_click_through_conversions) AS conv_click_through_conversions,
    SUM(activity_view_through_conversions) AS conv_view_through_conversions,
    SUM(total_conversions_revenue) AS conv_total_revenue,
    SUM(activity_click_through_revenue) AS conv_click_through_revenue,
    SUM(activity_view_through_revenue) AS conv_view_through_revenue,
    SUM(IF(LOWER(TRIM(activity)) = 'site visit', total_conversions, 0)) AS conv_site_visits,
    SUM(IF(LOWER(TRIM(activity)) = 'view product', total_conversions, 0)) AS conv_view_products,
    SUM(IF(LOWER(TRIM(activity)) = 'add to cart', total_conversions, 0)) AS conv_add_to_carts,
    SUM(IF(LOWER(TRIM(activity)) = 'begin checkout', total_conversions, 0)) AS conv_begin_checkouts,
    SUM(IF(LOWER(TRIM(activity)) = 'purchase', total_conversions, 0)) AS conv_purchases,
    MAX(source_table_name) AS source_table_name,
    MAX(source_exported_at) AS source_exported_at,
    MAX(data_refresh_date) AS data_refresh_date,
    MAX(staged_at) AS staged_at
  FROM `looker-studio-pro-452620.master_stg.rtl_cm360_direct_conversions_qa`
  GROUP BY package_id, conversion_date, placement_id, creative, model_detail_key
),
model_package_date AS (
  SELECT DISTINCT
    LOWER(TRIM(_package_id)) AS package_id,
    _date AS conversion_date
  FROM `looker-studio-pro-452620.master_stg.data_model_v3`
  WHERE qa_v3_source_detail_type = 'dcm'
),
model_placement AS (
  SELECT DISTINCT
    LOWER(TRIM(_package_id)) AS package_id,
    _date AS conversion_date,
    LOWER(TRIM(_placement_id)) AS placement_id
  FROM `looker-studio-pro-452620.master_stg.data_model_v3`
  WHERE qa_v3_source_detail_type = 'dcm'
),
model_detail AS (
  SELECT
    LOWER(TRIM(_package_id)) AS package_id,
    _date AS conversion_date,
    LOWER(TRIM(_placement_id)) AS placement_id,
    LOWER(TRIM(_creative_name)) AS creative_key,
    COUNT(*) AS model_detail_match_count
  FROM `looker-studio-pro-452620.master_stg.data_model_v3`
  WHERE qa_v3_source_detail_type = 'dcm'
  GROUP BY package_id, conversion_date, placement_id, creative_key
)
SELECT
  -- QA-only sidecar: preserves every source metric and makes join safety visible.
  c.*,
  COALESCE(d.model_detail_match_count, 0) AS model_detail_match_count,
  CASE
    WHEN pd.package_id IS NULL THEN 'no_package_date_match'
    WHEN mp.package_id IS NULL THEN 'no_placement_match'
    WHEN d.model_detail_match_count IS NULL THEN 'no_creative_match'
    WHEN d.model_detail_match_count = 1 THEN 'unique_detail_match'
    ELSE 'ambiguous_detail_match'
  END AS model_detail_join_status
FROM conversion_metrics AS c
LEFT JOIN model_package_date AS pd
  ON LOWER(TRIM(c.package_id)) = pd.package_id
  AND c.conversion_date = pd.conversion_date
LEFT JOIN model_placement AS mp
  ON LOWER(TRIM(c.package_id)) = mp.package_id
  AND c.conversion_date = mp.conversion_date
  AND LOWER(TRIM(c.placement_id)) = mp.placement_id
LEFT JOIN model_detail AS d
  ON LOWER(TRIM(c.package_id)) = d.package_id
  AND c.conversion_date = d.conversion_date
  AND LOWER(TRIM(c.placement_id)) = d.placement_id
  AND LOWER(TRIM(c.creative)) = d.creative_key;

CREATE OR REPLACE TABLE
  `looker-studio-pro-452620.master_stg.rtl_cm360_dcm_detail_full_outer_qa`
OPTIONS (
  description = 'QA-only full outer join between current-window v3 DCM detail and the direct CM360 conversion sidecar. Created by model/branches/digital/conversions/create_rtl_cm360_direct_conversions_qa.sql; differs from the current Sheet outcome-row branch by preserving matched delivery-plus-conversion rows, delivery-only rows, and conversion-only rows without a conversion UNION branch. Safe to delete after direct-CM360 migration QA is approved or abandoned. Cleanup owner: Master Data Model migration review.'
) AS
WITH source_window AS (
  SELECT
    MIN(date) AS first_conversion_date,
    MAX(date) AS last_conversion_date
  FROM `looker-studio-pro-452620.master_stg.rtl_cm360_direct_conversions_qa`
),
dcm_detail AS (
  SELECT d.*
  FROM `looker-studio-pro-452620.master_stg.data_model_v3` AS d
  CROSS JOIN source_window AS w
  WHERE d.qa_v3_source_detail_type = 'dcm'
    AND d._date BETWEEN w.first_conversion_date AND w.last_conversion_date
),
conversion_detail AS (
  SELECT *
  FROM `looker-studio-pro-452620.master_stg.rtl_cm360_conversion_metrics_detail_qa`
)
SELECT
  -- QA-only full outer output: conversion-only rows retain no inferred delivery metrics.
  CASE
    WHEN d._package_id IS NULL THEN 'conversion_only'
    WHEN c.model_detail_key IS NULL THEN 'delivery_only'
    ELSE 'delivery_with_conversion'
  END AS qa_cm360_outer_join_row_type,
  COALESCE(d._package_id, c.package_id) AS `_package_id`,
  COALESCE(d._date, c.conversion_date) AS `_date`,
  COALESCE(d._placement_id, c.placement_id) AS `_placement_id`,
  COALESCE(d._creative_name, c.creative) AS `_creative_name`,
  TO_HEX(SHA256(TO_JSON_STRING(STRUCT(
    COALESCE(d._package_id, c.package_id) AS package_id,
    COALESCE(d._date, c.conversion_date) AS record_date,
    COALESCE(d._placement_id, c.placement_id) AS placement_id,
    COALESCE(d._creative_name, c.creative) AS creative,
    COALESCE(d.qa_v3_ad_name, '[no_dcm_ad]') AS dcm_ad_name,
    COALESCE(c.model_detail_join_status, 'no_conversion_source') AS conversion_join_status
  )))) AS qa_cm360_outer_join_record_key,
  d.* EXCEPT (_package_id, _date, _placement_id, _creative_name),
  c.model_detail_key AS cm360_model_detail_key,
  c.model_detail_join_status AS cm360_model_detail_join_status,
  c.model_detail_match_count AS cm360_model_detail_match_count,
  c.source_activity_record_count AS cm360_source_activity_record_count,
  c.conv_total_conversions AS cm360_conv_total_conversions,
  c.conv_click_through_conversions AS cm360_conv_click_through_conversions,
  c.conv_view_through_conversions AS cm360_conv_view_through_conversions,
  c.conv_total_revenue AS cm360_conv_total_revenue,
  c.conv_click_through_revenue AS cm360_conv_click_through_revenue,
  c.conv_view_through_revenue AS cm360_conv_view_through_revenue,
  c.conv_site_visits AS cm360_conv_site_visits,
  c.conv_view_products AS cm360_conv_view_products,
  c.conv_add_to_carts AS cm360_conv_add_to_carts,
  c.conv_begin_checkouts AS cm360_conv_begin_checkouts,
  c.conv_purchases AS cm360_conv_purchases,
  c.source_table_name AS cm360_source_table_name,
  c.source_exported_at AS cm360_source_exported_at,
  c.data_refresh_date AS cm360_data_refresh_date,
  c.staged_at AS cm360_staged_at
FROM dcm_detail AS d
FULL OUTER JOIN conversion_detail AS c
  ON CONCAT(
    LOWER(TRIM(d._package_id)), '|',
    FORMAT_DATE('%F', d._date), '|',
    LOWER(TRIM(d._placement_id)), '|',
    LOWER(TRIM(d._creative_name))
  ) = c.model_detail_key
  AND c.model_detail_join_status = 'unique_detail_match';
