-- Builds the package-level lookup table used by the Manual Data Editor loader.
--
-- Purpose:
--   Keep the R loader simple and fast by materializing the package rollup in
--   BigQuery before the loader reads it. This avoids planning the full reporting
--   mart view plus another package aggregation during every Sheet refresh.
--
-- Safe use:
--   Run this before load_manual_package_edits.R. It replaces only the lookup
--   table below; it does not modify manual edit landing tables or the Sheet.

CREATE OR REPLACE TABLE `looker-studio-pro-452620.master_stg.manual_package_editor_package_lookup`
OPTIONS (
  description = 'Package-level source-baseline lookup snapshot for the Manual Data Editor loader. Built from non-manual rows in the materialized master model support table with reporting-mart low-signal DCM and excluded social campaign removals, then joined to PRISMA package totals. Safe to refresh before each manual editor load.'
) AS
WITH
prisma_package_totals AS (
  SELECT
    package_id,
    ARRAY_AGG(planned_amount IGNORE NULLS ORDER BY report_date DESC NULLS LAST LIMIT 1)[SAFE_OFFSET(0)] AS prisma_planned_amount,
    ARRAY_AGG(planned_impressions IGNORE NULLS ORDER BY report_date DESC NULLS LAST LIMIT 1)[SAFE_OFFSET(0)] AS prisma_planned_impressions,
    ARRAY_AGG(planned_units IGNORE NULLS ORDER BY report_date DESC NULLS LAST LIMIT 1)[SAFE_OFFSET(0)] AS prisma_planned_units,
    ARRAY_AGG(unit_type IGNORE NULLS ORDER BY report_date DESC NULLS LAST LIMIT 1)[SAFE_OFFSET(0)] AS prisma_unit_type,
    ARRAY_AGG(payable_rate IGNORE NULLS ORDER BY report_date DESC NULLS LAST LIMIT 1)[SAFE_OFFSET(0)] AS prisma_rate
  FROM `looker-studio-pro-452620.20250327_data_model.prisma_expanded_full`
  WHERE package_type != 'Child'
    AND start_date >= DATE '2025-01-01'
    AND package_id IS NOT NULL
  GROUP BY package_id
),

mart_source AS (
  SELECT *
  FROM `looker-studio-pro-452620.master_stg.data_model_clustered_by_advertiser_qa`
  WHERE NOT CONTAINS_SUBSTR(COALESCE(`qa_data_issues`, ''), 'low_signal_dcm')
    AND NOT REGEXP_CONTAINS(LOWER(COALESCE(`_campaign_name`, '')), r'1000heads')
    AND COALESCE(`qa_manual_edit_flag`, FALSE) = FALSE
    AND COALESCE(`qa_row_data_source_primary`, '') != 'manual_package_edits'
    AND `_package_id` IS NOT NULL
),

mart_packages AS (
  SELECT
    `_package_id` AS package_id,
    ARRAY_AGG(`_advertiser` IGNORE NULLS ORDER BY `_date` DESC LIMIT 1)[SAFE_OFFSET(0)] AS advertiser_name,
    ARRAY_AGG(`_advertiser_short_name` IGNORE NULLS ORDER BY `_date` DESC LIMIT 1)[SAFE_OFFSET(0)] AS advertiser_short_name,
    ARRAY_AGG(`_campaign_name` IGNORE NULLS ORDER BY `_date` DESC LIMIT 1)[SAFE_OFFSET(0)] AS campaign_name,
    ARRAY_AGG(`_campaign_friendly` IGNORE NULLS ORDER BY `_date` DESC LIMIT 1)[SAFE_OFFSET(0)] AS campaign_friendly,
    ARRAY_AGG(`_product_code` IGNORE NULLS ORDER BY `_date` DESC LIMIT 1)[SAFE_OFFSET(0)] AS product_code,
    ARRAY_AGG(`_product_name` IGNORE NULLS ORDER BY `_date` DESC LIMIT 1)[SAFE_OFFSET(0)] AS product_name,
    ARRAY_AGG(`_package_type` IGNORE NULLS ORDER BY `_date` DESC LIMIT 1)[SAFE_OFFSET(0)] AS package_type,
    ARRAY_AGG(`_package_name` IGNORE NULLS ORDER BY `_date` DESC LIMIT 1)[SAFE_OFFSET(0)] AS package_name,
    ARRAY_AGG(`_package_name_friendly` IGNORE NULLS ORDER BY `_date` DESC LIMIT 1)[SAFE_OFFSET(0)] AS package_name_friendly,
    ARRAY_AGG(`initiative` IGNORE NULLS ORDER BY `_date` DESC LIMIT 1)[SAFE_OFFSET(0)] AS initiative,
    ARRAY_AGG(`ADIF_channel` IGNORE NULLS ORDER BY `_date` DESC LIMIT 1)[SAFE_OFFSET(0)] AS ADIF_channel,
    ARRAY_AGG(`_placement_id` IGNORE NULLS ORDER BY `_date` DESC LIMIT 1)[SAFE_OFFSET(0)] AS placement_id,
    ARRAY_AGG(`_placement_name` IGNORE NULLS ORDER BY `_date` DESC LIMIT 1)[SAFE_OFFSET(0)] AS placement_name,
    ARRAY_AGG(`_supplier_code` IGNORE NULLS ORDER BY `_date` DESC LIMIT 1)[SAFE_OFFSET(0)] AS supplier_code,
    ARRAY_AGG(`_supplier_name` IGNORE NULLS ORDER BY `_date` DESC LIMIT 1)[SAFE_OFFSET(0)] AS supplier_name,
    ARRAY_AGG(`_supplier_logo` IGNORE NULLS ORDER BY `_date` DESC LIMIT 1)[SAFE_OFFSET(0)] AS supplier_logo,
    ARRAY_AGG(`p_buy_type` IGNORE NULLS ORDER BY `_date` DESC LIMIT 1)[SAFE_OFFSET(0)] AS p_buy_type,
    ARRAY_AGG(`p_buy_category` IGNORE NULLS ORDER BY `_date` DESC LIMIT 1)[SAFE_OFFSET(0)] AS p_buy_category,
    ARRAY_AGG(`_channel` IGNORE NULLS ORDER BY `_date` DESC LIMIT 1)[SAFE_OFFSET(0)] AS channel,
    ARRAY_AGG(`qa_channel_raw` IGNORE NULLS ORDER BY `_date` DESC LIMIT 1)[SAFE_OFFSET(0)] AS channel_raw,
    ARRAY_AGG(`_channel_group` IGNORE NULLS ORDER BY `_date` DESC LIMIT 1)[SAFE_OFFSET(0)] AS channel_group,
    ARRAY_AGG(`_media_name` IGNORE NULLS ORDER BY `_date` DESC LIMIT 1)[SAFE_OFFSET(0)] AS media_name,
    ARRAY_AGG(`p_cost_method` IGNORE NULLS ORDER BY `_date` DESC LIMIT 1)[SAFE_OFFSET(0)] AS p_cost_method,
    ARRAY_AGG(`p_planned_amount_doNotSum` IGNORE NULLS ORDER BY `_date` DESC LIMIT 1)[SAFE_OFFSET(0)] AS mart_p_planned_amount_doNotSum,
    ARRAY_AGG(`p_planned_impressions_doNotSum` IGNORE NULLS ORDER BY `_date` DESC LIMIT 1)[SAFE_OFFSET(0)] AS mart_p_planned_impressions_doNotSum,
    ARRAY_AGG(`p_planned_units_doNotSum` IGNORE NULLS ORDER BY `_date` DESC LIMIT 1)[SAFE_OFFSET(0)] AS mart_p_planned_units_doNotSum,
    ARRAY_AGG(`p_unit_type` IGNORE NULLS ORDER BY `_date` DESC LIMIT 1)[SAFE_OFFSET(0)] AS mart_p_unit_type,
    ARRAY_AGG(`p_rate` IGNORE NULLS ORDER BY `_date` DESC LIMIT 1)[SAFE_OFFSET(0)] AS mart_p_rate,
    STRING_AGG(DISTINCT `qa_row_data_source_primary`, ' | ' ORDER BY `qa_row_data_source_primary`) AS qa_row_data_source_primary,
    COUNT(*) AS current_row_count,
    MIN(`_date`) AS current_first_date,
    MAX(`_date`) AS current_last_date,
    CASE
      WHEN LOGICAL_OR(`qa_media_data_type` = 'social')
        AND ARRAY_AGG(`_end_date` IGNORE NULLS ORDER BY `_date` DESC LIMIT 1)[SAFE_OFFSET(0)] IS NULL
        THEN CAST(NULL AS DATE)
      ELSE ARRAY_AGG(`_start_date` IGNORE NULLS ORDER BY `_date` DESC LIMIT 1)[SAFE_OFFSET(0)]
    END AS current_flight_start_date,
    CASE
      WHEN LOGICAL_OR(`qa_media_data_type` = 'social')
        AND ARRAY_AGG(`_end_date` IGNORE NULLS ORDER BY `_date` DESC LIMIT 1)[SAFE_OFFSET(0)] IS NULL
        THEN CAST(NULL AS DATE)
      ELSE ARRAY_AGG(`_end_date` IGNORE NULLS ORDER BY `_date` DESC LIMIT 1)[SAFE_OFFSET(0)]
    END AS current_flight_end_date,
    SUM(COALESCE(
      CASE
        WHEN `qa_media_data_type` = 'tv' THEN `tv_net_cost`
        WHEN `qa_media_data_type` = 'social' THEN `s_spend`
        WHEN REGEXP_CONTAINS(COALESCE(`qa_row_data_sources_available`, ''), r'prisma') THEN
          COALESCE(NULLIF(`fpd_spend`, 0), `dcm_daily_recalculated_cost`)
        ELSE NULL
      END,
      0
    )) AS current_spend,
    SUM(COALESCE(
      CASE
        WHEN `qa_media_data_type` = 'tv' THEN CAST(`tv_net_impressions` AS FLOAT64)
        WHEN `qa_media_data_type` = 'social' THEN `s_impressions`
        WHEN REGEXP_CONTAINS(COALESCE(`qa_row_data_sources_available`, ''), r'prisma') THEN
          COALESCE(NULLIF(`fpd_impressions`, 0), CAST(`dcm_impressions` AS FLOAT64))
        ELSE NULL
      END,
      0
    )) AS current_impressions,
    SUM(COALESCE(
      CASE
        WHEN `qa_media_data_type` = 'tv' THEN `tv_net_cost`
        WHEN `qa_media_data_type` = 'social' THEN `s_pacing_planned_spend`
        ELSE NULL
      END,
      0
    )) AS current_planned_spend_fallback,
    SUM(COALESCE(
      CASE
        WHEN `qa_media_data_type` = 'tv' THEN CAST(`tv_net_impressions` AS FLOAT64)
        ELSE NULL
      END,
      0
    )) AS current_planned_impressions_fallback,
    SUM(COALESCE(
      CASE
        WHEN `qa_media_data_type` = 'social' THEN `s_clicks`
        WHEN REGEXP_CONTAINS(COALESCE(`qa_row_data_sources_available`, ''), r'prisma') THEN
          COALESCE(`fpd_clicks`, CAST(`dcm_clicks` AS FLOAT64))
        ELSE NULL
      END,
      0
    )) AS current_clicks,
    SUM(COALESCE(
      CASE
        WHEN `qa_media_data_type` = 'social' THEN `s_video_plays`
        WHEN REGEXP_CONTAINS(COALESCE(`qa_row_data_sources_available`, ''), r'prisma') THEN
          CAST(`dcm_video_plays` AS FLOAT64)
        ELSE NULL
      END,
      0
    )) AS current_video_plays,
    SUM(COALESCE(
      CASE
        WHEN `qa_media_data_type` = 'social' THEN `s_video_comps`
        WHEN REGEXP_CONTAINS(COALESCE(`qa_row_data_sources_available`, ''), r'prisma') THEN
          CAST(`dcm_video_comps` AS FLOAT64)
        ELSE NULL
      END,
      0
    )) AS current_video_comps
  FROM mart_source
  GROUP BY package_id
)

SELECT
  -- LIVE TABLE NOTE: Package-level source-baseline lookup for the Manual Data
  -- Editor. It excludes rows already produced by manual package edits so user
  -- edits cannot feed back as the next refresh baseline.
  m.* EXCEPT(
    mart_p_planned_amount_doNotSum,
    mart_p_planned_impressions_doNotSum,
    mart_p_planned_units_doNotSum,
    mart_p_unit_type,
    mart_p_rate,
    current_planned_spend_fallback,
    current_planned_impressions_fallback
  ),
  COALESCE(p.prisma_planned_amount, m.mart_p_planned_amount_doNotSum) AS p_planned_amount_doNotSum,
  COALESCE(p.prisma_planned_impressions, m.mart_p_planned_impressions_doNotSum) AS p_planned_impressions_doNotSum,
  COALESCE(p.prisma_planned_units, m.mart_p_planned_units_doNotSum) AS p_planned_units_doNotSum,
  COALESCE(p.prisma_unit_type, m.mart_p_unit_type) AS p_unit_type,
  COALESCE(p.prisma_rate, m.mart_p_rate) AS p_rate,
  COALESCE(p.prisma_planned_amount, NULLIF(m.current_planned_spend_fallback, 0), m.mart_p_planned_amount_doNotSum) AS current_planned_spend,
  COALESCE(p.prisma_planned_impressions, NULLIF(m.current_planned_impressions_fallback, 0), m.mart_p_planned_impressions_doNotSum) AS current_planned_impressions
FROM mart_packages AS m
LEFT JOIN prisma_package_totals AS p
  ON m.package_id = p.package_id
ORDER BY advertiser_name, package_type, channel, campaign_name, initiative, supplier_code, supplier_name, package_name, package_id;
