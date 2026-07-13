-- Isolated V3 candidate that replaces Sheet-based RTL conversions with direct CM360 history.
--
-- Reads: master_stg.data_model_v3 and master_stg.rtl_cm360_direct_conversions.
-- Produces: master_stg.data_model_v3_cm360_direct_qa for review only.
-- Safe use: build and validate this candidate before applying the same post-processing
--            step to the production V3 builder. Conversion-only records remain separate
--            from delivery data; they never inherit delivery metrics through a broad join.

CREATE OR REPLACE TABLE `looker-studio-pro-452620.master_stg.data_model_v3_cm360_direct_qa`
CLUSTER BY _advertiser AS
WITH base_v3 AS (
  SELECT *
  FROM `looker-studio-pro-452620.master_stg.data_model_v3`
  WHERE qa_v3_source_detail_type != 'conversion_activity'
),
cm360_by_detail AS (
  SELECT
    model_detail_key,
    ANY_VALUE(date) AS conversion_date,
    ANY_VALUE(package_id) AS package_id,
    ANY_VALUE(placement_id) AS placement_id,
    ANY_VALUE(creative) AS creative,
    ANY_VALUE(advertiser) AS advertiser,
    ANY_VALUE(campaign) AS campaign,
    ANY_VALUE(site) AS site,
    ANY_VALUE(package_roadblock) AS package_roadblock,
    ARRAY_AGG(DISTINCT NULLIF(TRIM(activity_group), '') IGNORE NULLS ORDER BY NULLIF(TRIM(activity_group), '')) AS conv_activity_groups,
    ARRAY_AGG(DISTINCT NULLIF(TRIM(activity), '') IGNORE NULLS ORDER BY NULLIF(TRIM(activity), '')) AS conv_activities,
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
    COUNT(*) AS conv_source_activity_record_count,
    ARRAY_AGG(DISTINCT source_table_name ORDER BY source_table_name) AS source_table_names,
    MAX(source_exported_at) AS conv_source_exported_at,
    MAX(data_refresh_date) AS conv_data_refresh_date,
    MAX(staged_at) AS conv_staged_at
  FROM `looker-studio-pro-452620.master_stg.rtl_cm360_direct_conversions`
  GROUP BY model_detail_key
),
dcm_detail_counts AS (
  SELECT
    CONCAT(
      LOWER(TRIM(_package_id)), '|', FORMAT_DATE('%F', _date), '|',
      LOWER(TRIM(_placement_id)), '|', LOWER(TRIM(_creative_name))
    ) AS model_detail_key,
    COUNT(*) AS dcm_detail_match_count
  FROM base_v3
  WHERE qa_v3_source_detail_type = 'dcm'
  GROUP BY model_detail_key
),
cm360_ready AS (
  SELECT
    c.*,
    COALESCE(d.dcm_detail_match_count, 0) AS dcm_detail_match_count,
    CASE
      WHEN COALESCE(d.dcm_detail_match_count, 0) = 1 THEN 'matched_unique'
      WHEN COALESCE(d.dcm_detail_match_count, 0) = 0 THEN 'conversion_only'
      ELSE 'ambiguous_delivery_match'
    END AS conv_model_detail_join_status
  FROM cm360_by_detail AS c
  LEFT JOIN dcm_detail_counts AS d USING (model_detail_key)
),
base_with_direct AS (
  SELECT
    b.* REPLACE (
      IF(c.model_detail_key IS NOT NULL, 'dcm | cm360_direct_conversion', b.qa_data_source) AS qa_data_source,
      IF(c.model_detail_key IS NOT NULL, c.conv_staged_at, b.qa_data_source_refresh_at) AS qa_data_source_refresh_at,
      IF(c.model_detail_key IS NOT NULL, 'dcm | cm360_direct_conversion', b.qa_row_data_sources_available) AS qa_row_data_sources_available,
      IF(c.model_detail_key IS NOT NULL, CAST(NULL AS STRING), b.conv_activity) AS conv_activity,
      IF(c.model_detail_key IS NOT NULL, CAST(c.conv_total_conversions AS INT64), b.conv_total_conversions) AS conv_total_conversions,
      IF(c.model_detail_key IS NOT NULL, CAST(NULL AS FLOAT64), b.conv_source_impressions) AS conv_source_impressions,
      IF(c.model_detail_key IS NOT NULL, CAST(NULL AS FLOAT64), b.conv_source_clicks) AS conv_source_clicks,
      IF(c.model_detail_key IS NOT NULL, c.conv_source_activity_record_count, b.conv_source_row_count) AS conv_source_row_count,
      IF(c.model_detail_key IS NOT NULL, c.site, b.conv_site) AS conv_site,
      IF(c.model_detail_key IS NOT NULL, c.site, b.conv_site_cm360) AS conv_site_cm360,
      IF(c.model_detail_key IS NOT NULL, c.campaign, b.conv_campaign) AS conv_campaign,
      IF(c.model_detail_key IS NOT NULL, c.package_roadblock, b.conv_package_roadblock) AS conv_package_roadblock,
      IF(c.model_detail_key IS NOT NULL, c.creative, b.conv_creative) AS conv_creative,
      IF(c.model_detail_key IS NOT NULL, c.conv_staged_at, b.conv_loaded_at) AS conv_loaded_at,
      IF(c.model_detail_key IS NOT NULL, CAST(NULL AS STRING), b.conv_source_sheet_id) AS conv_source_sheet_id,
      IF(c.model_detail_key IS NOT NULL, CAST(NULL AS STRING), b.conv_source_sheet_tab) AS conv_source_sheet_tab,
      IF(c.model_detail_key IS NOT NULL, CAST(NULL AS STRING), b.conv_source_sheet_gid) AS conv_source_sheet_gid
    ),
    c.advertiser AS conv_advertiser,
    c.conv_activity_groups,
    c.conv_activities,
    c.placement_id AS conv_placement,
    c.conv_click_through_conversions,
    c.conv_view_through_conversions,
    c.conv_total_revenue,
    c.conv_click_through_revenue,
    c.conv_view_through_revenue,
    c.conv_site_visits,
    c.conv_view_products,
    c.conv_add_to_carts,
    c.conv_begin_checkouts,
    c.conv_purchases,
    c.model_detail_key AS conv_model_detail_key,
    c.conv_model_detail_join_status,
    c.dcm_detail_match_count AS conv_model_detail_match_count,
    c.conv_source_activity_record_count,
    c.source_table_names AS conv_source_table_names,
    c.conv_source_exported_at,
    c.conv_data_refresh_date,
    c.conv_staged_at,
    TO_HEX(SHA256(CONCAT('base|', TO_JSON_STRING(b)))) AS qa_cm360_record_key
  FROM base_v3 AS b
  LEFT JOIN cm360_ready AS c
    ON b.qa_v3_source_detail_type = 'dcm'
   AND CONCAT(
     LOWER(TRIM(b._package_id)), '|', FORMAT_DATE('%F', b._date), '|',
     LOWER(TRIM(b._placement_id)), '|', LOWER(TRIM(b._creative_name))
   ) = c.model_detail_key
   AND c.conv_model_detail_join_status = 'matched_unique'
),
conversion_only_rows AS (
  SELECT
    b.* REPLACE (
      'source_actual' AS qa_v3_row_type,
      'package_date_placement_creative_conversion' AS qa_v3_metric_grain,
      'cm360_direct_conversion' AS qa_v3_source_detail_type,
      c.advertiser AS qa_v3_ad_name,
      'Direct CM360 conversions without a unique delivery-row match' AS qa_v3_metric_behavior,
      'digital' AS qa_media_data_type,
      'cm360_direct_conversion' AS qa_row_data_source_primary,
      ARRAY_TO_STRING(c.source_table_names, ' | ') AS qa_data_source,
      c.conv_staged_at AS qa_data_source_refresh_at,
      'cm360_direct_conversion' AS qa_row_data_sources_available,
      c.conv_model_detail_join_status AS qa_data_issues,
      c.package_id AS _package_id,
      c.conversion_date AS _date,
      c.conversion_date AS _start_date,
      c.conversion_date AS _end_date,
      c.advertiser AS _advertiser_name,
      c.advertiser AS _advertiser_short_name,
      c.advertiser AS _advertiser,
      c.campaign AS _campaign_name,
      c.campaign AS _campaign_friendly,
      c.package_roadblock AS _package_name,
      c.package_roadblock AS _package_name_friendly,
      c.placement_id AS _placement_id,
      c.placement_id AS _placement_name,
      c.creative AS _creative_name,
      c.package_roadblock AS initiative,
      CAST(NULL AS STRING) AS conv_activity,
      CAST(c.conv_total_conversions AS INT64) AS conv_total_conversions,
      CAST(NULL AS FLOAT64) AS conv_source_impressions,
      CAST(NULL AS FLOAT64) AS conv_source_clicks,
      c.conv_source_activity_record_count AS conv_source_row_count,
      c.site AS conv_site,
      c.site AS conv_site_cm360,
      c.campaign AS conv_campaign,
      c.package_roadblock AS conv_package_roadblock,
      c.creative AS conv_creative,
      c.conv_staged_at AS conv_loaded_at,
      CAST(NULL AS STRING) AS conv_source_sheet_id,
      CAST(NULL AS STRING) AS conv_source_sheet_tab,
      CAST(NULL AS STRING) AS conv_source_sheet_gid
    ),
    c.advertiser AS conv_advertiser,
    c.conv_activity_groups,
    c.conv_activities,
    c.placement_id AS conv_placement,
    c.conv_click_through_conversions,
    c.conv_view_through_conversions,
    c.conv_total_revenue,
    c.conv_click_through_revenue,
    c.conv_view_through_revenue,
    c.conv_site_visits,
    c.conv_view_products,
    c.conv_add_to_carts,
    c.conv_begin_checkouts,
    c.conv_purchases,
    c.model_detail_key AS conv_model_detail_key,
    c.conv_model_detail_join_status,
    c.dcm_detail_match_count AS conv_model_detail_match_count,
    c.conv_source_activity_record_count,
    c.source_table_names AS conv_source_table_names,
    c.conv_source_exported_at,
    c.conv_data_refresh_date,
    c.conv_staged_at,
    TO_HEX(SHA256(CONCAT('cm360|', c.model_detail_key))) AS qa_cm360_record_key
  FROM cm360_ready AS c
  LEFT JOIN base_v3 AS b ON FALSE
  WHERE c.conv_model_detail_join_status != 'matched_unique'
)
-- Direct CM360 conversion fields preserve source dimensions and activity metrics at detail grain.
SELECT * FROM base_with_direct
UNION ALL
SELECT * FROM conversion_only_rows;

ALTER TABLE `looker-studio-pro-452620.master_stg.data_model_v3_cm360_direct_qa`
SET OPTIONS (
  description = 'Isolated V3 QA candidate that replaces Sheet conversion_activity rows with direct CM360 RTL history. Built by model/final_model/create_master_stg_data_model_v3_cm360_direct_qa.sql. Safe to delete after production validation; cleanup owner: Master Data Model migration review.'
);
