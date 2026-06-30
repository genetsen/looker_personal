-- Sample single-master hierarchy model for the creative-grain recovery.
--
-- Purpose:
--   Compare a one-table approach against the safer two-table approach.
--   Package rows carry package/date metrics. Detail rows carry delivery detail.
--   Package context on detail rows is explicitly marked doNotSum.

CREATE OR REPLACE VIEW `looker-studio-pro-452620.master_stg.data_model_detail_master_v3_sample` AS
SELECT
  'package' AS row_level,
  'package_date' AS metric_grain,
  `qa_row_data_source_primary` AS source_row_type,
  `_package_id`,
  `_date`,
  `_advertiser_name`,
  `_advertiser_short_name`,
  `_campaign_name`,
  `_campaign_friendly`,
  `_product_code`,
  `_product_name`,
  `_package_type`,
  `_package_name`,
  `_package_name_friendly`,
  initiative,
  ADIF_channel,
  `_placement_id`,
  `_placement_name`,
  CAST(NULL AS STRING) AS ad_name,
  CAST(NULL AS STRING) AS creative,
  CAST(NULL AS STRING) AS creative_git_link,
  `_supplier_code`,
  `_supplier_name`,
  `_supplier_logo`,
  p_buy_type,
  p_buy_category,
  `_channel`,
  qa_channel_raw,
  `_channel_group`,
  `_media_name`,
  `_planned_spend`,
  `_planned_impressions`,
  CAST(NULL AS FLOAT64) AS planned_daily_spend_doNotSum,
  CAST(NULL AS FLOAT64) AS planned_daily_impressions_doNotSum,
  `_spend`,
  `_impressions`,
  `_clicks`,
  `_video_plays`,
  `_video_comps`,
  qa_model_view_runtime_timestamp AS model_view_runtime_timestamp
FROM `looker-studio-pro-452620.master_stg.data_model_package_daily_v3_sample`

UNION ALL

SELECT
  'detail' AS row_level,
  CASE
    WHEN source_row_type = 'fpd_updated_package' THEN 'package_date_delivery'
    ELSE 'placement_creative_date'
  END AS metric_grain,
  source_row_type,
  `_package_id`,
  `_date`,
  `_advertiser_name`,
  `_advertiser_short_name`,
  `_campaign_name`,
  `_campaign_friendly`,
  `_product_code`,
  `_product_name`,
  `_package_type`,
  `_package_name`,
  `_package_name_friendly`,
  initiative,
  ADIF_channel,
  `_placement_id`,
  `_placement_name`,
  ad_name,
  creative,
  creative_git_link,
  `_supplier_code`,
  `_supplier_name`,
  `_supplier_logo`,
  p_buy_type,
  p_buy_category,
  `_channel`,
  qa_channel_raw,
  `_channel_group`,
  `_media_name`,
  CAST(NULL AS FLOAT64) AS `_planned_spend`,
  CAST(NULL AS FLOAT64) AS `_planned_impressions`,
  planned_daily_spend_doNotSum,
  planned_daily_impressions_doNotSum,
  `_spend`,
  `_impressions`,
  `_clicks`,
  `_video_plays`,
  `_video_comps`,
  model_view_runtime_timestamp
FROM `looker-studio-pro-452620.master_stg.data_model_delivery_detail_v3_sample`;
