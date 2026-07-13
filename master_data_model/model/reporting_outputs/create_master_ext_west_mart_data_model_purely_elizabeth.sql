-- Build the Purely Elizabeth reporting view used by the west-region dashboard.
--
-- Purpose:
--   Preserve the advertiser-specific output schema while presenting planned
--   spend and impressions as delivery values for linear media and QUAN rows.
--
-- Safe modification:
--   Keep the advertiser filter and column order stable. Changes to the metric
--   override condition should be validated against the live view before deploy.

CREATE OR REPLACE VIEW `looker-studio-pro-452620.master_ext_west.mart_data_model_purelyElizabeth` AS
SELECT
  -- LIVE VIEW NOTE: Purely Elizabeth reporting output. Linear media and QUAN
  -- rows intentionally publish planned spend and impressions in the delivery
  -- fields; all other rows preserve the underlying delivered values.
  `_package_id`,
  `_date`,
  `_start_date`,
  `_end_date`,
  `_advertiser_short_name`,
  `_advertiser`,
  `_campaign_name`,
  `_package_type`,
  `_package_name`,
  `_package_name_friendly`,
  `_placement_id`,
  `_placement_name`,
  `_supplier_code`,
  `_supplier_name`,
  `_supplier_logo`,
  `_channel`,
  `_channel_group`,
  ADIF_channel AS `_channel_custom`,
  initiative AS `_tactic`,
  `_media_name`,
  `_planned_spend`,
  `_planned_impressions`,
  `_creative_img`,
  CASE
    WHEN `_channel_group` = 'linear'
      OR UPPER(TRIM(COALESCE(`_supplier_code`, ''))) = 'QUAN'
      THEN `_planned_spend`
    ELSE `_spend`
  END AS `_spend`,
  CASE
    WHEN `_channel_group` = 'linear'
      OR UPPER(TRIM(COALESCE(`_supplier_code`, ''))) = 'QUAN'
      THEN `_planned_impressions`
    ELSE `_impressions`
  END AS `_impressions`,
  `_clicks`,
  `_video_plays`,
  `_video_views`,
  `_video_comps`,
  `_creative_name`,
  p_planned_amount_doNotSum,
  p_planned_impressions_doNotSum,
  fpd_creative
FROM `looker-studio-pro-452620.master_ext_west.data_model`
WHERE `_advertiser` = 'Purely Elizabeth';
