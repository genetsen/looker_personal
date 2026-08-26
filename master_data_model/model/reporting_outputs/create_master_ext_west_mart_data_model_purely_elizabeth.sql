-- Build the Purely Elizabeth reporting view used by the west-region dashboard.
--
-- Purpose:
--   Preserve the advertiser-specific output schema while presenting planned
--   spend and impressions as delivery values for linear media and QUAN rows,
--   with creative benchmark rates from the linked source Sheet.
--
-- Safe modification:
--   Keep the advertiser filter and established column order stable. Benchmark
--   fields are non-summable context joined by tactic and exact raw creative.

CREATE OR REPLACE VIEW `looker-studio-pro-452620.master_ext_west.mart_data_model_purelyElizabeth` AS
WITH creative_benchmarks AS (
  SELECT
    UPPER(TRIM(initiative)) AS initiative_key,
    UPPER(TRIM(COALESCE(creative_name, ''))) AS creative_key,
    IF(COUNT(*) = 1, ANY_VALUE(bm_ctr_range_min), ERROR('Conflicting PE CTR minimum benchmark keys'))
      AS bm_ctr_range_min,
    IF(COUNT(*) = 1, ANY_VALUE(bm_ctr_range_max), ERROR('Conflicting PE CTR maximum benchmark keys'))
      AS bm_ctr_range_max,
    IF(COUNT(*) = 1, ANY_VALUE(bm_social_er_bm), ERROR('Conflicting PE social engagement benchmark keys'))
      AS bm_social_er_bm,
    IF(COUNT(*) = 1, ANY_VALUE(bm_vcr_range_min), ERROR('Conflicting PE VCR minimum benchmark keys'))
      AS bm_vcr_range_min,
    IF(COUNT(*) = 1, ANY_VALUE(bm_vcr_range_max), ERROR('Conflicting PE VCR maximum benchmark keys'))
      AS bm_vcr_range_max
  FROM `looker-studio-pro-452620.master_stg.purely_elizabeth_creative_mapping_sheet`
  GROUP BY initiative_key, creative_key
)
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
  `_creative_name_raw`,
  `_creative_name`,
  p_planned_amount_doNotSum,
  p_planned_impressions_doNotSum,
  fpd_creative,
  creative_benchmarks.bm_ctr_range_min,
  creative_benchmarks.bm_ctr_range_max,
  creative_benchmarks.bm_social_er_bm,
  creative_benchmarks.bm_vcr_range_min,
  creative_benchmarks.bm_vcr_range_max
FROM `looker-studio-pro-452620.master_ext_west.data_model` AS report
LEFT JOIN creative_benchmarks
  ON UPPER(TRIM(report.initiative)) = creative_benchmarks.initiative_key
  AND UPPER(TRIM(COALESCE(report.`_creative_name_raw`, ''))) = creative_benchmarks.creative_key
WHERE report.`_advertiser` = 'Purely Elizabeth';
