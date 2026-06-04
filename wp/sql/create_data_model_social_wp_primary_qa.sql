-- @description: Exposes a reporting-shaped QA social candidate from the
--               WP-first shared-source view so Apollo channel effects can be
--               reviewed without changing master_stg.data_model production.
-- @sources:     repo_stg.stg__crossplatform_wp_primary_qa
-- @output:      master_stg.data_model_social_wp_primary_qa.
-- @safety:      QA view only; safe to delete after Apollo impact review.

CREATE OR REPLACE VIEW `looker-studio-pro-452620.master_stg.data_model_social_wp_primary_qa`
OPTIONS (
  description = "QA-only reporting-shaped social candidate built by wp/sql/create_data_model_social_wp_primary_qa.sql. Shows campaign-grain Apollo rows using Paid Search/Online Video classification and WP precedence; cross-campaign ad-ID conflicts are included and flagged pending source-owner review. Production is unchanged; safe to delete after review by the model owner."
) AS
SELECT
  -- QA PURPOSE: Review the social branch delta before production model edits.
  -- CHANGE: This file applies WP campaign-marker classifications and
  --         WP-first values to campaign-grain records, including flagged
  --         pending-review conflicts; production remains unchanged.
  -- CLEANUP: Safe to delete after review by the master data model owner.
  'social' AS qa_row_type,
  IF(wp_record_source LIKE 'wp%', 'wp_search_data_template', 'social') AS qa_row_data_source_primary,
  CASE
    WHEN wp_record_source = 'wp_primary_with_standard_fallback' THEN 'wp_search_data_template | social'
    WHEN wp_record_source = 'wp_only' THEN 'wp_search_data_template'
    ELSE 'social'
  END AS qa_row_data_sources_available,
  COALESCE(NULLIF(wp_publication_status, ''), 'ok') AS qa_row_data_issue_category,
  CONCAT(
    'social:',
    LOWER(platform),
    ':',
    CAST(campaign_id AS STRING),
    ':',
    CAST(ad_group_id AS STRING)
  ) AS `_package_id`,
  date_day AS `_date`,
  IF(REGEXP_CONTAINS(UPPER(COALESCE(account_name, '')), r'APOLLO'), 'Apollo', account_name) AS `_advertiser`,
  campaign_name AS `_campaign_name`,
  ad_group_name AS `_package_name`,
  CAST(ad_id AS STRING) AS `_placement_id`,
  ad_name AS `_placement_name`,
  CASE
    WHEN wp_classification_source = 'campaign_marker_youtube'
      OR wp_media_name = 'Online Video' THEN 'YT'
    ELSE UPPER(platform)
  END AS `_supplier_code`,
  CASE
    WHEN wp_classification_source = 'campaign_marker_youtube'
      OR wp_media_name = 'Online Video' THEN 'Youtube'
    ELSE platform
  END AS `_supplier_name`,
  COALESCE(wp_channel, CONCAT('social_', LOWER(platform))) AS `_channel`,
  COALESCE(wp_channel_group, 'social') AS `_channel_group`,
  COALESCE(wp_media_name, 'Social') AS `_media_name`,
  COALESCE(wp_ADIF_channel, 'Social') AS `ADIF_channel`,
  spend AS `_spend`,
  CAST(impressions AS FLOAT64) AS `_impressions`,
  CAST(clicks AS FLOAT64) AS `_clicks`,
  COALESCE(CAST(video_view AS FLOAT64), CAST(video_play AS FLOAT64)) AS `_video_views`,
  wp_creative_img AS `_creative_img`,
  platform AS `s_platform`,
  campaign_id AS `s_campaign_id`,
  ad_group_id AS `s_ad_group_id`,
  ad_id AS `s_ad_id`,
  account_name AS `s_account_name`,
  spend AS `s_spend`,
  CAST(impressions AS FLOAT64) AS `s_impressions`,
  CAST(clicks AS FLOAT64) AS `s_clicks`,
  ad_name AS `s_creative_name`,
  wp_creative_img AS `man_creative_img`,
  wp_classification_source AS `s_channel_classification_source`,
  wp_record_source AS `s_record_source`,
  wp_fallback_fields AS `s_fallback_fields`,
  wp_source_sheet_url AS `s_source_sheet_url`,
  wp_loaded_at AS `s_loaded_at`
FROM `looker-studio-pro-452620.repo_stg.stg__crossplatform_wp_primary_qa`;
