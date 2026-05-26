-- @description: Exposes a reporting-shaped QA social candidate from the
--               APO-first shared-source view so Apollo channel effects can be
--               reviewed without changing master_stg.data_model production.
-- @sources:     repo_stg.stg__crossplatform_apo_primary_qa
-- @output:      master_stg.data_model_social_apo_primary_qa.
-- @safety:      QA view only; safe to delete after Apollo impact review.

CREATE OR REPLACE VIEW `looker-studio-pro-452620.master_stg.data_model_social_apo_primary_qa`
OPTIONS (
  description = "QA-only reporting-shaped social candidate built by apollo/sql/create_data_model_social_apo_primary_qa.sql. Shows unique publishable Apollo rows using Paid Search/Online Video classifications and APO precedence; conflicting daily-ad keys are held out. Production is unchanged; safe to delete after review by the model owner."
) AS
SELECT
  -- QA PURPOSE: Review the social branch delta before production model edits.
  -- CHANGE: This file applies APO campaign-marker classifications and
  --         APO-first values to publishable unique keys; the production
  --         master model remains unchanged.
  -- CLEANUP: Safe to delete after review by the master data model owner.
  'social' AS qa_row_type,
  IF(apo_record_source LIKE 'apo%', 'apo_search_data_template', 'social') AS qa_row_data_source_primary,
  CASE
    WHEN apo_record_source = 'apo_primary_with_standard_fallback' THEN 'apo_search_data_template | social'
    WHEN apo_record_source = 'apo_only' THEN 'apo_search_data_template'
    ELSE 'social'
  END AS qa_row_data_sources_available,
  COALESCE(NULLIF(apo_publication_status, ''), 'ok') AS qa_row_data_issue_category,
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
  UPPER(platform) AS `_supplier_code`,
  platform AS `_supplier_name`,
  COALESCE(apo_channel, CONCAT('social_', LOWER(platform))) AS `_channel`,
  COALESCE(apo_channel_group, 'social') AS `_channel_group`,
  COALESCE(apo_media_name, 'Social') AS `_media_name`,
  COALESCE(apo_ADIF_channel, 'Social') AS `ADIF_channel`,
  spend AS `_spend`,
  CAST(impressions AS FLOAT64) AS `_impressions`,
  CAST(clicks AS FLOAT64) AS `_clicks`,
  platform AS `s_platform`,
  campaign_id AS `s_campaign_id`,
  ad_group_id AS `s_ad_group_id`,
  ad_id AS `s_ad_id`,
  account_name AS `s_account_name`,
  spend AS `s_spend`,
  CAST(impressions AS FLOAT64) AS `s_impressions`,
  CAST(clicks AS FLOAT64) AS `s_clicks`,
  apo_creative_name AS `s_creative_name`,
  apo_creative_box_link AS `s_creative_box_link`,
  apo_classification_source AS `s_channel_classification_source`,
  apo_record_source AS `s_record_source`,
  apo_fallback_fields AS `s_fallback_fields`,
  apo_source_sheet_url AS `s_source_sheet_url`,
  apo_loaded_at AS `s_loaded_at`
FROM `looker-studio-pro-452620.repo_stg.stg__crossplatform_apo_primary_qa`;
