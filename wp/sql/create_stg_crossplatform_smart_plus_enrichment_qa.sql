-- Purpose: Preview TikTok ADIF Smart+ hierarchy enrichment without changing
-- production shared-social staging or any downstream master-model object.
-- Reads:   Current production shared-social staging and current TikTok ADIF
--          Smart+ creative history.
-- Produces: One isolated QA table with the production schema and grain.
-- Safety:  This file replaces only the named QA table. Delete it after review.

CREATE OR REPLACE TABLE
  `looker-studio-pro-452620.repo_stg.stg__olipop__crossplatform_smart_plus_enrichment_qa`
OPTIONS (
  description = "QA preview of generic TikTok ADIF Smart+ hierarchy enrichment. Compared with production shared-social staging, it fills only blank account, campaign, ad-group, and ad-name fields from the latest Smart+ creative record while preserving delivery ad IDs and metrics. Safe to delete after review; cleanup owner is the master-data-model maintainer."
) AS
WITH
tiktok_adif_smart_plus_creative AS (
  SELECT
    CAST(creative_id AS STRING) AS creative_id,
    CAST(advertiser_id AS STRING) AS advertiser_id,
    CAST(campaign_id AS STRING) AS campaign_id,
    CAST(adgroup_id AS STRING) AS ad_group_id,
    campaign_name,
    adgroup_name AS ad_group_name,
    creative_name AS ad_name
  FROM `giant-spoon-299605.tiktok_ads_adif.creative_history`
  WHERE campaign_automation_type = 'UPGRADED_SMART_PLUS_CREATIVE'
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY creative_id
    ORDER BY updated_at DESC, _fivetran_synced DESC
  ) = 1
)
SELECT
  -- QA PURPOSE: Test generic Smart+ hierarchy enrichment against the exact
  -- current production rows. This changes identity fields only, is safe to
  -- delete after review, and is owned by the master-data-model maintainer.
  production.* REPLACE (
    COALESCE(NULLIF(production.account_id, ''), smart_plus.advertiser_id) AS account_id,
    COALESCE(NULLIF(production.campaign_id, ''), smart_plus.campaign_id) AS campaign_id,
    COALESCE(NULLIF(production.campaign_name, ''), smart_plus.campaign_name) AS campaign_name,
    COALESCE(NULLIF(production.ad_group_id, ''), smart_plus.ad_group_id) AS ad_group_id,
    COALESCE(NULLIF(production.ad_group_name, ''), smart_plus.ad_group_name) AS ad_group_name,
    COALESCE(NULLIF(production.ad_name, ''), smart_plus.ad_name) AS ad_name
  )
FROM `looker-studio-pro-452620.repo_stg.stg__olipop__crossplatform_raw_tbl` AS production
LEFT JOIN tiktok_adif_smart_plus_creative AS smart_plus
  ON production.source_relation = 'tiktok_ads_adif'
 AND CAST(production.ad_id AS STRING) = smart_plus.creative_id;
