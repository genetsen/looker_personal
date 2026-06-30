-- @description: Builds a QA shared-social candidate where the WP Search Data
--               Template is primary for Apollo campaign/ad-group/ad cells and
--               the existing
--               production raw source fills absent rows or fields.
-- @sources:     repo_stg.stg__olipop__crossplatform_raw_tbl,
--               repo_stg.stg__wp__search_data_template_daily
-- @output:      repo_stg.stg__crossplatform_wp_primary_qa at
--               date/platform/campaign/ad-group/ad grain; conflicting
--               cross-campaign ad identities remain flagged pending review.
-- @safety:      QA view only. It does not replace production shared staging.
--               Safe to delete after WP source-precedence review.

CREATE OR REPLACE VIEW `looker-studio-pro-452620.repo_stg.stg__crossplatform_wp_primary_qa`
OPTIONS (
  description = "QA-only WP-first shared social candidate built by wp/sql/create_stg_crossplatform_wp_primary_qa.sql. Uses campaign-grain WP records first with production raw fallback; cross-campaign ad-ID conflicts are included and flagged pending source-owner review. Production is unchanged; safe to delete after review by the model owner."
) AS
WITH
standard_extended AS (
  SELECT
    s.*
  FROM `looker-studio-pro-452620.repo_stg.stg__olipop__crossplatform_raw_tbl` AS s
),
standard_apollo AS (
  SELECT *
  FROM standard_extended
  WHERE REGEXP_CONTAINS(UPPER(COALESCE(account_name, '')), r'APOLLO')
),
standard_non_apollo AS (
  SELECT *
  FROM standard_extended
  WHERE NOT REGEXP_CONTAINS(UPPER(COALESCE(account_name, '')), r'APOLLO')
),
wp_publishable AS (
  SELECT *
  FROM `looker-studio-pro-452620.repo_stg.stg__wp__search_data_template_daily`
  WHERE wp_publication_status IN ('publish', 'publish_pending_source_owner_review')
),
merged_apollo AS (
  SELECT
    COALESCE(NULLIF(a.source_relation, ''), s.source_relation) AS source_relation,
    COALESCE(a.date_day, s.date_day) AS date_day,
    COALESCE(NULLIF(a.platform, ''), s.platform) AS platform,
    COALESCE(NULLIF(a.account_id, ''), s.account_id) AS account_id,
    COALESCE(NULLIF(a.account_name, ''), s.account_name) AS account_name,
    COALESCE(NULLIF(a.campaign_id, ''), s.campaign_id) AS campaign_id,
    COALESCE(NULLIF(a.campaign_name, ''), s.campaign_name) AS campaign_name,
    COALESCE(NULLIF(a.ad_group_id, ''), s.ad_group_id) AS ad_group_id,
    COALESCE(NULLIF(a.ad_group_name, ''), s.ad_group_name) AS ad_group_name,
    COALESCE(NULLIF(a.ad_id, ''), s.ad_id) AS ad_id,
    COALESCE(NULLIF(a.ad_name, ''), s.ad_name) AS ad_name,
    COALESCE(a.clicks, s.clicks) AS clicks,
    COALESCE(a.impressions, s.impressions) AS impressions,
    COALESCE(a.spend, s.spend) AS spend,
    COALESCE(a.conversions, s.conversions) AS conversions,
    COALESCE(a.conversions_value, s.conversions_value) AS conversions_value,
    COALESCE(a.video_play, s.video_play) AS video_play,
    COALESCE(a.video_view, s.video_view) AS video_view,
    COALESCE(a.video_views_p_25, s.video_views_p_25) AS video_views_p_25,
    COALESCE(a.video_views_p_50, s.video_views_p_50) AS video_views_p_50,
    COALESCE(a.video_views_p_75, s.video_views_p_75) AS video_views_p_75,
    COALESCE(a.video_views_p_100, s.video_views_p_100) AS video_views_p_100,
    COALESCE(a.hookrate_num, s.hookrate_num) AS hookrate_num,
    COALESCE(NULLIF(a.video_flag, ''), s.video_flag) AS video_flag,
    a.channel AS wp_channel,
    a.channel_group AS wp_channel_group,
    a.media_name AS wp_media_name,
    a.ADIF_channel AS wp_ADIF_channel,
    a.wp_classification_source,
    a.wp_publication_status,
    a.wp_creative_name,
    a.wp_creative_img,
    a.wp_source_sheet_url,
    a.wp_loaded_at,
    a.wp_row_key,
    CASE
      WHEN a.wp_publication_status = 'publish_pending_source_owner_review'
        AND s.ad_id IS NOT NULL THEN 'wp_primary_pending_source_owner_review'
      WHEN a.wp_publication_status = 'publish_pending_source_owner_review'
        THEN 'wp_only_pending_source_owner_review'
      WHEN a.ad_id IS NOT NULL AND s.ad_id IS NOT NULL THEN 'wp_primary_with_standard_fallback'
      WHEN a.ad_id IS NOT NULL THEN 'wp_only'
      ELSE 'standard_only'
    END AS wp_record_source,
    a.ad_id IS NOT NULL AND s.ad_id IS NOT NULL AS wp_has_standard_fallback,
    NULLIF(ARRAY_TO_STRING(ARRAY(
      SELECT field_name
      FROM UNNEST([
        IF(a.conversions IS NULL AND s.conversions IS NOT NULL, 'conversions', NULL),
        IF(a.conversions_value IS NULL AND s.conversions_value IS NOT NULL, 'conversions_value', NULL),
        IF(a.video_play IS NULL AND s.video_play IS NOT NULL, 'video_play', NULL),
        IF(a.video_view IS NULL AND s.video_view IS NOT NULL, 'video_view', NULL),
        IF(a.video_views_p_25 IS NULL AND s.video_views_p_25 IS NOT NULL, 'video_views_p_25', NULL),
        IF(a.video_views_p_50 IS NULL AND s.video_views_p_50 IS NOT NULL, 'video_views_p_50', NULL),
        IF(a.video_views_p_75 IS NULL AND s.video_views_p_75 IS NOT NULL, 'video_views_p_75', NULL),
        IF(a.video_views_p_100 IS NULL AND s.video_views_p_100 IS NOT NULL, 'video_views_p_100', NULL),
        IF(a.hookrate_num IS NULL AND s.hookrate_num IS NOT NULL, 'hookrate_num', NULL)
      ]) AS field_name
      WHERE field_name IS NOT NULL
    ), ' | '), '') AS wp_fallback_fields
  FROM standard_apollo AS s
  FULL OUTER JOIN wp_publishable AS a
    ON s.date_day = a.date_day
   AND LOWER(s.platform) = LOWER(a.platform)
   AND CAST(s.ad_id AS STRING) = CAST(a.ad_id AS STRING)
   AND LOWER(TRIM(COALESCE(s.campaign_name, ''))) = LOWER(TRIM(COALESCE(a.campaign_name, '')))
)
SELECT
  -- QA PURPOSE: WP-first campaign/ad-group/ad source with standard fallback.
  -- CHANGE: This file adds WP Search/YouTube coverage and provenance; the
  --         existing production raw source remains unchanged; cross-campaign
  --         ad-ID conflicts are included with a pending-review status.
  -- CLEANUP: Safe to delete after review by the master data model owner.
  *
FROM standard_non_apollo
UNION ALL
SELECT *
FROM merged_apollo
UNION ALL
SELECT *
FROM `looker-studio-pro-452620.repo_stg.stg__olipop_reddit_crossplatform`;
