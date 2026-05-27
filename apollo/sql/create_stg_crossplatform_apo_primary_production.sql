-- @description: Production shared-social daily-ad builder with APO Search Data
--               Template precedence for Apollo records and the established
--               cross-platform delivery source as fallback.
-- @sources:     Freshest live ad-report relation selected at runtime,
--               repo_stg.stg__olipop_videoviews_crossplatform,
--               repo_stg.stg__apo__search_data_template_daily
-- @output:      repo_stg.stg__olipop__crossplatform_raw_tbl at daily-ad grain.
-- @safety:      Production replacement. Deploy only after QA candidate review.
-- @decision:    Campaign-separated APO ad IDs are included for now and remain
--               flagged publish_pending_source_owner_review until clarified.

CREATE OR REPLACE TABLE `looker-studio-pro-452620.repo_stg.stg__olipop__crossplatform_raw_tbl`
OPTIONS (
  description = "Production shared-social daily-ad staging. APO Search Data Template is primary for Apollo matching campaign/ad rows, with standard shared-social delivery filling absent rows or fields. APO campaign-separated ad-ID records are included pending source-owner review and remain flagged in provenance. Built by apollo/sql/create_stg_crossplatform_apo_primary_production.sql."
) AS
WITH
source_choice AS (
  SELECT source_name
  FROM (
    SELECT
      'transformed' AS source_name,
      TIMESTAMP_MILLIS(last_modified_time) AS last_modified_time
    FROM `giant-spoon-299605.ad_reporting_transformed.__TABLES__`
    WHERE table_id = 'ad_reporting__ad_report'

    UNION ALL

    SELECT
      'reports' AS source_name,
      TIMESTAMP_MILLIS(last_modified_time) AS last_modified_time
    FROM `giant-spoon-299605.ad_reporting_reports.__TABLES__`
    WHERE table_id = 'ad_reporting__ad_report'
  )
  ORDER BY last_modified_time DESC, source_name DESC
  LIMIT 1
),
delivery_source AS (
  SELECT *
  FROM `giant-spoon-299605.ad_reporting_transformed.ad_reporting__ad_report`
  WHERE (SELECT source_name FROM source_choice) = 'transformed'

  UNION ALL

  SELECT *
  FROM `giant-spoon-299605.ad_reporting_reports.ad_reporting__ad_report`
  WHERE (SELECT source_name FROM source_choice) = 'reports'
),
video_metrics AS (
  SELECT *
  FROM `looker-studio-pro-452620.repo_stg.stg__olipop_videoviews_crossplatform`
),
standard_extended AS (
  SELECT
    a.*,
    b.video_play,
    b.video_view,
    b.video_views_p_25,
    b.video_views_p_50,
    b.video_views_p_75,
    b.video_views_p_100,
    b.hookrate_num,
    CASE
      WHEN COALESCE(b.video_play, 0) + COALESCE(b.video_view, 0) > 0 THEN 'video'
      ELSE NULL
    END AS video_flag,
    CAST(NULL AS STRING) AS apo_channel,
    CAST(NULL AS STRING) AS apo_channel_group,
    CAST(NULL AS STRING) AS apo_media_name,
    CAST(NULL AS STRING) AS apo_ADIF_channel,
    CAST(NULL AS STRING) AS apo_classification_source,
    CAST(NULL AS STRING) AS apo_publication_status,
    CAST(NULL AS STRING) AS apo_creative_name,
    CAST(NULL AS STRING) AS apo_creative_box_link,
    CAST(NULL AS STRING) AS apo_source_sheet_url,
    CAST(NULL AS TIMESTAMP) AS apo_loaded_at,
    CAST(NULL AS STRING) AS apo_row_key,
    'standard_only' AS apo_record_source,
    FALSE AS apo_has_standard_fallback,
    CAST(NULL AS STRING) AS apo_fallback_fields
  FROM delivery_source AS a
  LEFT JOIN video_metrics AS b
    ON a.source_relation = b.source_relation
   AND a.date_day = b.date_day
   AND a.campaign_id = b.campaign_id
   AND a.ad_group_id = b.ad_group_id
   AND a.ad_id = b.ad_id
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
apo_publishable AS (
  SELECT *
  FROM `looker-studio-pro-452620.repo_stg.stg__apo__search_data_template_daily`
  WHERE apo_publication_status IN ('publish', 'publish_pending_source_owner_review')
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
    a.channel AS apo_channel,
    a.channel_group AS apo_channel_group,
    a.media_name AS apo_media_name,
    a.ADIF_channel AS apo_ADIF_channel,
    a.apo_classification_source,
    a.apo_publication_status,
    a.apo_creative_name,
    NULLIF(a.apo_creative_box_link, '-') AS apo_creative_box_link,
    a.apo_source_sheet_url,
    a.apo_loaded_at,
    a.apo_row_key,
    CASE
      WHEN a.apo_publication_status = 'publish_pending_source_owner_review'
        AND s.ad_id IS NOT NULL THEN 'apo_primary_pending_source_owner_review'
      WHEN a.apo_publication_status = 'publish_pending_source_owner_review'
        THEN 'apo_only_pending_source_owner_review'
      WHEN a.ad_id IS NOT NULL AND s.ad_id IS NOT NULL THEN 'apo_primary_with_standard_fallback'
      WHEN a.ad_id IS NOT NULL THEN 'apo_only'
      ELSE 'standard_only'
    END AS apo_record_source,
    a.ad_id IS NOT NULL AND s.ad_id IS NOT NULL AS apo_has_standard_fallback,
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
    ), ' | '), '') AS apo_fallback_fields
  FROM standard_apollo AS s
  FULL OUTER JOIN apo_publishable AS a
    ON s.date_day = a.date_day
   AND LOWER(s.platform) = LOWER(a.platform)
   AND CAST(s.ad_id AS STRING) = CAST(a.ad_id AS STRING)
   AND LOWER(TRIM(COALESCE(s.campaign_name, ''))) = LOWER(TRIM(COALESCE(a.campaign_name, '')))
)
SELECT *
FROM standard_non_apollo
UNION ALL
SELECT *
FROM merged_apollo;

-- Preserve the existing video-metric refresh in the live scheduled build.
CREATE OR REPLACE TABLE `looker-studio-pro-452620.repo_stg.stg__olipop_videoviews_crossplatform_tbl` AS
WITH
yt AS (
  SELECT
    'google_ads_olipop' AS source_relation,
    date AS date_day,
    advertiser_id,
    CAST(campaign_id AS STRING) AS campaign_id,
    campaign_name,
    CAST(ad_group_id AS STRING) AS ad_group_id,
    ad_group_name,
    CAST(ad_id AS STRING) AS ad_id,
    ad_name,
    SUM(video_views) AS video_view,
    CAST(NULL AS INT64) AS hookrate_num,
    SUM(impressions) AS video_play,
    SUM(video_views_p_25) AS video_views_p_25,
    SUM(video_views_p_50) AS video_views_p_50,
    SUM(video_views_p_75) AS video_views_p_75,
    SUM(video_views_p_100) AS video_views_p_100
  FROM `looker-studio-pro-452620.repo_google_ads.google_ads_video_stats_vw`
  GROUP BY 1,2,3,4,5,6,7,8,9
),
tt AS (
  SELECT
    'tiktok_ads' AS source_relation,
    date_day AS date,
    advertiser_id,
    CAST(campaign_id AS STRING) AS campaign_id,
    campaign_name,
    CAST(ad_group_id AS STRING) AS ad_group_id,
    ad_group_name,
    CAST(ad_id AS STRING) AS ad_id,
    ad_name,
    CAST(NULL AS INT64) AS video_view,
    video_watched_2_s AS hookrate_num,
    video_play_actions AS video_play,
    video_views_p_25,
    video_views_p_50,
    video_views_p_75,
    video_views_p_100
  FROM `looker-studio-pro-452620.repo_tiktok.mart__tiktok__ad_daily`
),
fb AS (
  SELECT
    'facebook_ads' AS source_relation,
    date_day,
    account_id AS advertiser_id,
    CAST(campaign_id AS STRING) AS campaign_id,
    campaign_name,
    CAST(ad_set_id AS STRING) AS ad_group_id,
    ad_set_name AS ad_group_name,
    CAST(ad_id AS STRING) AS ad_id,
    ad_name,
    video_play,
    video_view,
    video_view_3_sec AS hookrate_num,
    video_p25 AS video_views_p_25,
    video_p50 AS video_views_p_50,
    video_p75 AS video_views_p_75,
    video_p100 AS video_views_p_100
  FROM `looker-studio-pro-452620.repo_facebook.facebook_daily_and_lifetime_vw`
)
SELECT * FROM yt
UNION ALL
SELECT * FROM tt
UNION ALL
SELECT * FROM fb;
