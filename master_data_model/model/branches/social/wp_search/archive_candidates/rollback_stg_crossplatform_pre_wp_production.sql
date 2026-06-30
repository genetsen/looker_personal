-- @description: Rollback definition for the shared-social scheduled builder
--               captured before WP delivery workbook production promotion.
-- @captured:    2026-05-27 from the live scheduled-query definition.
-- @output:      repo_stg.stg__olipop__crossplatform_raw_tbl and its existing
--               cross-platform video companion table.
-- @usage:       Use only to remove WP precedence after a rollback decision.

CREATE OR REPLACE TABLE `looker-studio-pro-452620.repo_stg.stg__olipop__crossplatform_raw_tbl` AS
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
)
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
  END AS video_flag
FROM delivery_source AS a
LEFT JOIN video_metrics AS b
  ON a.source_relation = b.source_relation
 AND a.date_day = b.date_day
 AND a.campaign_id = b.campaign_id
 AND a.ad_group_id = b.ad_group_id
 AND a.ad_id = b.ad_id;

-- Preserve the existing video-metric refresh when restoring the prior builder.
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
