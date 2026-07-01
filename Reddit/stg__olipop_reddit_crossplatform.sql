CREATE OR REPLACE VIEW `looker-studio-pro-452620.repo_stg.stg__olipop_reddit_crossplatform` AS

/* ╭───────────────────────────────────────────────────────────╮
   │  View: stg__olipop_reddit_crossplatform                   │
   │  Layer / Dataset: repo_stg                               │
   │  Purpose:                                                │
   │     • Standardise Reddit Ads email-ingested daily metrics │
   │     • Map fields to match the shared-social daily-ad      │
   │       grain schema (stg__olipop__crossplatform_raw_tbl)   │
   │       to allow seamless union/layering in the future      │
   │  Owner:  Gene – Marketing Data Analytics                  │
   │  Created: 2026-06-30                                      │
   │  Output columns (in order):                               │
   │     1-38 columns matching standard shared-social daily     │
   ╰───────────────────────────────────────────────────────────╯ */

SELECT
  'reddit-ads-email' AS source_relation,
  date AS date_day,
  'reddit' AS platform,
  CAST(NULL AS STRING) AS account_id,
  'Olipop Reddit' AS account_name,
  CAST(campaign_id AS STRING) AS campaign_id,
  campaign_name AS campaign_name,
  CAST(ad_group_id AS STRING) AS ad_group_id,
  ad_group_name AS ad_group_name,
  CAST(ad_id AS STRING) AS ad_id,
  ad_name AS ad_name,
  CAST(clicks AS INT64) AS clicks,
  CAST(impressions AS INT64) AS impressions,
  CAST(amount_spent_usd AS FLOAT64) AS spend,
  CAST(total_results AS FLOAT64) AS conversions,
  CAST(NULL AS FLOAT64) AS conversions_value,
  CAST(NULL AS FLOAT64) AS video_play,
  CAST(video_views AS FLOAT64) AS video_view,
  CAST(watches_at_25_percent AS FLOAT64) AS video_views_p_25,
  CAST(NULL AS FLOAT64) AS video_views_p_50,
  CAST(NULL AS FLOAT64) AS video_views_p_75,
  CAST(watches_at_100_percent AS FLOAT64) AS video_views_p_100,
  CAST(NULL AS FLOAT64) AS hookrate_num,
  CAST(NULL AS STRING) AS video_flag,
  CAST(NULL AS STRING) AS wp_channel,
  CAST(NULL AS STRING) AS wp_channel_group,
  CAST(NULL AS STRING) AS wp_media_name,
  CAST(NULL AS STRING) AS wp_ADIF_channel,
  CAST(NULL AS STRING) AS wp_classification_source,
  CAST(NULL AS STRING) AS wp_publication_status,
  CAST(NULL AS STRING) AS wp_creative_name,
  CAST(NULL AS STRING) AS wp_creative_img,
  CAST(NULL AS STRING) AS wp_source_sheet_url,
  CAST(NULL AS TIMESTAMP) AS wp_loaded_at,
  CAST(NULL AS STRING) AS wp_row_key,
  'standard_only' AS wp_record_source,
  FALSE AS wp_has_standard_fallback,
  CAST(NULL AS STRING) AS wp_fallback_fields
FROM `looker-studio-pro-452620.landing.reddit-ads-email`
