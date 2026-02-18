-- * SECTION [1]: TABLE REBUILD
--   Recreate the target table from the updated-FPD ADIF view (schema + base rows).
CREATE OR REPLACE TABLE `looker-studio-pro-452620.repo_stg.adif__prisma_expanded_plus_dcm_with_social_tbl` AS
SELECT
  *
FROM `looker-studio-pro-452620.repo_stg.adif__prisma_expanded_plus_dcm_updated_fpd_view`;


-- * SECTION [2]: SOCIAL APPEND
--   Append WP social rows with ad_set->package and ad->placement mapping.
INSERT INTO `looker-studio-pro-452620.repo_stg.adif__prisma_expanded_plus_dcm_with_social_tbl` (
  package_id_joined,
  date,
  flight_status_flag,
  d_daily_recalculated_cost,
  d_daily_recalculated_imps,
  d_clicks,
  d_video_plays,
  d_video_comps,
  d_min_date,
  d_max_date,
  d_min_flight_date,
  d_max_flight_date,
  d_daily_cpm,
  d_total_delivered_imps,
  d_total_del_inflight_imps,
  planned_daily_spend_pk,
  planned_daily_impressions_pk,
  final_spend,
  final_impressions,
  final_clicks,
  pkg_est_spend,
  pkg_act_spend,
  pkg_est_imps,
  pkg_act_imps,
  pkg_over_bool,
  pkg_over_flag,
  package_type,
  cost_method,
  buy_type,
  buy_category,
  advertiser_name,
  campaign_name,
  supplier_code,
  supplier_name,
  planned_clicks,
  planned_impressions,
  channel_if_buy_category_custom_45,
  package_id,
  package_name,
  PlacementName,
  placement_name,
  placement_id,
  package_group_name,
  start_date,
  end_date,
  report_date,
  script_run_date,
  channel,
  channel_raw,
  channel_group,
  campaign_friendly,
  p_package_friendly,
  placement_type_site,
  line_item_name,
  media_type,
  funnel,
  kpi,
  initiative,
  initative,
  n_of_placements,
  planned_imps_pk,
  planned_cost_pk
)
WITH

-- * SECTION [2.1]: SOCIAL DAILY
--   Normalize social source to ad-level daily grain.
  social_daily AS (
    SELECT
      date_day,
      CASE
        WHEN LOWER(social_platform) IN ('facebook_ads', 'instagram_ads', 'meta') THEN 'meta'
        WHEN LOWER(social_platform) IN ('tiktok_ads', 'tiktok_ads_adif', 'tiktok') THEN 'tiktok'
        ELSE LOWER(social_platform)
      END AS social_platform_norm,
      CAST(campaign_id AS STRING) AS campaign_id,
      campaign_name,
      CAST(ad_group_id AS STRING) AS ad_group_id,
      ad_group_name,
      CAST(ad_id AS STRING) AS ad_id,
      ad_name,
      account_name,
      SUM(spend) AS spend,
      SUM(impressions) AS impressions,
      SUM(clicks) AS clicks,
      SUM(video_play) AS video_play,
      SUM(video_view) AS video_view,
      SUM(video_views_p_100) AS video_complete_proxy
    FROM `looker-studio-pro-452620.repo_stg.stg__adif__social_crossplatform`
    GROUP BY 1,2,3,4,5,6,7,8,9
  ),


-- * SECTION [2.2]: TOKEN PREP
--   Parse ad_set/ad naming tokens used for dimension mapping.
  social_tokens AS (
    SELECT
      s.*,
      SPLIT(s.ad_group_name, '_')[SAFE_OFFSET(1)] AS initiative_token_ag,
      SPLIT(s.ad_group_name, '_')[SAFE_OFFSET(2)] AS funnel_token_ag,
      LOWER(
        REGEXP_EXTRACT(
          s.ad_group_name,
          r'_(facebook|instagram|tiktok|youtube|snapchat|linkedin|pinterest)(?:_|$)'
        )
      ) AS supplier_token_ag,
      SPLIT(s.ad_name, '_')[SAFE_OFFSET(1)] AS initiative_token_ad,
      SPLIT(s.ad_name, '_')[SAFE_OFFSET(2)] AS line_item_name_token_ad,
      SPLIT(s.ad_name, '_')[SAFE_OFFSET(3)] AS placement_type_token_1_ad,
      SPLIT(s.ad_name, '_')[SAFE_OFFSET(4)] AS buy_category_token_1_ad
    FROM social_daily AS s
  ),


-- * SECTION [2.3]: SOCIAL ENRICHMENT
--   Resolve mapped dimensions and ad-group daily allocation helpers.
  social_enriched AS (
    SELECT
      t.*,
      COALESCE(t.initiative_token_ag, t.initiative_token_ad, 'NA') AS initiative,
      COALESCE(t.line_item_name_token_ad, 'NA') AS line_item_name,
      CONCAT(
        COALESCE(t.placement_type_token_1_ad, 'na'),
        '_',
        COALESCE(t.buy_category_token_1_ad, 'na')
      ) AS placement_type_buy_category,
      CASE
        WHEN t.supplier_token_ag = 'facebook' THEN 'FB'
        WHEN t.supplier_token_ag = 'instagram' THEN 'IG'
        WHEN t.supplier_token_ag = 'tiktok' THEN 'TT'
        WHEN t.social_platform_norm = 'meta' THEN 'FB'
        WHEN t.social_platform_norm = 'tiktok' THEN 'TT'
        ELSE UPPER(SUBSTR(t.social_platform_norm, 1, 2))
      END AS supplier_code_short,
      CASE
        WHEN t.supplier_token_ag = 'facebook' THEN 'Facebook'
        WHEN t.supplier_token_ag = 'instagram' THEN 'Instagram'
        WHEN t.supplier_token_ag = 'tiktok' THEN 'TikTok'
        WHEN t.social_platform_norm = 'meta' THEN 'Facebook'
        WHEN t.social_platform_norm = 'tiktok' THEN 'TikTok'
        ELSE INITCAP(t.social_platform_norm)
      END AS supplier_name_full,
      SUM(t.spend) OVER (
        PARTITION BY t.date_day, t.social_platform_norm, t.campaign_id, t.ad_group_id
      ) AS ad_group_day_spend,
      COUNT(*) OVER (
        PARTITION BY t.date_day, t.social_platform_norm, t.campaign_id, t.ad_group_id
      ) AS ad_group_day_ad_count
    FROM social_tokens AS t
  ),


-- * SECTION [2.4]: PACING DEDUPE
--   Keep WP campaign pacing rows, deduped at adgroup flight-window level.
  pacing_dedup AS (
    SELECT
      CASE
        WHEN LOWER(platform) IN ('facebook', 'instagram', 'meta') THEN 'meta'
        WHEN LOWER(platform) = 'tiktok' THEN 'tiktok'
        ELSE LOWER(platform)
      END AS social_platform_norm,
      CAST(c_id AS STRING) AS campaign_id,
      CAST(ag_id AS STRING) AS ad_group_id,
      ag_start_date,
      ag_end_date,
      MAX(final_budget) AS final_budget
    FROM `looker-studio-pro-452620.repo_int.crossplatform_pacing`
    WHERE REGEXP_CONTAINS(IFNULL(campaign_name, ''), r'WP_')
      AND final_budget > 0
      AND ag_start_date IS NOT NULL
      AND ag_end_date IS NOT NULL
    GROUP BY 1,2,3,4,5
  ),


-- * SECTION [2.5]: PACING DAILY
--   Convert pacing budgets into daily planned spend per campaign/platform/ad_set.
  pacing_daily AS (
    SELECT
      day AS date_day,
      social_platform_norm,
      campaign_id,
      ad_group_id,
      SUM(
        final_budget / NULLIF(DATE_DIFF(ag_end_date, ag_start_date, DAY) + 1, 0)
      ) AS planned_daily_spend
    FROM pacing_dedup,
      UNNEST(GENERATE_DATE_ARRAY(ag_start_date, ag_end_date)) AS day
    GROUP BY 1,2,3,4
  ),


-- * SECTION [2.6]: SOCIAL + PLAN JOIN
--   Join social facts to pacing plan by date + platform + campaign_id + ad_group_id.
  social_joined AS (
    SELECT
      s.*,
      p.planned_daily_spend
    FROM social_enriched AS s
    LEFT JOIN pacing_daily AS p
      ON s.date_day = p.date_day
      AND s.social_platform_norm = p.social_platform_norm
      AND s.campaign_id = p.campaign_id
      AND s.ad_group_id = p.ad_group_id
  ),


-- * SECTION [2.7]: BASE MAPPING
--   Map social rows into ADIF-compatible fields.
  social_mapped AS (
    SELECT
      CONCAT(
        'Social_',
        supplier_code_short,
        '_',
        initiative,
        '_',
        line_item_name
      ) AS package_id_joined,
      date_day AS date,
      CAST(ad_group_id AS STRING) AS package_id,
      ad_group_name AS package_name,
      ad_name AS placement_name,
      CAST(ad_id AS STRING) AS placement_id,
      ad_name AS PlacementName,
      ad_group_name AS package_group_name,
      CONCAT(
        'Social_',
        supplier_code_short,
        '_',
        initiative,
        '_',
        line_item_name
      ) AS p_package_friendly,
      placement_type_buy_category AS placement_type_site,
      placement_type_buy_category AS buy_category,
      'social' AS buy_type,
      CONCAT('social_', LOWER(placement_type_buy_category)) AS channel_if_buy_category_custom_45,
      CASE
        WHEN COALESCE(video_play, 0) > 0 OR COALESCE(video_complete_proxy, 0) > 0 THEN 'Video'
        WHEN REGEXP_CONTAINS(ad_group_name, r'(?i)_VV(_|$)|_Video(_|$)|_Reel(_|$)') THEN 'Video'
        WHEN REGEXP_CONTAINS(ad_name, r'(?i)video|reel') THEN 'Video'
        ELSE 'Static'
      END AS media_type,
      CASE
        WHEN funnel_token_ag IN ('VV', 'Reach') THEN 'Awareness'
        WHEN funnel_token_ag = 'Engagement' THEN 'Consideration'
        ELSE 'Awareness'
      END AS funnel,
      CASE
        WHEN funnel_token_ag IN ('VV', 'VideoViews', 'Video') THEN 'Video Views'
        WHEN funnel_token_ag = 'Reach' THEN 'Impressions'
        WHEN funnel_token_ag = 'Engagement' THEN 'Engagement'
        ELSE 'Impressions'
      END AS kpi,
      initiative,
      initiative AS initative,
      line_item_name,
      spend AS final_spend,
      CAST(impressions AS FLOAT64) AS final_impressions,
      CAST(clicks AS FLOAT64) AS final_clicks,
      spend AS d_daily_recalculated_cost,
      CAST(impressions AS INT64) AS d_daily_recalculated_imps,
      CAST(clicks AS INT64) AS d_clicks,
      SAFE_CAST(ROUND(video_play) AS INT64) AS d_video_plays,
      SAFE_CAST(ROUND(video_complete_proxy) AS INT64) AS d_video_comps,
      CASE
        WHEN planned_daily_spend IS NULL THEN NULL
        WHEN ad_group_day_spend > 0 THEN planned_daily_spend * SAFE_DIVIDE(spend, ad_group_day_spend)
        WHEN ad_group_day_ad_count > 0 THEN planned_daily_spend / ad_group_day_ad_count
        ELSE NULL
      END AS planned_daily_spend_pk,
      CAST(NULL AS FLOAT64) AS planned_daily_impressions_pk,
      supplier_name_full AS supplier_name,
      supplier_code_short AS supplier_code,
      campaign_name,
      social_platform_norm,
      campaign_id,
      ad_group_id
    FROM social_joined
  ),


-- * SECTION [2.8]: ROLLUPS
--   Compute package-level totals used by pacing and over/under flags.
  social_with_rollups AS (
    SELECT
      *,
      MIN(date) OVER (
        PARTITION BY package_id
      ) AS start_date_pkg,
      MAX(date) OVER (
        PARTITION BY package_id
      ) AS end_date_pkg,
      SUM(final_spend) OVER (
        PARTITION BY package_id
      ) AS pkg_act_spend,
      SUM(final_impressions) OVER (
        PARTITION BY package_id
      ) AS pkg_act_imps,
      SUM(planned_daily_spend_pk) OVER (
        PARTITION BY package_id
      ) AS pkg_est_spend
    FROM social_mapped
  )


-- * SECTION [2.9]: FINAL SOCIAL ROWS
--   Output social rows aligned to target table columns.
SELECT
  package_id_joined,
  date,
  CASE
    WHEN CURRENT_DATE() > end_date_pkg THEN 'ended'
    ELSE 'live'
  END AS flight_status_flag,
  d_daily_recalculated_cost,
  d_daily_recalculated_imps,
  d_clicks,
  d_video_plays,
  d_video_comps,
  start_date_pkg AS d_min_date,
  end_date_pkg AS d_max_date,
  start_date_pkg AS d_min_flight_date,
  end_date_pkg AS d_max_flight_date,
  CASE
    WHEN final_impressions > 0 THEN (final_spend * 1000) / final_impressions
    ELSE NULL
  END AS d_daily_cpm,
  SAFE_CAST(ROUND(pkg_act_imps) AS INT64) AS d_total_delivered_imps,
  SAFE_CAST(ROUND(pkg_act_imps) AS INT64) AS d_total_del_inflight_imps,
  planned_daily_spend_pk,
  planned_daily_impressions_pk,
  final_spend,
  final_impressions,
  final_clicks,
  pkg_est_spend,
  pkg_act_spend,
  CAST(NULL AS FLOAT64) AS pkg_est_imps,
  pkg_act_imps,
  CASE
    WHEN pkg_est_spend IS NULL OR pkg_est_spend = 0 THEN NULL
    ELSE pkg_act_spend > pkg_est_spend
  END AS pkg_over_bool,
  CASE
    WHEN pkg_est_spend IS NULL OR pkg_est_spend = 0 THEN NULL
    WHEN pkg_act_spend > pkg_est_spend THEN 1
    ELSE 0
  END AS pkg_over_flag,
  'Standalone' AS package_type,
  CAST(NULL AS STRING) AS cost_method,
  buy_type,
  buy_category,
  'Forevermark US' AS advertiser_name,
  campaign_name,
  supplier_code,
  supplier_name,
  CAST(NULL AS INT64) AS planned_clicks,
  CAST(NULL AS INT64) AS planned_impressions,
  channel_if_buy_category_custom_45,
  package_id,
  package_name,
  PlacementName,
  placement_name,
  placement_id,
  package_group_name,
  start_date_pkg AS start_date,
  end_date_pkg AS end_date,
  CURRENT_DATE() AS report_date,
  CURRENT_TIMESTAMP() AS script_run_date,
  CASE
    WHEN media_type = 'Video' THEN 'social_paid video'
    ELSE 'social_paid display'
  END AS channel,
  'social' AS channel_raw,
  'social' AS channel_group,
  campaign_name AS campaign_friendly,
  p_package_friendly,
  placement_type_site,
  line_item_name,
  media_type,
  funnel,
  kpi,
  initiative,
  initative,
  1 AS n_of_placements,
  CAST(NULL AS INT64) AS planned_imps_pk,
  pkg_est_spend AS planned_cost_pk
FROM social_with_rollups;
