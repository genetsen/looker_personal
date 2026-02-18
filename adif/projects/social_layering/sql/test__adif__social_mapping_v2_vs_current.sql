-- Validation script: proposed social mapping (ad_set->package, ad->placement)
-- Run:
-- bq query --project_id=looker-studio-pro-452620 --use_legacy_sql=false \
--   < projects/social_layering/sql/test__adif__social_mapping_v2_vs_current.sql


-- # * SECTION [1]: RAW SOCIAL BASE
--   Normalize raw social to ad-level daily grain used for proposed mapping.
CREATE TEMP TABLE qa_raw_social AS
SELECT
  date_day AS date,
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
  SUM(video_views_p_100) AS video_comps
FROM `looker-studio-pro-452620.repo_stg.stg__adif__social_crossplatform`
GROUP BY 1,2,3,4,5,6,7,8,9;


-- # * SECTION [2]: PACING DAILY (AD SET GRAIN)
--   Build planned spend at date + platform + campaign + ad_group grain.
CREATE TEMP TABLE qa_pacing_daily AS
WITH pacing_dedup AS (
  SELECT
    CASE
      WHEN LOWER(platform) IN ('facebook', 'instagram', 'meta') THEN 'meta'
      WHEN LOWER(platform) IN ('tiktok') THEN 'tiktok'
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
)
SELECT
  day AS date,
  social_platform_norm,
  campaign_id,
  ad_group_id,
  SUM(
    final_budget / NULLIF(DATE_DIFF(ag_end_date, ag_start_date, DAY) + 1, 0)
  ) AS planned_daily_spend
FROM pacing_dedup,
  UNNEST(GENERATE_DATE_ARRAY(ag_start_date, ag_end_date)) AS day
GROUP BY 1,2,3,4;


-- # * SECTION [3]: PROPOSED MAPPING V2 PREVIEW
--   Map ad_set -> package and ad -> placement with token-based dimensions.
CREATE TEMP TABLE qa_proposed_social AS
WITH social_tokens AS (
  SELECT
    r.*,
    SPLIT(r.ad_group_name, '_')[SAFE_OFFSET(1)] AS initiative_token_ag,
    SPLIT(r.ad_group_name, '_')[SAFE_OFFSET(2)] AS funnel_token_ag,
    LOWER(
      REGEXP_EXTRACT(
        r.ad_group_name,
        r'_(facebook|instagram|tiktok|youtube|snapchat|linkedin|pinterest)(?:_|$)'
      )
    ) AS supplier_token_ag,
    SPLIT(r.ad_name, '_')[SAFE_OFFSET(1)] AS initiative_token_ad,
    SPLIT(r.ad_name, '_')[SAFE_OFFSET(2)] AS line_item_name_token_ad,
    SPLIT(r.ad_name, '_')[SAFE_OFFSET(3)] AS placement_type_token_1_ad,
    SPLIT(r.ad_name, '_')[SAFE_OFFSET(4)] AS buy_category_token_1_ad
  FROM qa_raw_social AS r
),
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
      PARTITION BY t.date, t.social_platform_norm, t.campaign_id, t.ad_group_id
    ) AS ad_group_day_spend,
    COUNT(*) OVER (
      PARTITION BY t.date, t.social_platform_norm, t.campaign_id, t.ad_group_id
    ) AS ad_group_day_ad_count
  FROM social_tokens AS t
),
social_with_plan AS (
  SELECT
    s.*,
    p.planned_daily_spend
  FROM social_enriched AS s
  LEFT JOIN qa_pacing_daily AS p
    ON s.date = p.date
    AND s.social_platform_norm = p.social_platform_norm
    AND s.campaign_id = p.campaign_id
    AND s.ad_group_id = p.ad_group_id
)
SELECT
  date,
  social_platform_norm,
  campaign_id,
  campaign_name,
  ad_group_id,
  ad_group_name,
  ad_id,
  ad_name,
  CONCAT('Social_', supplier_code_short, '_', initiative, '_', line_item_name) AS package_id_joined,
  CAST(ad_group_id AS STRING) AS package_id,
  ad_group_name AS package_name,
  CAST(ad_id AS STRING) AS placement_id,
  ad_name AS placement_name,
  CONCAT('Social_', supplier_code_short, '_', initiative, '_', line_item_name) AS p_package_friendly,
  initiative,
  funnel_token_ag AS funnel,
  supplier_code_short AS supplier_code,
  supplier_name_full AS supplier_name,
  line_item_name,
  placement_type_buy_category AS placement_type_site,
  placement_type_buy_category AS buy_category,
  'social' AS buy_type,
  CONCAT('social_', LOWER(placement_type_buy_category)) AS channel_if_buy_category_custom_45,
  CASE
    WHEN COALESCE(video_play, 0) > 0 OR COALESCE(video_comps, 0) > 0 THEN 'Video'
    WHEN REGEXP_CONTAINS(ad_group_name, r'(?i)_VV(_|$)|_Video(_|$)|_Reel(_|$)') THEN 'Video'
    WHEN REGEXP_CONTAINS(ad_name, r'(?i)video|reel') THEN 'Video'
    ELSE 'Static'
  END AS media_type,
  CASE
    WHEN funnel_token_ag IN ('VV', 'Reach') THEN 'Awareness'
    WHEN funnel_token_ag = 'Engagement' THEN 'Consideration'
    ELSE 'Awareness'
  END AS mapped_funnel,
  CASE
    WHEN funnel_token_ag IN ('VV', 'VideoViews', 'Video') THEN 'Video Views'
    WHEN funnel_token_ag = 'Reach' THEN 'Impressions'
    WHEN funnel_token_ag = 'Engagement' THEN 'Engagement'
    ELSE 'Impressions'
  END AS mapped_kpi,
  CAST(spend AS FLOAT64) AS final_spend,
  CAST(impressions AS FLOAT64) AS final_impressions,
  CAST(clicks AS FLOAT64) AS final_clicks,
  CAST(NULL AS FLOAT64) AS d_media_cost,
  CAST(NULL AS INT64) AS d_impressions,
  CAST(NULL AS FLOAT64) AS d_daily_recalculated_cost,
  CAST(NULL AS INT64) AS d_daily_recalculated_imps,
  CASE
    WHEN planned_daily_spend IS NULL THEN NULL
    WHEN ad_group_day_spend > 0 THEN planned_daily_spend * SAFE_DIVIDE(spend, ad_group_day_spend)
    WHEN ad_group_day_ad_count > 0 THEN planned_daily_spend / ad_group_day_ad_count
    ELSE NULL
  END AS planned_daily_spend_pk
FROM social_with_plan;


-- # * SECTION [4]: CURRENT SOCIAL SNAPSHOT
--   Pull current output for side-by-side comparison.
CREATE TEMP TABLE qa_current_social AS
SELECT
  date,
  CASE
    WHEN LOWER(channel_if_buy_category_custom_45) IN ('facebook_ads', 'instagram_ads', 'meta') THEN 'meta'
    WHEN LOWER(channel_if_buy_category_custom_45) IN ('tiktok_ads', 'tiktok_ads_adif', 'tiktok') THEN 'tiktok'
    ELSE LOWER(channel_if_buy_category_custom_45)
  END AS social_platform_norm,
  campaign_name,
  SUM(final_spend) AS final_spend,
  SUM(final_impressions) AS final_impressions,
  SUM(planned_daily_spend_pk) AS planned_daily_spend_pk
FROM `looker-studio-pro-452620.repo_stg.adif__prisma_expanded_plus_dcm_with_social_tbl`
WHERE channel_raw = 'social'
GROUP BY 1,2,3;


-- # * SECTION [5]: QA CHECK - OVERALL TOTALS
--   Ensure proposed totals match raw social totals.
SELECT
  'overall_raw_vs_proposed' AS qa_check,
  raw_total_spend,
  proposed_total_spend,
  proposed_total_spend - raw_total_spend AS spend_diff,
  raw_total_impressions,
  proposed_total_impressions,
  proposed_total_impressions - raw_total_impressions AS impressions_diff
FROM (
  SELECT
    SUM(spend) AS raw_total_spend,
    SUM(impressions) AS raw_total_impressions
  FROM qa_raw_social
) AS r
CROSS JOIN (
  SELECT
    SUM(final_spend) AS proposed_total_spend,
    SUM(final_impressions) AS proposed_total_impressions
  FROM qa_proposed_social
) AS p;


-- # * SECTION [6]: QA CHECK - DAILY TOTAL MISMATCHES
--   Surface any date-level raw vs proposed differences.
SELECT
  COALESCE(r.date, p.date) AS date,
  r.raw_spend,
  p.proposed_spend,
  p.proposed_spend - r.raw_spend AS spend_diff,
  r.raw_impressions,
  p.proposed_impressions,
  p.proposed_impressions - r.raw_impressions AS impressions_diff
FROM (
  SELECT
    date,
    SUM(spend) AS raw_spend,
    SUM(impressions) AS raw_impressions
  FROM qa_raw_social
  GROUP BY 1
) AS r
FULL OUTER JOIN (
  SELECT
    date,
    SUM(final_spend) AS proposed_spend,
    SUM(final_impressions) AS proposed_impressions
  FROM qa_proposed_social
  GROUP BY 1
) AS p
  ON r.date = p.date
WHERE ABS(COALESCE(p.proposed_spend, 0) - COALESCE(r.raw_spend, 0)) > 0.01
   OR ABS(COALESCE(p.proposed_impressions, 0) - COALESCE(r.raw_impressions, 0)) > 0.01
ORDER BY date;


-- # * SECTION [7]: QA CHECK - CURRENT VS PROPOSED TOTALS
--   Compare current social output vs proposed output at shared reporting grain.
SELECT
  COALESCE(c.date, p.date) AS date,
  COALESCE(c.social_platform_norm, p.social_platform_norm) AS social_platform_norm,
  COALESCE(c.campaign_name, p.campaign_name) AS campaign_name,
  c.final_spend AS current_spend,
  p.final_spend AS proposed_spend,
  p.final_spend - c.final_spend AS spend_diff,
  c.final_impressions AS current_impressions,
  p.final_impressions AS proposed_impressions,
  p.final_impressions - c.final_impressions AS impressions_diff
FROM qa_current_social AS c
FULL OUTER JOIN (
  SELECT
    date,
    social_platform_norm,
    campaign_name,
    SUM(final_spend) AS final_spend,
    SUM(final_impressions) AS final_impressions
  FROM qa_proposed_social
  GROUP BY 1,2,3
) AS p
  ON c.date = p.date
  AND c.social_platform_norm = p.social_platform_norm
  AND c.campaign_name = p.campaign_name
WHERE ABS(COALESCE(p.final_spend, 0) - COALESCE(c.final_spend, 0)) > 0.01
   OR ABS(COALESCE(p.final_impressions, 0) - COALESCE(c.final_impressions, 0)) > 0.01
ORDER BY 1,2,3;


-- # * SECTION [8]: QA CHECK - PACING COVERAGE SUMMARY
--   Compare expected pacing on raw keys vs current and proposed modeled pacing.
WITH raw_keys AS (
  SELECT DISTINCT
    date,
    social_platform_norm,
    campaign_id,
    ad_group_id
  FROM qa_raw_social
),
expected_pacing AS (
  SELECT
    SUM(p.planned_daily_spend) AS expected_planned_spend_on_raw_keys
  FROM raw_keys AS k
  LEFT JOIN qa_pacing_daily AS p
    ON k.date = p.date
    AND k.social_platform_norm = p.social_platform_norm
    AND k.campaign_id = p.campaign_id
    AND k.ad_group_id = p.ad_group_id
),
proposed_pacing AS (
  SELECT
    SUM(planned_daily_spend_pk) AS proposed_planned_spend
  FROM qa_proposed_social
),
current_pacing AS (
  SELECT
    SUM(planned_daily_spend_pk) AS current_planned_spend
  FROM qa_current_social
)
SELECT
  expected_planned_spend_on_raw_keys,
  proposed_planned_spend,
  proposed_planned_spend - expected_planned_spend_on_raw_keys AS proposed_vs_expected_diff,
  current_planned_spend,
  current_planned_spend - expected_planned_spend_on_raw_keys AS current_vs_expected_diff
FROM expected_pacing, proposed_pacing, current_pacing;


-- # * SECTION [9]: QA CHECK - AD SET PACING MATCH DETAIL
--   Ensure allocated proposed pacing re-aggregates to pacing table at ad_set/day grain.
WITH proposed_adset AS (
  SELECT
    date,
    social_platform_norm,
    campaign_id,
    ad_group_id,
    SUM(planned_daily_spend_pk) AS proposed_planned_daily_spend
  FROM qa_proposed_social
  GROUP BY 1,2,3,4
),
raw_keys AS (
  SELECT DISTINCT
    date,
    social_platform_norm,
    campaign_id,
    ad_group_id
  FROM qa_raw_social
)
SELECT
  k.date,
  k.social_platform_norm,
  k.campaign_id,
  k.ad_group_id,
  p.planned_daily_spend AS pacing_daily_spend,
  a.proposed_planned_daily_spend,
  a.proposed_planned_daily_spend - p.planned_daily_spend AS pacing_diff
FROM raw_keys AS k
LEFT JOIN qa_pacing_daily AS p
  ON k.date = p.date
  AND k.social_platform_norm = p.social_platform_norm
  AND k.campaign_id = p.campaign_id
  AND k.ad_group_id = p.ad_group_id
LEFT JOIN proposed_adset AS a
  ON k.date = a.date
  AND k.social_platform_norm = a.social_platform_norm
  AND k.campaign_id = a.campaign_id
  AND k.ad_group_id = a.ad_group_id
WHERE ABS(COALESCE(a.proposed_planned_daily_spend, 0) - COALESCE(p.planned_daily_spend, 0)) > 0.01
ORDER BY 1,2,3,4;


-- # * SECTION [10]: QA CHECK - MAPPING SAMPLE
--   Print sample mapped rows for quick sanity checks.
SELECT
  date,
  social_platform_norm,
  campaign_name,
  ad_group_name,
  ad_name,
  package_id_joined,
  package_id,
  package_name,
  placement_id,
  placement_name,
  supplier_code,
  supplier_name,
  initiative,
  line_item_name,
  placement_type_site,
  buy_category,
  buy_type,
  channel_if_buy_category_custom_45,
  final_spend,
  final_impressions,
  planned_daily_spend_pk
FROM qa_proposed_social
ORDER BY final_spend DESC
LIMIT 25;
