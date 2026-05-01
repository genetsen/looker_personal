-- Build generalized package/date data model.
--
-- Purpose:
--   Combine DCM + original FPD + updated FPD + Prisma into final digital
--   metrics, TV estimates, package rollups, and social rows where a compatible
--   social grain is available.
--
-- Scope rules:
--   - No advertiser/client/source-file filters.
--   - Prisma rows include packages with start_date >= 2025-01-01.
--   - Delivery rows only include dates >= 2025-01-01, even when no matching
--     Prisma package exists.

CREATE OR REPLACE VIEW `looker-studio-pro-452620.master_stg.data_model` AS
WITH
dcm_daily AS (
  SELECT
    d.package_id,
    DATE(d.date) AS date,
    ARRAY_AGG(d.advertiser IGNORE NULLS ORDER BY d.advertiser LIMIT 1)[SAFE_OFFSET(0)] AS d_advertiser_name,
    ARRAY_AGG(d.campaign IGNORE NULLS ORDER BY d.campaign LIMIT 1)[SAFE_OFFSET(0)] AS d_campaign_name,
    ARRAY_AGG(d.package_roadblock IGNORE NULLS ORDER BY d.package_roadblock LIMIT 1)[SAFE_OFFSET(0)] AS d_package_name,
    ARRAY_AGG(d.site IGNORE NULLS ORDER BY d.site LIMIT 1)[SAFE_OFFSET(0)] AS d_supplier_name,
    ARRAY_AGG(d.p_channel_group IGNORE NULLS ORDER BY d.p_channel_group LIMIT 1)[SAFE_OFFSET(0)] AS d_channel_group,
    MIN(d.p_start_date) AS d_package_start_date,
    MAX(d.p_end_date) AS d_package_end_date,
    MIN(d.flight_status_flag) AS flight_status_flag,
    SUM(d.daily_recalculated_cost) AS d_daily_recalculated_cost,
    SUM(d.daily_recalculated_imps) AS d_daily_recalculated_imps,
    SUM(d.impressions) AS d_impressions,
    SUM(d.media_cost) AS d_media_cost,
    SUM(d.clicks) AS d_clicks,
    SUM(d.rich_media_video_plays) AS d_video_plays,
    SUM(d.rich_media_video_completions) AS d_video_comps,
    MIN(d.d_min_date) AS d_min_date,
    MAX(d.d_max_date) AS d_max_date,
    MIN(d.prorated_planned_cost_pk) AS d_prorated_planned_cost_pk,
    MIN(d.prorated_planned_imps_pk) AS d_prorated_planned_imps_pk,
    MIN(d.min_flight_date) AS d_min_flight_date,
    MAX(d.max_flight_date) AS d_max_flight_date,
    AVG(d.daily_cpm) AS d_daily_cpm,
    MAX(d.pkg_total_imps) AS d_total_delivered_imps,
    MAX(d.total_inflight_impressions) AS d_total_del_inflight_imps
  FROM `looker-studio-pro-452620.DCM.20250505_costModel_v5` AS d
  WHERE DATE(d.date) >= DATE '2025-01-01'
    AND d.package_id IS NOT NULL
  GROUP BY d.package_id, DATE(d.date)
),

fpd_original_raw AS (
  SELECT
    f.package_id,
    DATE(f.date_final) AS date,
    f.impressions,
    f.clicks,
    f.spend,
    f.sends,
    f.opens,
    f.benchmark,
    f.benchmark_metric,
    f.partner_creative_name AS creative,
    f.source_file,
    f.source_url,
    f.last_modified_time,
    f.client,
    f.campaign,
    f.site,
    f.package_name
  FROM `looker-studio-pro-452620.landing.fpd_data_ranged_shortcutsFolder` AS f
  WHERE DATE(f.date_final) >= DATE '2025-01-01'
    AND f.package_id IS NOT NULL
),

fpd_original_daily AS (
  SELECT
    package_id,
    date,
    SUM(impressions) AS fpd_orig_impressions,
    SUM(clicks) AS fpd_orig_clicks,
    SUM(spend) AS fpd_orig_spend,
    SAFE_CAST(ROUND(SUM(sends)) AS INT64) AS fpd_orig_sends,
    SAFE_CAST(ROUND(SUM(opens)) AS INT64) AS fpd_orig_opens,
    MAX(benchmark) AS fpd_orig_benchmark,
    SAFE_CAST(ROUND(MAX(benchmark_metric)) AS INT64) AS fpd_orig_benchmark_metric,
    STRING_AGG(DISTINCT CAST(creative AS STRING), ' | ' ORDER BY CAST(creative AS STRING)) AS fpd_orig_creative,
    STRING_AGG(DISTINCT CAST(source_file AS STRING), ' | ' ORDER BY CAST(source_file AS STRING)) AS fpd_orig_source_files,
    STRING_AGG(DISTINCT CAST(source_url AS STRING), ' | ' ORDER BY CAST(source_url AS STRING)) AS fpd_orig_source_urls,
    MAX(last_modified_time) AS fpd_orig_source_modified_time,
    ARRAY_AGG(client IGNORE NULLS ORDER BY client LIMIT 1)[SAFE_OFFSET(0)] AS fpd_orig_client,
    ARRAY_AGG(campaign IGNORE NULLS ORDER BY campaign LIMIT 1)[SAFE_OFFSET(0)] AS fpd_orig_campaign_name,
    ARRAY_AGG(site IGNORE NULLS ORDER BY site LIMIT 1)[SAFE_OFFSET(0)] AS fpd_orig_supplier_name,
    ARRAY_AGG(package_name IGNORE NULLS ORDER BY package_name LIMIT 1)[SAFE_OFFSET(0)] AS fpd_orig_package_name
  FROM fpd_original_raw
  GROUP BY package_id, date
),

fpd_updated_daily AS (
  SELECT
    u.package_id,
    DATE(u.date) AS date,
    SUM(u.daily_fpd_impressions) AS fpd_updated_impressions,
    SUM(u.daily_fpd_spend) AS fpd_updated_spend,
    STRING_AGG(DISTINCT u.supplier_name, ', ' ORDER BY u.supplier_name) AS fpd_updated_suppliers,
    STRING_AGG(DISTINCT u.initiative, ', ' ORDER BY u.initiative) AS fpd_updated_initiatives,
    MAX(u.data_update_datetime) AS fpd_updated_data_timestamp,
    MAX(u.source_sheet_modified_time) AS fpd_updated_source_sheet_modified_time,
    ARRAY_AGG(u.package_name IGNORE NULLS ORDER BY u.package_name LIMIT 1)[SAFE_OFFSET(0)] AS fpd_updated_package_name
  FROM `looker-studio-pro-452620.landing.adif_updated_fpd_daily` AS u
  WHERE DATE(u.date) >= DATE '2025-01-01'
    AND u.package_id IS NOT NULL
  GROUP BY u.package_id, DATE(u.date)
),

prisma_daily_raw AS (
  SELECT
    p.package_id,
    DATE(p.date) AS date,
    p.planned_daily_spend_pk,
    p.planned_daily_impressions_pk,
    p.planned_clicks,
    p.report_date
  FROM `looker-studio-pro-452620.20250327_data_model.prisma_expanded_full` AS p
  WHERE p.package_type != 'Child'
    AND p.start_date >= DATE '2025-01-01'
    AND DATE(p.date) >= DATE '2025-01-01'
),

prisma_daily AS (
  SELECT
    package_id,
    date,
    SUM(planned_daily_spend_pk) AS planned_daily_spend_pk,
    SUM(planned_daily_impressions_pk) AS planned_daily_impressions_pk,
    SUM(planned_clicks) AS planned_clicks,
    MAX(report_date) AS max_prisma_report_date
  FROM prisma_daily_raw
  GROUP BY package_id, date
),

prisma_meta_src AS (
  SELECT
    p.* EXCEPT(planned_daily_spend_pk, planned_daily_impressions_pk, date)
  FROM `looker-studio-pro-452620.20250327_data_model.prisma_expanded_full` AS p
  WHERE p.package_type != 'Child'
    AND p.start_date >= DATE '2025-01-01'
),

prisma_meta AS (
  SELECT x.*
  FROM (
    SELECT
      (ARRAY_AGG(m ORDER BY m.report_date DESC NULLS LAST))[OFFSET(0)] AS x
    FROM prisma_meta_src AS m
    GROUP BY m.package_id
  )
),

digital_joined AS (
  SELECT
    COALESCE(d.package_id, fo.package_id, fu.package_id, p.package_id) AS package_id_joined,
    COALESCE(d.date, fo.date, fu.date, p.date) AS date,
    d.* EXCEPT(package_id, date),
    fo.* EXCEPT(package_id, date),
    fu.* EXCEPT(package_id, date),
    p.* EXCEPT(package_id, date)
  FROM dcm_daily AS d
  FULL OUTER JOIN fpd_original_daily AS fo
    ON d.package_id = fo.package_id
   AND d.date = fo.date
  FULL OUTER JOIN fpd_updated_daily AS fu
    ON COALESCE(d.package_id, fo.package_id) = fu.package_id
   AND COALESCE(d.date, fo.date) = fu.date
  FULL OUTER JOIN prisma_daily AS p
    ON COALESCE(d.package_id, fo.package_id, fu.package_id) = p.package_id
   AND COALESCE(d.date, fo.date, fu.date) = p.date
),

digital_with_meta AS (
  SELECT
    j.*,
    m.* EXCEPT(package_id, planned_clicks, report_date, script_run_date),
    m.package_id AS prisma_package_id,
    m.report_date AS prisma_metadata_report_date,
    m.script_run_date AS prisma_script_run_date
  FROM digital_joined AS j
  LEFT JOIN prisma_meta AS m
    ON j.package_id_joined = m.package_id
),

dcm_low_signal_primary_packages AS (
  SELECT
    package_id_joined AS low_signal_package_id
  FROM digital_with_meta
  WHERE fpd_updated_impressions IS NULL
    AND fpd_updated_spend IS NULL
    AND fpd_orig_impressions IS NULL
    AND fpd_orig_spend IS NULL
    AND (d_daily_recalculated_imps IS NOT NULL OR d_daily_recalculated_cost IS NOT NULL)
    AND COALESCE(d_impressions, 0) < 1000
    AND COALESCE(d_media_cost, 0) < 0.1
    AND COALESCE(d_clicks, 0) < 5
  GROUP BY package_id_joined
  HAVING SAFE_DIVIDE(SUM(COALESCE(d_impressions, 0)), COUNT(*)) < 100
),

digital_final AS (
  SELECT
    'digital' AS row_type,
    CASE
      WHEN fpd_updated_impressions IS NOT NULL OR fpd_updated_spend IS NOT NULL THEN
        CASE
          WHEN fpd_orig_impressions IS NOT NULL OR fpd_orig_spend IS NOT NULL THEN 'fpd_combined'
          ELSE 'fpd_updated_only'
        END
      WHEN fpd_orig_impressions IS NOT NULL OR fpd_orig_spend IS NOT NULL THEN 'fpd_original_only'
      WHEN d_daily_recalculated_imps IS NOT NULL OR d_daily_recalculated_cost IS NOT NULL THEN 'dcm'
      ELSE 'planned_only'
    END AS row_data_source_primary,
    COALESCE(
      NULLIF(ARRAY_TO_STRING(ARRAY_CONCAT(
        IF(prisma_package_id IS NOT NULL, ['prisma'], []),
        IF(planned_daily_spend_pk IS NOT NULL
          OR planned_daily_impressions_pk IS NOT NULL
          OR planned_clicks IS NOT NULL, ['prisma_daily'], []),
        IF(d_daily_recalculated_imps IS NOT NULL
          OR d_daily_recalculated_cost IS NOT NULL, ['dcm'], []),
        IF(fpd_orig_impressions IS NOT NULL
          OR fpd_orig_spend IS NOT NULL, ['fpd_original'], []),
        IF(fpd_updated_impressions IS NOT NULL
          OR fpd_updated_spend IS NOT NULL, ['fpd_updated'], [])
      ), ' | '), ''),
      'none'
    ) AS row_data_sources_available,
    COALESCE(
      NULLIF(ARRAY_TO_STRING(ARRAY_CONCAT(
        IF(prisma_package_id IS NULL
          AND (
            d_daily_recalculated_imps IS NOT NULL
            OR d_daily_recalculated_cost IS NOT NULL
            OR fpd_orig_impressions IS NOT NULL
            OR fpd_orig_spend IS NOT NULL
            OR fpd_updated_impressions IS NOT NULL
            OR fpd_updated_spend IS NOT NULL
          ), ['missing_prisma_package'], []),
        IF(prisma_package_id IS NOT NULL
          AND planned_daily_spend_pk IS NULL
          AND planned_daily_impressions_pk IS NULL
          AND planned_clicks IS NULL
          AND (
            d_daily_recalculated_imps IS NOT NULL
            OR d_daily_recalculated_cost IS NOT NULL
            OR fpd_orig_impressions IS NOT NULL
            OR fpd_orig_spend IS NOT NULL
            OR fpd_updated_impressions IS NOT NULL
            OR fpd_updated_spend IS NOT NULL
          ), ['missing_prisma_daily'], []),
        IF(
          (
            d_daily_recalculated_imps IS NOT NULL
            OR d_daily_recalculated_cost IS NOT NULL
          )
          AND (
            fpd_orig_impressions IS NOT NULL
            OR fpd_orig_spend IS NOT NULL
            OR fpd_updated_impressions IS NOT NULL
            OR fpd_updated_spend IS NOT NULL
          )
          AND (
            ABS(COALESCE(fpd_orig_spend, 0) + COALESCE(fpd_updated_spend, 0) - COALESCE(d_daily_recalculated_cost, 0)) > 1
            OR ABS(COALESCE(fpd_orig_impressions, 0) + COALESCE(fpd_updated_impressions, 0) - COALESCE(d_daily_recalculated_imps, d_impressions, 0)) > 1
          ), ['actual_source_conflict'], []),
        IF(low_signal.low_signal_package_id IS NOT NULL
          AND fpd_updated_impressions IS NULL
          AND fpd_updated_spend IS NULL
          AND fpd_orig_impressions IS NULL
          AND fpd_orig_spend IS NULL
          AND (d_daily_recalculated_imps IS NOT NULL OR d_daily_recalculated_cost IS NOT NULL)
          AND COALESCE(d_impressions, 0) < 1000
          AND COALESCE(d_media_cost, 0) < 0.1
          AND COALESCE(d_clicks, 0) < 5, ['low_signal_dcm'], []),
        IF(prisma_package_id IS NOT NULL
          AND d_daily_recalculated_imps IS NULL
          AND d_daily_recalculated_cost IS NULL
          AND fpd_orig_impressions IS NULL
          AND fpd_orig_spend IS NULL
          AND fpd_updated_impressions IS NULL
          AND fpd_updated_spend IS NULL, ['missing_actuals'], []),
        IF(prisma_package_id IS NOT NULL
          AND COALESCE(
            NULLIF(COALESCE(fpd_orig_spend, 0) + COALESCE(fpd_updated_spend, 0), 0),
            d_daily_recalculated_cost
          ) IS NULL
          AND COALESCE(
            NULLIF(COALESCE(fpd_orig_impressions, 0) + COALESCE(fpd_updated_impressions, 0), 0),
            d_daily_recalculated_imps,
            d_impressions
          ) IS NULL
          AND COALESCE(fpd_orig_clicks, d_clicks) IS NULL
          AND (
            d_daily_recalculated_imps IS NOT NULL
            OR d_daily_recalculated_cost IS NOT NULL
            OR fpd_orig_impressions IS NOT NULL
            OR fpd_orig_spend IS NOT NULL
            OR fpd_updated_impressions IS NOT NULL
            OR fpd_updated_spend IS NOT NULL
          ), ['missing_final_metrics'], [])
      ), ' | '), ''),
      'ok'
    ) AS row_data_issue_category,
    package_id_joined,
    date,
    COALESCE(start_date, d_package_start_date, date) AS package_start_date,
    COALESCE(end_date, d_package_end_date, date) AS package_end_date,
    COALESCE(advertiser_name, d_advertiser_name, fpd_orig_client) AS advertiser_name,
    advertiser_short_name,
    COALESCE(campaign_name, d_campaign_name, fpd_orig_campaign_name) AS campaign_name,
    COALESCE(campaign_friendly, d_campaign_name, fpd_orig_campaign_name) AS campaign_friendly,
    product_code,
    product_name,
    COALESCE(package_type, 'UnmatchedDigitalPackage') AS package_type,
    COALESCE(package_name, d_package_name, fpd_updated_package_name, fpd_orig_package_name, package_id_joined) AS package_name,
    p_package_friendly,
    CASE
      WHEN COALESCE(package_name, d_package_name, fpd_updated_package_name, fpd_orig_package_name) IS NULL
        OR COALESCE(package_name, d_package_name, fpd_updated_package_name, fpd_orig_package_name) = '' THEN NULL
      WHEN REGEXP_CONTAINS(COALESCE(package_name, d_package_name, fpd_updated_package_name, fpd_orig_package_name), r'(?i)(iHeart|SiriusXM|WeAreAOk|Wonder)') THEN 'Audio'
      WHEN REGEXP_CONTAINS(COALESCE(package_name, d_package_name, fpd_updated_package_name, fpd_orig_package_name), r'(?i)(Peacock|DISNED|ESPN|Hulu|Roku|HBO|Paramount|Tubi|YouTube|NBCU)') THEN 'Video'
      WHEN REGEXP_CONTAINS(COALESCE(package_name, d_package_name, fpd_updated_package_name, fpd_orig_package_name), r'(?i)(NBC|WBD|CBS|Playfly|Audience Express|Disney)') THEN 'Linear'
      WHEN REGEXP_CONTAINS(supplier_code, r'(?i)(PROJEX|Quan|QUAN)') THEN 'OOH Regional'
      WHEN REGEXP_CONTAINS(COALESCE(package_name, d_package_name, fpd_updated_package_name, fpd_orig_package_name), r'(?i)(PROJEX|Quan|QUAN)') THEN 'OOH Regional'
      WHEN REGEXP_CONTAINS(COALESCE(package_name, d_package_name, fpd_updated_package_name, fpd_orig_package_name), r'(?i)(ScreenVision|NCM)') THEN 'Cinema Regional'
      WHEN REGEXP_CONTAINS(COALESCE(package_name, d_package_name, fpd_updated_package_name, fpd_orig_package_name), r'(?i)(Vox|CondeNast|NYT|Meredi|TINYBE|NATVLY)') THEN 'Publisher Partnership'
      WHEN REGEXP_CONTAINS(COALESCE(package_name, d_package_name, fpd_updated_package_name, fpd_orig_package_name), r'(?i)People First') THEN 'Influencer'
      WHEN REGEXP_CONTAINS(COALESCE(package_name, d_package_name, fpd_updated_package_name, fpd_orig_package_name), r'(?i)(People|STLWED|NJB|BLISS)') THEN 'Publisher Partnership'
      WHEN REGEXP_CONTAINS(COALESCE(package_name, d_package_name, fpd_updated_package_name, fpd_orig_package_name), r'(?i)(Jeweler|JCK|Instore|Gem|AGS|Centurion|Zimnisky|KENIL|RELX)') THEN 'Trade'
      WHEN REGEXP_CONTAINS(COALESCE(package_name, d_package_name, fpd_updated_package_name, fpd_orig_package_name), r'(?i)MIQ') THEN 'Programmatic'
      WHEN REGEXP_CONTAINS(COALESCE(package_name, d_package_name, fpd_updated_package_name, fpd_orig_package_name), r'(?i)feeorder') THEN 'fee'
      WHEN REGEXP_CONTAINS(package_type, r'(?i)Print') THEN 'Publisher Partnership'
      ELSE 'Unmapped'
    END AS gsMediaTeam_channel,
    COALESCE(placement_id, package_id_joined) AS placement_id,
    COALESCE(placement_name, d_package_name, fpd_updated_package_name, fpd_orig_package_name, package_id_joined) AS placement_name,
    COALESCE(
      supplier_code,
      UPPER(REGEXP_REPLACE(COALESCE(d_supplier_name, fpd_updated_suppliers, fpd_orig_supplier_name, 'UNKNOWN'), r'[^A-Za-z0-9]+', '_'))
    ) AS supplier_code,
    COALESCE(supplier_name, d_supplier_name, fpd_updated_suppliers, fpd_orig_supplier_name) AS supplier_name,
    supplier_logo,
    COALESCE(buy_type, d_channel_group, 'Unmapped') AS buy_type,
    COALESCE(buy_category, d_channel_group, 'Unmapped') AS buy_category,
    COALESCE(channel, d_channel_group, 'unmapped') AS channel,
    COALESCE(channel_raw, d_channel_group, 'Unmapped') AS channel_raw,
    COALESCE(channel_group, d_channel_group, 'unmapped') AS channel_group,
    COALESCE(media_name, 'Digital') AS media_name,
    cost_method,
    planned_amount,
    planned_impressions,
    planned_units,
    unit_type,
    payable_rate,
    planned_daily_spend_pk,
    planned_daily_impressions_pk,
    planned_clicks AS prisma_planned_clicks,
    max_prisma_report_date,
    d_daily_recalculated_cost,
    d_daily_recalculated_imps,
    d_impressions,
    d_media_cost,
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
    fpd_orig_impressions,
    fpd_orig_spend,
    fpd_orig_clicks,
    fpd_orig_sends,
    fpd_orig_opens,
    fpd_orig_benchmark,
    fpd_orig_benchmark_metric,
    fpd_orig_creative,
    fpd_orig_source_files,
    fpd_orig_source_urls,
    fpd_orig_source_modified_time,
    fpd_updated_impressions,
    fpd_updated_spend,
    fpd_updated_suppliers,
    fpd_updated_initiatives,
    fpd_updated_data_timestamp,
    fpd_updated_source_sheet_modified_time,
    COALESCE(fpd_orig_impressions, 0) + COALESCE(fpd_updated_impressions, 0) AS fpd_impressions,
    COALESCE(fpd_orig_spend, 0) + COALESCE(fpd_updated_spend, 0) AS fpd_spend,
    fpd_orig_clicks AS fpd_clicks,
    fpd_orig_sends AS fpd_sends,
    fpd_orig_opens AS fpd_opens,
    CASE
      WHEN prisma_package_id IS NULL THEN NULL
      ELSE COALESCE(
        NULLIF(COALESCE(fpd_orig_spend, 0) + COALESCE(fpd_updated_spend, 0), 0),
        d_daily_recalculated_cost
      )
    END AS final_spend,
    CASE
      WHEN prisma_package_id IS NULL THEN NULL
      ELSE COALESCE(
        NULLIF(COALESCE(fpd_orig_impressions, 0) + COALESCE(fpd_updated_impressions, 0), 0),
        d_daily_recalculated_imps,
        d_impressions
      )
    END AS final_impressions,
    CASE
      WHEN prisma_package_id IS NULL THEN NULL
      ELSE COALESCE(fpd_orig_clicks, d_clicks)
    END AS final_clicks,
    CASE
      WHEN prisma_package_id IS NULL THEN NULL
      ELSE CAST(d_video_plays AS FLOAT64)
    END AS final_video_plays,
    CASE
      WHEN prisma_package_id IS NULL THEN NULL
      ELSE CAST(d_video_comps AS FLOAT64)
    END AS final_video_comps,
    CAST(NULL AS DATE) AS tv_data_refresh_date,
    CAST(NULL AS STRING) AS tv_media_outlet,
    CAST(NULL AS STRING) AS tv_type,
    CAST(NULL AS STRING) AS tv_program_name,
    CAST(NULL AS STRING) AS tv_market,
    CAST(NULL AS STRING) AS tv_quarter,
    CAST(NULL AS INT64) AS tv_year,
    CAST(NULL AS INT64) AS tv_net_impressions,
    CAST(NULL AS FLOAT64) AS tv_net_cost,
    CAST(NULL AS INT64) AS tv_total_units,
    CAST(NULL AS STRING) AS social_platform,
    CAST(NULL AS STRING) AS social_campaign_id,
    CAST(NULL AS STRING) AS social_ad_group_id,
    CAST(NULL AS STRING) AS social_ad_id,
    CAST(NULL AS STRING) AS social_account_name,
    CAST(NULL AS FLOAT64) AS social_pacing_planned_spend,
    CAST(NULL AS FLOAT64) AS social_spend,
    CAST(NULL AS FLOAT64) AS social_impressions,
    CAST(NULL AS FLOAT64) AS social_clicks,
    CAST(NULL AS FLOAT64) AS social_video_plays,
    CAST(NULL AS FLOAT64) AS social_video_views,
    CAST(NULL AS FLOAT64) AS social_video_comps
  FROM digital_with_meta
  LEFT JOIN dcm_low_signal_primary_packages AS low_signal
    ON package_id_joined = low_signal.low_signal_package_id
  WHERE package_id_joined IS NOT NULL
),

social_daily AS (
  SELECT
    s.date_day,
    CASE
      WHEN LOWER(s.platform) IN ('facebook_ads', 'instagram_ads', 'meta', 'facebook', 'instagram') THEN 'meta'
      WHEN LOWER(s.platform) IN ('tiktok_ads', 'tiktok') THEN 'tiktok'
      WHEN LOWER(s.platform) IN ('pinterest_ads', 'pinterest') THEN 'pinterest'
      WHEN LOWER(s.platform) IN ('linkedin_ads', 'linkedin') THEN 'linkedin'
      WHEN LOWER(s.platform) IN ('snapchat_ads', 'snapchat') THEN 'snapchat'
      ELSE LOWER(s.platform)
    END AS social_platform,
    CAST(s.campaign_id AS STRING) AS campaign_id,
    s.campaign_name,
    CAST(s.ad_group_id AS STRING) AS ad_group_id,
    s.ad_group_name,
    CAST(s.ad_id AS STRING) AS ad_id,
    s.ad_name,
    s.account_name,
    SUM(s.spend) AS spend,
    SUM(s.impressions) AS impressions,
    SUM(s.clicks) AS clicks,
    SUM(s.video_play) AS video_play,
    SUM(s.video_view) AS video_view,
    SUM(s.video_views_p_100) AS video_complete_proxy
  FROM `looker-studio-pro-452620.repo_stg.stg__olipop__crossplatform_raw_tbl` AS s
  WHERE s.date_day >= DATE '2025-01-01'
  GROUP BY 1,2,3,4,5,6,7,8,9
),

social_pacing_dedup AS (
  SELECT
    CASE
      WHEN LOWER(platform) IN ('facebook_ads', 'instagram_ads', 'meta', 'facebook', 'instagram') THEN 'meta'
      WHEN LOWER(platform) IN ('tiktok_ads', 'tiktok') THEN 'tiktok'
      WHEN LOWER(platform) IN ('pinterest_ads', 'pinterest') THEN 'pinterest'
      WHEN LOWER(platform) IN ('linkedin_ads', 'linkedin') THEN 'linkedin'
      WHEN LOWER(platform) IN ('snapchat_ads', 'snapchat') THEN 'snapchat'
      ELSE LOWER(platform)
    END AS social_platform,
    CAST(c_id AS STRING) AS campaign_id,
    CAST(ag_id AS STRING) AS ad_group_id,
    MIN(start_date) AS start_date,
    MAX(end_date) AS end_date,
    MAX(final_budget) AS final_budget
  FROM `looker-studio-pro-452620.repo_int.crossplatform_pacing`
  WHERE start_date >= DATE '2025-01-01'
    AND final_budget > 0
    AND start_date IS NOT NULL
    AND end_date IS NOT NULL
  GROUP BY 1,2,3
),

social_pacing_daily AS (
  SELECT
    day AS date_day,
    social_platform,
    campaign_id,
    ad_group_id,
    start_date,
    end_date,
    SUM(final_budget / NULLIF(DATE_DIFF(end_date, start_date, DAY) + 1, 0)) AS planned_daily_spend
  FROM social_pacing_dedup,
    UNNEST(GENERATE_DATE_ARRAY(start_date, end_date)) AS day
  GROUP BY 1,2,3,4,5,6
),

social_with_pacing AS (
  SELECT
    s.*,
    p.start_date AS pacing_start_date,
    p.end_date AS pacing_end_date,
    p.planned_daily_spend,
    COUNT(*) OVER (
      PARTITION BY s.date_day, s.social_platform, s.campaign_id, s.ad_group_id
    ) AS ad_rows_in_group_day
  FROM social_daily AS s
  LEFT JOIN social_pacing_daily AS p
    ON s.date_day = p.date_day
   AND s.social_platform = p.social_platform
   AND s.campaign_id = p.campaign_id
   AND s.ad_group_id = p.ad_group_id
  WHERE COALESCE(p.start_date, s.date_day) >= DATE '2025-01-01'
),

social_final AS (
  SELECT
    'social' AS row_type,
    'social' AS row_data_source_primary,
    ARRAY_TO_STRING(ARRAY_CONCAT(
      ['social'],
      IF(planned_daily_spend IS NOT NULL, ['social_pacing'], [])
    ), ' | ') AS row_data_sources_available,
    COALESCE(
      NULLIF(ARRAY_TO_STRING(ARRAY_CONCAT(
        IF(planned_daily_spend IS NULL, ['missing_social_pacing'], []),
        IF(spend IS NULL AND impressions IS NULL AND clicks IS NULL, ['missing_final_metrics'], [])
      ), ' | '), ''),
      'ok'
    ) AS row_data_issue_category,
    CONCAT('social:', social_platform, ':', campaign_id, ':', ad_group_id) AS package_id_joined,
    date_day AS date,
    COALESCE(pacing_start_date, date_day) AS package_start_date,
    pacing_end_date AS package_end_date,
    account_name AS advertiser_name,
    CAST(NULL AS STRING) AS advertiser_short_name,
    campaign_name,
    campaign_name AS campaign_friendly,
    CAST(NULL AS STRING) AS product_code,
    CAST(NULL AS STRING) AS product_name,
    'SocialAdGroup' AS package_type,
    ad_group_name AS package_name,
    CAST(NULL AS STRING) AS p_package_friendly,
    'Social' AS gsMediaTeam_channel,
    ad_id AS placement_id,
    ad_name AS placement_name,
    UPPER(social_platform) AS supplier_code,
    social_platform AS supplier_name,
    CAST(NULL AS STRING) AS supplier_logo,
    'Social' AS buy_type,
    social_platform AS buy_category,
    CONCAT('social_', social_platform) AS channel,
    CONCAT('Social_', social_platform) AS channel_raw,
    'social' AS channel_group,
    'Social' AS media_name,
    CAST(NULL AS STRING) AS cost_method,
    CAST(NULL AS FLOAT64) AS planned_amount,
    CAST(NULL AS INT64) AS planned_impressions,
    CAST(NULL AS INT64) AS planned_units,
    CAST(NULL AS STRING) AS unit_type,
    CAST(NULL AS FLOAT64) AS payable_rate,
    SAFE_DIVIDE(planned_daily_spend, NULLIF(ad_rows_in_group_day, 0)) AS planned_daily_spend_pk,
    CAST(NULL AS FLOAT64) AS planned_daily_impressions_pk,
    CAST(NULL AS INT64) AS prisma_planned_clicks,
    CAST(NULL AS DATE) AS max_prisma_report_date,
    CAST(NULL AS FLOAT64) AS d_daily_recalculated_cost,
    CAST(NULL AS INT64) AS d_daily_recalculated_imps,
    CAST(NULL AS INT64) AS d_impressions,
    CAST(NULL AS FLOAT64) AS d_media_cost,
    CAST(NULL AS INT64) AS d_clicks,
    CAST(NULL AS INT64) AS d_video_plays,
    CAST(NULL AS INT64) AS d_video_comps,
    CAST(NULL AS DATE) AS d_min_date,
    CAST(NULL AS DATE) AS d_max_date,
    CAST(NULL AS DATE) AS d_min_flight_date,
    CAST(NULL AS DATE) AS d_max_flight_date,
    CAST(NULL AS FLOAT64) AS d_daily_cpm,
    CAST(NULL AS INT64) AS d_total_delivered_imps,
    CAST(NULL AS INT64) AS d_total_del_inflight_imps,
    CAST(NULL AS FLOAT64) AS fpd_orig_impressions,
    CAST(NULL AS FLOAT64) AS fpd_orig_spend,
    CAST(NULL AS FLOAT64) AS fpd_orig_clicks,
    CAST(NULL AS INT64) AS fpd_orig_sends,
    CAST(NULL AS INT64) AS fpd_orig_opens,
    CAST(NULL AS FLOAT64) AS fpd_orig_benchmark,
    CAST(NULL AS INT64) AS fpd_orig_benchmark_metric,
    CAST(NULL AS STRING) AS fpd_orig_creative,
    CAST(NULL AS STRING) AS fpd_orig_source_files,
    CAST(NULL AS STRING) AS fpd_orig_source_urls,
    CAST(NULL AS TIMESTAMP) AS fpd_orig_source_modified_time,
    CAST(NULL AS FLOAT64) AS fpd_updated_impressions,
    CAST(NULL AS FLOAT64) AS fpd_updated_spend,
    CAST(NULL AS STRING) AS fpd_updated_suppliers,
    CAST(NULL AS STRING) AS fpd_updated_initiatives,
    CAST(NULL AS TIMESTAMP) AS fpd_updated_data_timestamp,
    CAST(NULL AS TIMESTAMP) AS fpd_updated_source_sheet_modified_time,
    CAST(NULL AS FLOAT64) AS fpd_impressions,
    CAST(NULL AS FLOAT64) AS fpd_spend,
    CAST(NULL AS FLOAT64) AS fpd_clicks,
    CAST(NULL AS INT64) AS fpd_sends,
    CAST(NULL AS INT64) AS fpd_opens,
    spend AS final_spend,
    CAST(impressions AS FLOAT64) AS final_impressions,
    CAST(clicks AS FLOAT64) AS final_clicks,
    video_play AS final_video_plays,
    video_complete_proxy AS final_video_comps,
    CAST(NULL AS DATE) AS tv_data_refresh_date,
    CAST(NULL AS STRING) AS tv_media_outlet,
    CAST(NULL AS STRING) AS tv_type,
    CAST(NULL AS STRING) AS tv_program_name,
    CAST(NULL AS STRING) AS tv_market,
    CAST(NULL AS STRING) AS tv_quarter,
    CAST(NULL AS INT64) AS tv_year,
    CAST(NULL AS INT64) AS tv_net_impressions,
    CAST(NULL AS FLOAT64) AS tv_net_cost,
    CAST(NULL AS INT64) AS tv_total_units,
    social_platform,
    campaign_id AS social_campaign_id,
    ad_group_id AS social_ad_group_id,
    ad_id AS social_ad_id,
    account_name AS social_account_name,
    SAFE_DIVIDE(planned_daily_spend, NULLIF(ad_rows_in_group_day, 0)) AS social_pacing_planned_spend,
    spend AS social_spend,
    CAST(impressions AS FLOAT64) AS social_impressions,
    CAST(clicks AS FLOAT64) AS social_clicks,
    video_play AS social_video_plays,
    video_view AS social_video_views,
    video_complete_proxy AS social_video_comps
  FROM social_with_pacing
),

tv_base AS (
  SELECT
    DATE(t.date) AS date,
    t.data_refresh_date,
    NULLIF(TRIM(CAST(t.media_outlet AS STRING)), '') AS media_outlet,
    NULLIF(TRIM(CAST(t.campaign_name AS STRING)), '') AS campaign_name,
    NULLIF(TRIM(CAST(t.type AS STRING)), '') AS tv_type,
    NULLIF(TRIM(CAST(t.program_name AS STRING)), '') AS program_name,
    NULLIF(TRIM(CAST(t.advertiser AS STRING)), '') AS advertiser,
    NULLIF(TRIM(CAST(t.market AS STRING)), '') AS market,
    NULLIF(TRIM(CAST(t.quarter AS STRING)), '') AS quarter,
    SAFE_CAST(t.year AS INT64) AS year,
    SAFE_CAST(t.net_impressions AS INT64) AS net_impressions,
    SAFE_CAST(t.net_cost AS FLOAT64) AS net_cost,
    SAFE_CAST(t.total_units AS INT64) AS total_units,
    CONCAT(
      'tv_pkg:',
      SUBSTR(
        TO_HEX(MD5(CONCAT(
          COALESCE(LOWER(TRIM(CAST(t.advertiser AS STRING))), 'unknown_advertiser'), '|',
          COALESCE(LOWER(TRIM(CAST(t.campaign_name AS STRING))), 'unknown_campaign'), '|',
          COALESCE(LOWER(TRIM(CAST(t.type AS STRING))), 'unknown_type'), '|',
          COALESCE(LOWER(TRIM(CAST(t.media_outlet AS STRING))), 'unknown_outlet')
        ))),
        1,
        16
      )
    ) AS package_id_joined,
    CONCAT(
      'tv_plc:',
      SUBSTR(
        TO_HEX(MD5(CONCAT(
          COALESCE(LOWER(TRIM(CAST(t.advertiser AS STRING))), 'unknown_advertiser'), '|',
          COALESCE(LOWER(TRIM(CAST(t.campaign_name AS STRING))), 'unknown_campaign'), '|',
          COALESCE(LOWER(TRIM(CAST(t.type AS STRING))), 'unknown_type'), '|',
          COALESCE(LOWER(TRIM(CAST(t.media_outlet AS STRING))), 'unknown_outlet'), '|',
          COALESCE(LOWER(TRIM(CAST(t.program_name AS STRING))), 'unknown_program'), '|',
          COALESCE(LOWER(TRIM(CAST(t.market AS STRING))), 'unknown_market')
        ))),
        1,
        16
      )
    ) AS placement_id
  FROM `looker-studio-pro-452620.landing.tv_combined` AS t
  WHERE DATE(t.date) >= DATE '2025-01-01'
),

tv_daily AS (
  SELECT
    package_id_joined,
    placement_id,
    date,
    MAX(data_refresh_date) AS data_refresh_date,
    ARRAY_AGG(media_outlet IGNORE NULLS ORDER BY media_outlet LIMIT 1)[SAFE_OFFSET(0)] AS media_outlet,
    ARRAY_AGG(campaign_name IGNORE NULLS ORDER BY campaign_name LIMIT 1)[SAFE_OFFSET(0)] AS campaign_name,
    ARRAY_AGG(tv_type IGNORE NULLS ORDER BY tv_type LIMIT 1)[SAFE_OFFSET(0)] AS tv_type,
    ARRAY_AGG(program_name IGNORE NULLS ORDER BY program_name LIMIT 1)[SAFE_OFFSET(0)] AS program_name,
    ARRAY_AGG(advertiser IGNORE NULLS ORDER BY advertiser LIMIT 1)[SAFE_OFFSET(0)] AS advertiser,
    ARRAY_AGG(market IGNORE NULLS ORDER BY market LIMIT 1)[SAFE_OFFSET(0)] AS market,
    ARRAY_AGG(quarter IGNORE NULLS ORDER BY quarter LIMIT 1)[SAFE_OFFSET(0)] AS quarter,
    MAX(year) AS year,
    SUM(net_impressions) AS net_impressions,
    SUM(net_cost) AS net_cost,
    SUM(total_units) AS total_units
  FROM tv_base
  GROUP BY package_id_joined, placement_id, date
),

tv_with_package_dates AS (
  SELECT
    *,
    MIN(date) OVER (PARTITION BY package_id_joined) AS package_start_date,
    MAX(date) OVER (PARTITION BY package_id_joined) AS package_end_date
  FROM tv_daily
),

tv_final AS (
  SELECT
    'tv' AS row_type,
    'tv_combined' AS row_data_source_primary,
    'tv_combined' AS row_data_sources_available,
    CASE
      WHEN net_cost IS NULL AND net_impressions IS NULL THEN 'missing_final_metrics'
      ELSE 'ok'
    END AS row_data_issue_category,
    package_id_joined,
    date,
    package_start_date,
    package_end_date,
    advertiser AS advertiser_name,
    advertiser AS advertiser_short_name,
    campaign_name,
    campaign_name AS campaign_friendly,
    CAST(NULL AS STRING) AS product_code,
    CAST(NULL AS STRING) AS product_name,
    'TVPackage' AS package_type,
    CONCAT(
      COALESCE(advertiser, 'Unknown Advertiser'),
      ' | ',
      COALESCE(campaign_name, 'Unknown Campaign'),
      ' | ',
      COALESCE(tv_type, 'Unknown'),
      ' TV | ',
      COALESCE(media_outlet, 'Unknown Outlet')
    ) AS package_name,
    CONCAT(
      COALESCE(campaign_name, 'Unknown Campaign'),
      ' | ',
      COALESCE(tv_type, 'Unknown'),
      ' TV | ',
      COALESCE(media_outlet, 'Unknown Outlet')
    ) AS p_package_friendly,
    'Linear' AS gsMediaTeam_channel,
    placement_id,
    CONCAT(
      COALESCE(program_name, 'Unknown Program'),
      ' | ',
      COALESCE(market, 'Unknown Market'),
      ' | ',
      COALESCE(media_outlet, 'Unknown Outlet')
    ) AS placement_name,
    UPPER(REGEXP_REPLACE(COALESCE(media_outlet, 'UNKNOWN'), r'[^A-Za-z0-9]+', '_')) AS supplier_code,
    COALESCE(media_outlet, 'Unknown Outlet') AS supplier_name,
    CAST(NULL AS STRING) AS supplier_logo,
    'TV' AS buy_type,
    COALESCE(tv_type, 'Unknown') AS buy_category,
    'linear_tv' AS channel,
    CONCAT('TV_', COALESCE(tv_type, 'Unknown')) AS channel_raw,
    'linear' AS channel_group,
    'TV' AS media_name,
    CAST(NULL AS STRING) AS cost_method,
    CAST(NULL AS FLOAT64) AS planned_amount,
    CAST(NULL AS INT64) AS planned_impressions,
    CAST(NULL AS INT64) AS planned_units,
    CAST(NULL AS STRING) AS unit_type,
    CAST(NULL AS FLOAT64) AS payable_rate,
    CAST(NULL AS FLOAT64) AS planned_daily_spend_pk,
    CAST(NULL AS FLOAT64) AS planned_daily_impressions_pk,
    CAST(NULL AS INT64) AS prisma_planned_clicks,
    CAST(NULL AS DATE) AS max_prisma_report_date,
    CAST(NULL AS FLOAT64) AS d_daily_recalculated_cost,
    CAST(NULL AS INT64) AS d_daily_recalculated_imps,
    CAST(NULL AS INT64) AS d_impressions,
    CAST(NULL AS FLOAT64) AS d_media_cost,
    CAST(NULL AS INT64) AS d_clicks,
    CAST(NULL AS INT64) AS d_video_plays,
    CAST(NULL AS INT64) AS d_video_comps,
    CAST(NULL AS DATE) AS d_min_date,
    CAST(NULL AS DATE) AS d_max_date,
    CAST(NULL AS DATE) AS d_min_flight_date,
    CAST(NULL AS DATE) AS d_max_flight_date,
    CAST(NULL AS FLOAT64) AS d_daily_cpm,
    CAST(NULL AS INT64) AS d_total_delivered_imps,
    CAST(NULL AS INT64) AS d_total_del_inflight_imps,
    CAST(NULL AS FLOAT64) AS fpd_orig_impressions,
    CAST(NULL AS FLOAT64) AS fpd_orig_spend,
    CAST(NULL AS FLOAT64) AS fpd_orig_clicks,
    CAST(NULL AS INT64) AS fpd_orig_sends,
    CAST(NULL AS INT64) AS fpd_orig_opens,
    CAST(NULL AS FLOAT64) AS fpd_orig_benchmark,
    CAST(NULL AS INT64) AS fpd_orig_benchmark_metric,
    CAST(NULL AS STRING) AS fpd_orig_creative,
    CAST(NULL AS STRING) AS fpd_orig_source_files,
    CAST(NULL AS STRING) AS fpd_orig_source_urls,
    CAST(NULL AS TIMESTAMP) AS fpd_orig_source_modified_time,
    CAST(NULL AS FLOAT64) AS fpd_updated_impressions,
    CAST(NULL AS FLOAT64) AS fpd_updated_spend,
    CAST(NULL AS STRING) AS fpd_updated_suppliers,
    CAST(NULL AS STRING) AS fpd_updated_initiatives,
    CAST(NULL AS TIMESTAMP) AS fpd_updated_data_timestamp,
    CAST(NULL AS TIMESTAMP) AS fpd_updated_source_sheet_modified_time,
    CAST(NULL AS FLOAT64) AS fpd_impressions,
    CAST(NULL AS FLOAT64) AS fpd_spend,
    CAST(NULL AS FLOAT64) AS fpd_clicks,
    CAST(NULL AS INT64) AS fpd_sends,
    CAST(NULL AS INT64) AS fpd_opens,
    net_cost AS final_spend,
    CAST(net_impressions AS FLOAT64) AS final_impressions,
    CAST(NULL AS FLOAT64) AS final_clicks,
    CAST(NULL AS FLOAT64) AS final_video_plays,
    CAST(NULL AS FLOAT64) AS final_video_comps,
    data_refresh_date AS tv_data_refresh_date,
    media_outlet AS tv_media_outlet,
    tv_type,
    program_name AS tv_program_name,
    market AS tv_market,
    quarter AS tv_quarter,
    year AS tv_year,
    net_impressions AS tv_net_impressions,
    net_cost AS tv_net_cost,
    total_units AS tv_total_units,
    CAST(NULL AS STRING) AS social_platform,
    CAST(NULL AS STRING) AS social_campaign_id,
    CAST(NULL AS STRING) AS social_ad_group_id,
    CAST(NULL AS STRING) AS social_ad_id,
    CAST(NULL AS STRING) AS social_account_name,
    CAST(NULL AS FLOAT64) AS social_pacing_planned_spend,
    CAST(NULL AS FLOAT64) AS social_spend,
    CAST(NULL AS FLOAT64) AS social_impressions,
    CAST(NULL AS FLOAT64) AS social_clicks,
    CAST(NULL AS FLOAT64) AS social_video_plays,
    CAST(NULL AS FLOAT64) AS social_video_views,
    CAST(NULL AS FLOAT64) AS social_video_comps
  FROM tv_with_package_dates
),

all_rows AS (
  SELECT * FROM digital_final
  UNION ALL
  SELECT * FROM social_final
  UNION ALL
  SELECT * FROM tv_final
),

with_rollups AS (
  SELECT
    *,
    SUM(COALESCE(planned_daily_spend_pk, 0)) OVER (PARTITION BY package_id_joined) AS pkg_est_spend,
    SUM(COALESCE(planned_daily_impressions_pk, 0)) OVER (PARTITION BY package_id_joined) AS pkg_est_impressions,
    SUM(COALESCE(final_spend, 0)) OVER (PARTITION BY package_id_joined) AS pkg_act_spend,
    SUM(COALESCE(final_impressions, 0)) OVER (PARTITION BY package_id_joined) AS pkg_act_impressions,
    SUM(COALESCE(final_clicks, 0)) OVER (PARTITION BY package_id_joined) AS pkg_act_clicks,
    SUM(COALESCE(fpd_orig_impressions, 0)) OVER (PARTITION BY package_id_joined) AS pkg_fpd_orig_impressions,
    SUM(COALESCE(fpd_orig_spend, 0)) OVER (PARTITION BY package_id_joined) AS pkg_fpd_orig_spend,
    SUM(COALESCE(fpd_updated_impressions, 0)) OVER (PARTITION BY package_id_joined) AS pkg_fpd_updated_impressions,
    SUM(COALESCE(fpd_updated_spend, 0)) OVER (PARTITION BY package_id_joined) AS pkg_fpd_updated_spend,
    SUM(COALESCE(fpd_impressions, 0)) OVER (PARTITION BY package_id_joined) AS pkg_fpd_combined_impressions,
    SUM(COALESCE(fpd_spend, 0)) OVER (PARTITION BY package_id_joined) AS pkg_fpd_combined_spend
  FROM all_rows
)

SELECT
  *,
  NULLIF(row_data_issue_category, 'ok') AS row_data_callouts,
  CASE
    WHEN pkg_est_spend = 0 THEN NULL
    ELSE pkg_act_spend > pkg_est_spend
  END AS pkg_over_bool,
  CASE
    WHEN pkg_est_spend = 0 THEN NULL
    WHEN pkg_act_spend > pkg_est_spend THEN 1
    ELSE 0
  END AS pkg_over_flag,
  CURRENT_TIMESTAMP() AS model_view_runtime_timestamp
FROM with_rollups;
