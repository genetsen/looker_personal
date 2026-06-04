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
    f.creative_git_link,
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
    STRING_AGG(DISTINCT CAST(creative_git_link AS STRING), ' | ' ORDER BY CAST(creative_git_link AS STRING)) AS fpd_creative_img,
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

manual_package_daily AS (
  SELECT
    package_id,
    DATE(date) AS date,
    man_start_date,
    man_end_date,
    man_daily_spend,
    man_daily_impressions,
    man_daily_planned_spend,
    man_daily_planned_impressions,
    man_daily_clicks,
    man_daily_video_plays,
    man_daily_video_comps,
    man_total_spend_doNotSum,
    man_total_impressions_doNotSum,
    man_total_planned_spend_doNotSum,
    man_total_planned_impressions_doNotSum,
    man_total_clicks_doNotSum,
    man_total_video_plays_doNotSum,
    man_total_video_comps_doNotSum,
    advertiser_name,
    advertiser_short_name,
    campaign_name,
    campaign_friendly,
    product_code,
    product_name,
    package_type,
    package_name,
    package_name_friendly,
    ADIF_channel,
    placement_id,
    placement_name,
    supplier_code,
    supplier_name,
    supplier_logo,
    p_buy_type,
    p_buy_category,
    channel,
    channel_raw,
    channel_group,
    media_name,
    p_cost_method,
    p_planned_amount_doNotSum,
    p_planned_impressions_doNotSum,
    p_planned_units_doNotSum,
    p_unit_type,
    p_rate,
    edit_id,
    edit_reason,
    editor_email,
    source_sheet_url,
    source_sheet_modified_time,
    loaded_at
  FROM `looker-studio-pro-452620.landing.master_data_model_manual_package_daily`
  WHERE is_active = TRUE
    AND validation_status = 'valid'
    AND package_id IS NOT NULL
    AND date IS NOT NULL
),

manual_package_metadata AS (
  SELECT
    package_id,
    ARRAY_AGG(edit_id IGNORE NULLS ORDER BY loaded_at DESC, edit_id DESC LIMIT 1)[SAFE_OFFSET(0)] AS edit_id,
    ARRAY_AGG(edit_reason IGNORE NULLS ORDER BY loaded_at DESC, edit_id DESC LIMIT 1)[SAFE_OFFSET(0)] AS edit_reason,
    ARRAY_AGG(editor_email IGNORE NULLS ORDER BY loaded_at DESC, edit_id DESC LIMIT 1)[SAFE_OFFSET(0)] AS editor_email,
    ARRAY_AGG(source_sheet_url IGNORE NULLS ORDER BY loaded_at DESC, edit_id DESC LIMIT 1)[SAFE_OFFSET(0)] AS source_sheet_url,
    ARRAY_AGG(source_sheet_modified_time IGNORE NULLS ORDER BY loaded_at DESC, edit_id DESC LIMIT 1)[SAFE_OFFSET(0)] AS source_sheet_modified_time,
    MAX(loaded_at) AS loaded_at,
    ARRAY_AGG(replacement_flight_start_date IGNORE NULLS ORDER BY loaded_at DESC, edit_id DESC LIMIT 1)[SAFE_OFFSET(0)] AS man_flight_start_date,
    ARRAY_AGG(replacement_flight_end_date IGNORE NULLS ORDER BY loaded_at DESC, edit_id DESC LIMIT 1)[SAFE_OFFSET(0)] AS man_flight_end_date,
    ARRAY_AGG(man_advertiser_name IGNORE NULLS ORDER BY loaded_at DESC, edit_id DESC LIMIT 1)[SAFE_OFFSET(0)] AS advertiser_name,
    ARRAY_AGG(man_campaign_name IGNORE NULLS ORDER BY loaded_at DESC, edit_id DESC LIMIT 1)[SAFE_OFFSET(0)] AS campaign_name,
    ARRAY_AGG(man_package_type IGNORE NULLS ORDER BY loaded_at DESC, edit_id DESC LIMIT 1)[SAFE_OFFSET(0)] AS package_type,
    ARRAY_AGG(man_package_name IGNORE NULLS ORDER BY loaded_at DESC, edit_id DESC LIMIT 1)[SAFE_OFFSET(0)] AS package_name,
    ARRAY_AGG(man_package_name_friendly IGNORE NULLS ORDER BY loaded_at DESC, edit_id DESC LIMIT 1)[SAFE_OFFSET(0)] AS package_name_friendly,
    ARRAY_AGG(man_ADIF_channel IGNORE NULLS ORDER BY loaded_at DESC, edit_id DESC LIMIT 1)[SAFE_OFFSET(0)] AS ADIF_channel,
    ARRAY_AGG(man_supplier_code IGNORE NULLS ORDER BY loaded_at DESC, edit_id DESC LIMIT 1)[SAFE_OFFSET(0)] AS supplier_code,
    ARRAY_AGG(man_supplier_name IGNORE NULLS ORDER BY loaded_at DESC, edit_id DESC LIMIT 1)[SAFE_OFFSET(0)] AS supplier_name,
    ARRAY_AGG(man_channel IGNORE NULLS ORDER BY loaded_at DESC, edit_id DESC LIMIT 1)[SAFE_OFFSET(0)] AS channel,
    ARRAY_AGG(man_channel IGNORE NULLS ORDER BY loaded_at DESC, edit_id DESC LIMIT 1)[SAFE_OFFSET(0)] AS channel_group,
    ARRAY_AGG(man_package_type IGNORE NULLS ORDER BY loaded_at DESC, edit_id DESC LIMIT 1)[SAFE_OFFSET(0)] AS media_name,
    ARRAY_AGG(man_initiative IGNORE NULLS ORDER BY loaded_at DESC, edit_id DESC LIMIT 1)[SAFE_OFFSET(0)] AS initiative
  FROM `looker-studio-pro-452620.landing.master_data_model_manual_package_edits_raw`
  WHERE is_active = TRUE
    AND validation_status = 'valid'
    AND package_id IS NOT NULL
    AND (
      replacement_flight_start_date IS NOT NULL
      OR replacement_flight_end_date IS NOT NULL
      OR man_advertiser_name IS NOT NULL
      OR man_campaign_name IS NOT NULL
      OR man_package_type IS NOT NULL
      OR man_package_name IS NOT NULL
      OR man_package_name_friendly IS NOT NULL
      OR man_ADIF_channel IS NOT NULL
      OR man_supplier_code IS NOT NULL
      OR man_supplier_name IS NOT NULL
      OR man_channel IS NOT NULL
      OR man_initiative IS NOT NULL
    )
  GROUP BY package_id
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
    fpd_creative_img,
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
    CAST(NULL AS FLOAT64) AS social_video_comps,
    CAST(NULL AS STRING) AS social_creative_name,
    CAST(NULL AS STRING) AS man_creative_img,
    CAST(NULL AS STRING) AS social_classification_source,
    CAST(NULL AS STRING) AS social_publication_status,
    CAST(NULL AS STRING) AS social_source_sheet_url,
    CAST(NULL AS TIMESTAMP) AS social_loaded_at,
    CAST(NULL AS STRING) AS social_row_key,
    CAST(NULL AS STRING) AS social_record_source,
    CAST(NULL AS STRING) AS social_fallback_fields
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
    SUM(s.video_views_p_100) AS video_complete_proxy,
    ANY_VALUE(s.wp_channel) AS wp_channel,
    ANY_VALUE(s.wp_channel_group) AS wp_channel_group,
    ANY_VALUE(s.wp_media_name) AS wp_media_name,
    ANY_VALUE(s.wp_ADIF_channel) AS wp_ADIF_channel,
    ANY_VALUE(s.wp_classification_source) AS wp_classification_source,
    ANY_VALUE(s.wp_publication_status) AS wp_publication_status,
    ANY_VALUE(s.wp_creative_name) AS wp_creative_name,
    ANY_VALUE(s.wp_creative_img) AS wp_creative_img,
    ANY_VALUE(s.wp_source_sheet_url) AS wp_source_sheet_url,
    ANY_VALUE(s.wp_loaded_at) AS wp_loaded_at,
    ANY_VALUE(s.wp_row_key) AS wp_row_key,
    ANY_VALUE(s.wp_record_source) AS wp_record_source,
    ANY_VALUE(s.wp_fallback_fields) AS wp_fallback_fields
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
  -- LIVE RECONCILIATION 2026-05-27: Preserve the production scheduled-source
  -- change observed before the WP workbook addition; do not redeploy the older view.
  FROM looker-studio-pro-452620.repo_int.crossplatform_pacing_tbl
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
    IF(STARTS_WITH(COALESCE(wp_record_source, ''), 'wp_'), 'wp_search_data_template', 'social') AS row_data_source_primary,
    ARRAY_TO_STRING(ARRAY_CONCAT(
      IF(STARTS_WITH(COALESCE(wp_record_source, ''), 'wp_'), ['wp_search_data_template'], ['social']),
      IF(REGEXP_CONTAINS(COALESCE(wp_record_source, ''), r'wp_primary'), ['social'], []),
      IF(planned_daily_spend IS NOT NULL, ['social_pacing'], [])
    ), ' | ') AS row_data_sources_available,
    COALESCE(
      NULLIF(ARRAY_TO_STRING(ARRAY_CONCAT(
        IF(wp_publication_status = 'publish_pending_source_owner_review', ['pending_wp_source_owner_review'], []),
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
    COALESCE(wp_ADIF_channel, 'Social') AS gsMediaTeam_channel,
    ad_id AS placement_id,
    ad_name AS placement_name,
    CASE
      WHEN wp_classification_source = 'campaign_marker_youtube'
        OR wp_media_name = 'Online Video' THEN 'YT'
      ELSE UPPER(social_platform)
    END AS supplier_code,
    CASE
      WHEN wp_classification_source = 'campaign_marker_youtube'
        OR wp_media_name = 'Online Video' THEN 'Youtube'
      ELSE social_platform
    END AS supplier_name,
    CAST(NULL AS STRING) AS supplier_logo,
    'Social' AS buy_type,
    social_platform AS buy_category,
    COALESCE(wp_channel, CONCAT('social_', social_platform)) AS channel,
    COALESCE(wp_channel, CONCAT('Social_', social_platform)) AS channel_raw,
    COALESCE(wp_channel_group, 'social') AS channel_group,
    COALESCE(wp_media_name, 'Social') AS media_name,
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
    CAST(NULL AS STRING) AS fpd_creative_img,
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
    video_complete_proxy AS social_video_comps,
    ad_name AS social_creative_name,
    wp_creative_img AS man_creative_img,
    wp_classification_source AS social_classification_source,
    wp_publication_status AS social_publication_status,
    wp_source_sheet_url AS social_source_sheet_url,
    wp_loaded_at AS social_loaded_at,
    wp_row_key AS social_row_key,
    wp_record_source AS social_record_source,
    wp_fallback_fields AS social_fallback_fields
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
    -- CHANGE 2026-05-08: Linear TV net cost/impressions now populate planned
    -- daily fields so `_planned_spend` and `_planned_impressions` match the
    -- TV values already carried in `_spend`, `_impressions`, and `tv_*`.
    net_cost AS planned_daily_spend_pk,
    CAST(net_impressions AS FLOAT64) AS planned_daily_impressions_pk,
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
    CAST(NULL AS STRING) AS fpd_creative_img,
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
    CAST(NULL AS FLOAT64) AS social_video_comps,
    CAST(NULL AS STRING) AS social_creative_name,
    CAST(NULL AS STRING) AS man_creative_img,
    CAST(NULL AS STRING) AS social_classification_source,
    CAST(NULL AS STRING) AS social_publication_status,
    CAST(NULL AS STRING) AS social_source_sheet_url,
    CAST(NULL AS TIMESTAMP) AS social_loaded_at,
    CAST(NULL AS STRING) AS social_row_key,
    CAST(NULL AS STRING) AS social_record_source,
    CAST(NULL AS STRING) AS social_fallback_fields
  FROM tv_with_package_dates
),

all_rows AS (
  SELECT * FROM digital_final
  UNION ALL
  SELECT * FROM social_final
  UNION ALL
  SELECT * FROM tv_final
),

manual_existing_rows AS (
  SELECT
    r.* REPLACE (
      IF(COALESCE(m.edit_id, pm.edit_id) IS NOT NULL, 'manual_package_edits', r.row_data_source_primary) AS row_data_source_primary,
      CASE
        WHEN COALESCE(m.edit_id, pm.edit_id) IS NULL THEN r.row_data_sources_available
        ELSE ARRAY_TO_STRING(
          ARRAY_CONCAT(
            IF(COALESCE(r.row_data_sources_available, 'none') IN ('', 'none'), ARRAY<STRING>[], SPLIT(r.row_data_sources_available, ' | ')),
            ['manual_package_edits']
          ),
          ' | '
        )
      END AS row_data_sources_available,
      COALESCE(pm.man_flight_start_date, r.package_start_date) AS package_start_date,
      COALESCE(pm.man_flight_end_date, r.package_end_date) AS package_end_date,
      COALESCE(pm.advertiser_name, r.advertiser_name) AS advertiser_name,
      r.advertiser_short_name AS advertiser_short_name,
      COALESCE(pm.campaign_name, r.campaign_name) AS campaign_name,
      COALESCE(m.campaign_friendly, r.campaign_friendly) AS campaign_friendly,
      COALESCE(m.product_code, r.product_code) AS product_code,
      COALESCE(m.product_name, r.product_name) AS product_name,
      COALESCE(pm.package_type, r.package_type) AS package_type,
      COALESCE(pm.package_name, r.package_name) AS package_name,
      COALESCE(pm.package_name_friendly, r.p_package_friendly) AS p_package_friendly,
      COALESCE(pm.ADIF_channel, r.gsMediaTeam_channel) AS gsMediaTeam_channel,
      COALESCE(m.placement_id, r.placement_id) AS placement_id,
      COALESCE(m.placement_name, r.placement_name) AS placement_name,
      COALESCE(pm.supplier_code, r.supplier_code) AS supplier_code,
      COALESCE(pm.supplier_name, r.supplier_name) AS supplier_name,
      COALESCE(m.supplier_logo, r.supplier_logo) AS supplier_logo,
      COALESCE(m.p_buy_type, r.buy_type) AS buy_type,
      COALESCE(m.p_buy_category, r.buy_category) AS buy_category,
      COALESCE(pm.channel, r.channel) AS channel,
      COALESCE(m.channel_raw, r.channel_raw) AS channel_raw,
      COALESCE(pm.channel_group, r.channel_group) AS channel_group,
      COALESCE(pm.media_name, r.media_name) AS media_name,
      COALESCE(m.p_cost_method, r.cost_method) AS cost_method,
      COALESCE(m.man_total_planned_spend_doNotSum, m.p_planned_amount_doNotSum, r.planned_amount) AS planned_amount,
      COALESCE(SAFE_CAST(ROUND(COALESCE(m.man_total_planned_impressions_doNotSum, m.p_planned_impressions_doNotSum)) AS INT64), r.planned_impressions) AS planned_impressions,
      COALESCE(SAFE_CAST(ROUND(m.p_planned_units_doNotSum) AS INT64), r.planned_units) AS planned_units,
      COALESCE(m.p_unit_type, r.unit_type) AS unit_type,
      COALESCE(m.p_rate, r.payable_rate) AS payable_rate,
      COALESCE(m.man_daily_planned_spend, r.planned_daily_spend_pk) AS planned_daily_spend_pk,
      COALESCE(m.man_daily_planned_impressions, r.planned_daily_impressions_pk) AS planned_daily_impressions_pk,
      COALESCE(m.man_daily_spend, r.final_spend) AS final_spend,
      COALESCE(m.man_daily_impressions, r.final_impressions) AS final_impressions,
      COALESCE(m.man_daily_clicks, r.final_clicks) AS final_clicks,
      COALESCE(m.man_daily_video_plays, r.final_video_plays) AS final_video_plays,
      COALESCE(m.man_daily_video_comps, r.final_video_comps) AS final_video_comps
    ),
    COALESCE(m.edit_id, pm.edit_id) AS man_edit_id,
    COALESCE(m.edit_reason, pm.edit_reason) AS man_edit_reason,
    COALESCE(m.editor_email, pm.editor_email) AS man_editor_email,
    COALESCE(m.source_sheet_url, pm.source_sheet_url) AS man_source_sheet_url,
    COALESCE(m.source_sheet_modified_time, pm.source_sheet_modified_time) AS man_source_sheet_modified_time,
    COALESCE(m.loaded_at, pm.loaded_at) AS man_loaded_at,
    m.man_start_date,
    m.man_end_date,
    m.man_daily_spend,
    m.man_daily_impressions,
    m.man_daily_planned_spend,
    m.man_daily_planned_impressions,
    m.man_daily_clicks,
    m.man_daily_video_plays,
    m.man_daily_video_comps,
    m.man_total_spend_doNotSum,
    m.man_total_impressions_doNotSum,
    m.man_total_planned_spend_doNotSum,
    m.man_total_planned_impressions_doNotSum,
    m.man_total_clicks_doNotSum,
    m.man_total_video_plays_doNotSum,
    m.man_total_video_comps_doNotSum
  FROM all_rows AS r
  LEFT JOIN manual_package_daily AS m
    ON r.package_id_joined = m.package_id
   AND r.date = m.date
  LEFT JOIN manual_package_metadata AS pm
    ON r.package_id_joined = pm.package_id
),

manual_only_rows AS (
  SELECT
    'manual' AS row_type,
    'manual_package_edits' AS row_data_source_primary,
    'manual_package_edits' AS row_data_sources_available,
    'ok' AS row_data_issue_category,
    m.package_id AS package_id_joined,
    m.date,
    COALESCE(pm.man_flight_start_date, m.man_start_date) AS package_start_date,
    COALESCE(pm.man_flight_end_date, m.man_end_date) AS package_end_date,
    COALESCE(pm.advertiser_name, m.advertiser_name) AS advertiser_name,
    m.advertiser_short_name,
    COALESCE(pm.campaign_name, m.campaign_name) AS campaign_name,
    m.campaign_friendly,
    m.product_code,
    m.product_name,
    COALESCE(pm.package_type, m.package_type, 'ManualPackage') AS package_type,
    COALESCE(pm.package_name, m.package_name) AS package_name,
    COALESCE(pm.package_name_friendly, m.package_name_friendly) AS p_package_friendly,
    COALESCE(pm.ADIF_channel, m.ADIF_channel) AS gsMediaTeam_channel,
    COALESCE(m.placement_id, m.package_id) AS placement_id,
    COALESCE(m.placement_name, m.package_name, m.package_id) AS placement_name,
    COALESCE(pm.supplier_code, m.supplier_code) AS supplier_code,
    COALESCE(pm.supplier_name, m.supplier_name) AS supplier_name,
    m.supplier_logo,
    m.p_buy_type AS buy_type,
    m.p_buy_category AS buy_category,
    COALESCE(pm.channel, m.channel) AS channel,
    m.channel_raw,
    COALESCE(pm.channel_group, m.channel_group) AS channel_group,
    COALESCE(pm.media_name, m.media_name) AS media_name,
    m.p_cost_method AS cost_method,
    COALESCE(m.man_total_planned_spend_doNotSum, m.p_planned_amount_doNotSum) AS planned_amount,
    SAFE_CAST(ROUND(COALESCE(m.man_total_planned_impressions_doNotSum, m.p_planned_impressions_doNotSum)) AS INT64) AS planned_impressions,
    SAFE_CAST(ROUND(m.p_planned_units_doNotSum) AS INT64) AS planned_units,
    m.p_unit_type AS unit_type,
    m.p_rate AS payable_rate,
    m.man_daily_planned_spend AS planned_daily_spend_pk,
    m.man_daily_planned_impressions AS planned_daily_impressions_pk,
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
    CAST(NULL AS STRING) AS fpd_creative_img,
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
    m.man_daily_spend AS final_spend,
    m.man_daily_impressions AS final_impressions,
    m.man_daily_clicks AS final_clicks,
    m.man_daily_video_plays AS final_video_plays,
    m.man_daily_video_comps AS final_video_comps,
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
    CAST(NULL AS FLOAT64) AS social_video_comps,
    CAST(NULL AS STRING) AS social_creative_name,
    CAST(NULL AS STRING) AS man_creative_img,
    CAST(NULL AS STRING) AS social_classification_source,
    CAST(NULL AS STRING) AS social_publication_status,
    CAST(NULL AS STRING) AS social_source_sheet_url,
    CAST(NULL AS TIMESTAMP) AS social_loaded_at,
    CAST(NULL AS STRING) AS social_row_key,
    CAST(NULL AS STRING) AS social_record_source,
    CAST(NULL AS STRING) AS social_fallback_fields,
    COALESCE(m.edit_id, pm.edit_id) AS man_edit_id,
    COALESCE(m.edit_reason, pm.edit_reason) AS man_edit_reason,
    COALESCE(m.editor_email, pm.editor_email) AS man_editor_email,
    COALESCE(m.source_sheet_url, pm.source_sheet_url) AS man_source_sheet_url,
    COALESCE(m.source_sheet_modified_time, pm.source_sheet_modified_time) AS man_source_sheet_modified_time,
    COALESCE(m.loaded_at, pm.loaded_at) AS man_loaded_at,
    m.man_start_date,
    m.man_end_date,
    m.man_daily_spend,
    m.man_daily_impressions,
    m.man_daily_planned_spend,
    m.man_daily_planned_impressions,
    m.man_daily_clicks,
    m.man_daily_video_plays,
    m.man_daily_video_comps,
    m.man_total_spend_doNotSum,
    m.man_total_impressions_doNotSum,
    m.man_total_planned_spend_doNotSum,
    m.man_total_planned_impressions_doNotSum,
    m.man_total_clicks_doNotSum,
    m.man_total_video_plays_doNotSum,
    m.man_total_video_comps_doNotSum
  FROM manual_package_daily AS m
  LEFT JOIN (
    SELECT DISTINCT package_id_joined, date
    FROM all_rows
  ) AS existing
    ON m.package_id = existing.package_id_joined
   AND m.date = existing.date
  LEFT JOIN manual_package_metadata AS pm
    ON m.package_id = pm.package_id
  WHERE existing.package_id_joined IS NULL
),

manual_applied_rows AS (
  SELECT * FROM manual_existing_rows
  UNION ALL
  SELECT * FROM manual_only_rows
),

planned_metric_backfills AS (
  SELECT
    * REPLACE (
      CASE
        -- CHANGE 2026-05-08: If a row clearly delivered media and had planned
        -- spend, but planned impressions are zero or missing, use final
        -- impressions as the planned-impressions fallback. This preserves the
        -- spend plan while preventing a paid/delivered row from carrying an
        -- impossible zero-impression plan. Null planned impressions are treated
        -- like zero here because downstream QA callouts also evaluate missing
        -- planned impressions as zero for mismatch detection.
        WHEN COALESCE(final_impressions, 0) > 0
          AND COALESCE(final_spend, 0) > 0
          AND COALESCE(planned_daily_impressions_pk, 0) = 0
          AND COALESCE(planned_daily_spend_pk, 0) > 0
          THEN final_impressions
        ELSE planned_daily_impressions_pk
      END AS planned_daily_impressions_pk
    )
  FROM manual_applied_rows
),

row_callouts AS (
  SELECT
    * REPLACE (
      COALESCE(
        NULLIF(ARRAY_TO_STRING(ARRAY_CONCAT(
          -- CHANGE 2026-05-08: Keep source/cause callouts first, then add only
          -- metric-symptom callouts that still add information. For example,
          -- `missing_actuals` already explains planned delivery with no actuals,
          -- so it suppresses the planned-without-actual symptom labels. Likewise,
          -- missing Prisma daily or social pacing already explains actuals that
          -- have no planned values, so those suppress actual-without-plan symptom
          -- labels. Standalone metric symptoms still surface with shorter labels.
          IF('missing_prisma_package' IN UNNEST(SPLIT(COALESCE(row_data_issue_category, ''), ' | ')), ['missing_prisma_package'], []),
          IF('missing_prisma_daily' IN UNNEST(SPLIT(COALESCE(row_data_issue_category, ''), ' | ')), ['missing_prisma_daily'], []),
          IF('missing_social_pacing' IN UNNEST(SPLIT(COALESCE(row_data_issue_category, ''), ' | ')), ['missing_social_pacing'], []),
          IF('missing_actuals' IN UNNEST(SPLIT(COALESCE(row_data_issue_category, ''), ' | ')), ['missing_actuals'], []),
          IF('actual_source_conflict' IN UNNEST(SPLIT(COALESCE(row_data_issue_category, ''), ' | ')), ['actual_source_conflict'], []),
          IF('low_signal_dcm' IN UNNEST(SPLIT(COALESCE(row_data_issue_category, ''), ' | ')), ['low_signal_dcm'], []),
          IF('missing_final_metrics' IN UNNEST(SPLIT(COALESCE(row_data_issue_category, ''), ' | ')), ['missing_final_metrics'], []),
          IF('pending_wp_source_owner_review' IN UNNEST(SPLIT(COALESCE(row_data_issue_category, ''), ' | ')), ['pending_wp_source_owner_review'], []),
          IF(
            COALESCE(planned_daily_impressions_pk, 0) = 0
              AND COALESCE(final_impressions, 0) = 0
              AND COALESCE(planned_daily_spend_pk, 0) > 0
              AND COALESCE(final_spend, 0) > 0,
            ['spend_no_imps'],
            []
          ),
          IF(
            COALESCE(final_impressions, 0) > 0
              AND COALESCE(planned_daily_impressions_pk, 0) = 0
              AND 'missing_prisma_daily' NOT IN UNNEST(SPLIT(COALESCE(row_data_issue_category, ''), ' | '))
              AND 'missing_social_pacing' NOT IN UNNEST(SPLIT(COALESCE(row_data_issue_category, ''), ' | ')),
            ['actual_imps_no_plan'],
            []
          ),
          IF(
            COALESCE(final_spend, 0) > 0
              AND COALESCE(planned_daily_spend_pk, 0) = 0
              AND 'missing_prisma_daily' NOT IN UNNEST(SPLIT(COALESCE(row_data_issue_category, ''), ' | '))
              AND 'missing_social_pacing' NOT IN UNNEST(SPLIT(COALESCE(row_data_issue_category, ''), ' | ')),
            ['actual_spend_no_plan'],
            []
          ),
          IF(
            COALESCE(planned_daily_impressions_pk, 0) > 0
              AND COALESCE(final_impressions, 0) = 0
              AND 'missing_actuals' NOT IN UNNEST(SPLIT(COALESCE(row_data_issue_category, ''), ' | ')),
            ['planned_imps_no_actual'],
            []
          ),
          IF(
            COALESCE(planned_daily_spend_pk, 0) > 0
              AND COALESCE(final_spend, 0) = 0
              AND 'missing_actuals' NOT IN UNNEST(SPLIT(COALESCE(row_data_issue_category, ''), ' | ')),
            ['planned_spend_no_actual'],
            []
          )
        ), ' | '), ''),
        'ok'
      ) AS row_data_issue_category
    )
  FROM planned_metric_backfills
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
  FROM row_callouts
),

with_initiative AS (
  SELECT
    wr.*,
    -- CHANGE 2026-05-15: Consolidate the v2 initiative field into the main
    -- package/date model so data_model owns the reporting column directly.
    COALESCE(mpm.initiative, pm.initative, wr.package_name) AS initiative
  FROM with_rollups AS wr
  LEFT JOIN prisma_meta AS pm
    ON wr.package_id_joined = pm.package_id
  LEFT JOIN manual_package_metadata AS mpm
    ON wr.package_id_joined = mpm.package_id
)

SELECT
  row_type AS `qa_row_type`,
  row_data_source_primary AS `qa_row_data_source_primary`,
  row_data_sources_available AS `qa_row_data_sources_available`,
  row_data_issue_category AS `qa_row_data_issue_category`,
  package_id_joined AS `_package_id`,
  date AS `_date`,
  package_start_date AS `_start_date`,
  package_end_date AS `_end_date`,
  advertiser_name AS `_advertiser_name`,
  advertiser_short_name AS `_advertiser_short_name`,
  CASE
    WHEN UPPER(COALESCE(advertiser_short_name, '')) = 'APO'
      OR UPPER(COALESCE(advertiser_name, '')) = 'APO'
      OR REGEXP_CONTAINS(UPPER(COALESCE(advertiser_name, '')), r'APOLLO|apollo') THEN 'Apollo'
    WHEN UPPER(COALESCE(advertiser_short_name, '')) = 'OLI'
      OR UPPER(COALESCE(advertiser_name, '')) = 'OLI'
      OR REGEXP_CONTAINS(UPPER(COALESCE(advertiser_name, '')), r'OLIPOP') THEN 'Olipop'
    WHEN UPPER(COALESCE(advertiser_short_name, '')) = 'MASS'
      OR UPPER(COALESCE(advertiser_name, '')) = 'MASS'
      OR REGEXP_CONTAINS(UPPER(COALESCE(advertiser_name, '')), r'MASSMUTUAL') THEN 'MassMutual'
    WHEN UPPER(COALESCE(advertiser_short_name, '')) = 'FMUS'
      OR REGEXP_CONTAINS(UPPER(COALESCE(advertiser_name, '')), r'(ADIF|DIAMOND|FOREVERMARK|DE BEERS)') THEN 'A Diamond Is Forever'
    WHEN UPPER(COALESCE(advertiser_short_name, '')) = 'GEA'
      OR UPPER(COALESCE(advertiser_name, '')) = 'GEA'
      OR REGEXP_CONTAINS(UPPER(COALESCE(advertiser_name, '')), r'GE AEROSPACE') THEN 'GE Aerospace'
    WHEN UPPER(COALESCE(advertiser_short_name, '')) = 'RTL'
      OR UPPER(COALESCE(advertiser_name, '')) = 'RTL'
      OR REGEXP_CONTAINS(UPPER(COALESCE(advertiser_name, '')), r'RITUAL') THEN 'Ritual'
    WHEN UPPER(COALESCE(advertiser_short_name, '')) = 'ICE'
      OR UPPER(COALESCE(advertiser_name, '')) = 'ICE'
      OR REGEXP_CONTAINS(UPPER(COALESCE(advertiser_name, '')), r'INTERCONTINENTAL EXCHANGE') THEN 'ICE'
    WHEN UPPER(COALESCE(advertiser_short_name, '')) = 'ADSK'
      OR UPPER(COALESCE(advertiser_name, '')) = 'ADSK'
      OR REGEXP_CONTAINS(UPPER(COALESCE(advertiser_name, '')), r'AUTODESK') THEN 'Autodesk'
    WHEN UPPER(COALESCE(advertiser_short_name, '')) = 'ITR'
      OR REGEXP_CONTAINS(UPPER(COALESCE(advertiser_name, '')), r'CUMBERLAND') THEN 'Cumberland Packing'
    WHEN UPPER(COALESCE(advertiser_short_name, '')) = 'NBC'
      OR REGEXP_CONTAINS(UPPER(COALESCE(advertiser_name, '')), r'NBC') THEN 'NBC Entertainment'
    WHEN UPPER(COALESCE(advertiser_short_name, '')) = 'SYNC'
      OR REGEXP_CONTAINS(UPPER(COALESCE(advertiser_name, '')), r'SYNCHRONY') THEN 'Synchrony'
    WHEN REGEXP_CONTAINS(UPPER(COALESCE(advertiser_name, '')), r'HIGHLIGHTS') THEN 'Highlights'
    ELSE COALESCE(NULLIF(TRIM(advertiser_name), ''), NULLIF(TRIM(advertiser_short_name), ''), 'Unknown')
  END AS `_advertiser`,
  campaign_name AS `_campaign_name`,
  campaign_friendly AS `_campaign_friendly`,
  product_code AS `_product_code`,
  product_name AS `_product_name`,
  package_type AS `_package_type`,
  package_name AS `_package_name`,
  COALESCE(NULLIF(p_package_friendly, ''), package_name) AS `_package_name_friendly`,
  gsMediaTeam_channel AS `ADIF_channel`,
  placement_id AS `_placement_id`,
  placement_name AS `_placement_name`,
  supplier_code AS `_supplier_code`,
  supplier_name AS `_supplier_name`,
  supplier_logo AS `_supplier_logo`,
  buy_type AS `p_buy_type`,
  buy_category AS `p_buy_category`,
  channel AS `_channel`,
  channel_raw AS `qa_channel_raw`,
  channel_group AS `_channel_group`,
  media_name AS `_media_name`,
  cost_method AS `p_cost_method`,
  planned_amount AS `p_planned_amount_doNotSum`,
  planned_impressions AS `p_planned_impressions_doNotSum`,
  planned_units AS `p_planned_units_doNotSum`,
  unit_type AS `p_unit_type`,
  payable_rate AS `p_rate`,
  planned_daily_spend_pk AS `_planned_spend`,
  planned_daily_impressions_pk AS `_planned_impressions`,
  prisma_planned_clicks AS `p_planned_clicks`,
  max_prisma_report_date AS `p_max_report_date`,
  d_daily_recalculated_cost AS `dcm_daily_recalculated_cost`,
  d_daily_recalculated_imps AS `dcm_daily_recalculated_imps`,
  d_impressions AS `dcm_impressions`,
  d_media_cost AS `dcm_media_cost`,
  d_clicks AS `dcm_clicks`,
  d_video_plays AS `dcm_video_plays`,
  d_video_comps AS `dcm_video_comps`,
  d_min_date AS `dcm_min_date`,
  d_max_date AS `dcm_max_date`,
  d_min_flight_date AS `dcm_min_flight_date`,
  d_max_flight_date AS `dcm_max_flight_date`,
  d_daily_cpm AS `dcm_daily_cpm`,
  d_total_delivered_imps AS `dcm_total_delivered_imps`,
  d_total_del_inflight_imps AS `dcm_total_del_inflight_imps`,
  fpd_orig_impressions AS `fpd_orig_impressions`,
  fpd_orig_spend AS `fpd_orig_spend`,
  fpd_orig_clicks AS `fpd_orig_clicks`,
  fpd_orig_sends AS `fpd_orig_sends`,
  fpd_orig_opens AS `fpd_orig_opens`,
  fpd_orig_benchmark AS `fpd_orig_benchmark`,
  fpd_orig_benchmark_metric AS `fpd_orig_benchmark_metric`,
  fpd_orig_creative AS `fpd_orig_creative`,
  fpd_creative_img AS `fpd_creative_img`,
  COALESCE(fpd_creative_img, man_creative_img) AS `_creative_img`,
  man_creative_img AS `man_creative_img`,
  fpd_orig_source_files AS `fpd_orig_source_files`,
  fpd_orig_source_urls AS `fpd_orig_source_urls`,
  fpd_orig_source_modified_time AS `fpd_orig_source_modified_time`,
  fpd_updated_impressions AS `fpd_updated_impressions`,
  fpd_updated_spend AS `fpd_updated_spend`,
  fpd_updated_suppliers AS `fpd_updated_suppliers`,
  fpd_updated_initiatives AS `fpd_updated_initiatives`,
  fpd_updated_data_timestamp AS `fpd_updated_data_timestamp`,
  fpd_updated_source_sheet_modified_time AS `fpd_updated_source_sheet_modified_time`,
  fpd_impressions AS `fpd_impressions`,
  fpd_spend AS `fpd_spend`,
  fpd_clicks AS `fpd_clicks`,
  fpd_sends AS `fpd_sends`,
  fpd_opens AS `fpd_opens`,
  final_spend AS `_spend`,
  final_impressions AS `_impressions`,
  final_clicks AS `_clicks`,
  final_video_plays AS `_video_plays`,
  COALESCE(social_video_views, final_video_plays) AS `_video_views`,
  final_video_comps AS `_video_comps`,
  tv_data_refresh_date AS `tv_data_refresh_date`,
  tv_media_outlet AS `tv_media_outlet`,
  tv_type AS `tv_type`,
  tv_program_name AS `tv_program_name`,
  tv_market AS `tv_market`,
  tv_quarter AS `tv_quarter`,
  tv_year AS `tv_year`,
  tv_net_impressions AS `tv_net_impressions`,
  tv_net_cost AS `tv_net_cost`,
  tv_total_units AS `tv_total_units`,
  social_platform AS `s_platform`,
  social_campaign_id AS `s_campaign_id`,
  social_ad_group_id AS `s_ad_group_id`,
  social_ad_id AS `s_ad_id`,
  social_account_name AS `s_account_name`,
  social_pacing_planned_spend AS `s_pacing_planned_spend`,
  social_spend AS `s_spend`,
  social_impressions AS `s_impressions`,
  social_clicks AS `s_clicks`,
  social_video_plays AS `s_video_plays`,
  social_video_views AS `s_video_views`,
  social_video_comps AS `s_video_comps`,
  social_creative_name AS `s_creative_name`,
  social_classification_source AS `s_channel_classification_source`,
  social_publication_status AS `s_publication_status`,
  social_source_sheet_url AS `s_source_sheet_url`,
  social_loaded_at AS `s_loaded_at`,
  social_row_key AS `s_wp_row_key`,
  social_record_source AS `s_record_source`,
  social_fallback_fields AS `s_fallback_fields`,
  man_edit_id AS `man_edit_id`,
  man_edit_reason AS `man_edit_reason`,
  man_editor_email AS `man_editor_email`,
  man_source_sheet_url AS `man_source_sheet_url`,
  man_source_sheet_modified_time AS `man_source_sheet_modified_time`,
  man_loaded_at AS `man_loaded_at`,
  man_start_date AS `man_start_date`,
  man_end_date AS `man_end_date`,
  man_daily_spend AS `man_daily_spend`,
  man_daily_impressions AS `man_daily_impressions`,
  man_daily_planned_spend AS `man_daily_planned_spend`,
  man_daily_planned_impressions AS `man_daily_planned_impressions`,
  man_daily_clicks AS `man_daily_clicks`,
  man_daily_video_plays AS `man_daily_video_plays`,
  man_daily_video_comps AS `man_daily_video_comps`,
  man_total_spend_doNotSum AS `man_total_spend_doNotSum`,
  man_total_impressions_doNotSum AS `man_total_impressions_doNotSum`,
  man_total_planned_spend_doNotSum AS `man_total_planned_spend_doNotSum`,
  man_total_planned_impressions_doNotSum AS `man_total_planned_impressions_doNotSum`,
  man_total_clicks_doNotSum AS `man_total_clicks_doNotSum`,
  man_total_video_plays_doNotSum AS `man_total_video_plays_doNotSum`,
  man_total_video_comps_doNotSum AS `man_total_video_comps_doNotSum`,
  pkg_est_spend AS `qa_pkg_est_spend_doNotSum`,
  pkg_est_impressions AS `qa_pkg_est_impressions_doNotSum`,
  pkg_act_spend AS `qa_pkg_act_spend_doNotSum`,
  pkg_act_impressions AS `qa_pkg_act_impressions_doNotSum`,
  pkg_act_clicks AS `qa_pkg_act_clicks_doNotSum`,
  pkg_fpd_orig_impressions AS `qa_pkg_fpd_orig_impressions_doNotSum`,
  pkg_fpd_orig_spend AS `qa_pkg_fpd_orig_spend_doNotSum`,
  pkg_fpd_updated_impressions AS `qa_pkg_fpd_updated_impressions_doNotSum`,
  pkg_fpd_updated_spend AS `qa_pkg_fpd_updated_spend_doNotSum`,
  pkg_fpd_combined_impressions AS `qa_pkg_fpd_combined_impressions_doNotSum`,
  pkg_fpd_combined_spend AS `qa_pkg_fpd_combined_spend_doNotSum`,
  NULLIF(row_data_issue_category, 'ok') AS `qa_row_data_callouts`,
  CASE
    WHEN pkg_est_spend = 0 THEN NULL
    ELSE pkg_act_spend > pkg_est_spend
  END AS `qa_pkg_over_bool`,
  CASE
    WHEN pkg_est_spend = 0 THEN NULL
    WHEN pkg_act_spend > pkg_est_spend THEN 1
    ELSE 0
  END AS `qa_pkg_over_flag`,
  CURRENT_TIMESTAMP() AS `qa_model_view_runtime_timestamp`,
  initiative AS `initiative`
FROM with_initiative;
