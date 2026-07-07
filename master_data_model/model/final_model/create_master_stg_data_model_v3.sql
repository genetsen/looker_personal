-- Versioned master data model candidate for lowest-available-grain actuals.
--
-- Purpose:
--   Build a sibling v3 model that keeps the stable master-model columns while
--   separating package/date planned metrics from lower-grain delivery actuals.
--   This leaves the production `master_stg.data_model` view untouched.
--
-- Safe-use notes:
--   * Planned metrics are carried on one natural row per package/date so
--     package/date rollups work without a separate grain filter.
--   * Package/date planned values are also exposed as
--     `qa_v3_package_planned_*_doNotSum` context fields, following the DCM
--     cost-model pattern for package context repeated on detail rows.
--   * Digital DCM and original FPD actuals expand to placement/ad/creative or
--     placement/creative/factor grain instead of collapsing those fields into
--     pipe-delimited package/date values.
--   * If a package/date has a manual delivery override, lower-grain final
--     actual metrics are suppressed and the manual row is the final actual row.
--   * DCM final impressions use source `impressions`; `daily_recalculated_imps`
--     remains source/QA context only.

DECLARE existing_v3_object_type STRING DEFAULT (
  SELECT table_type
  FROM `looker-studio-pro-452620.master_stg.INFORMATION_SCHEMA.TABLES`
  WHERE table_name = 'data_model_v3'
);

CREATE TEMP TABLE stable_v3_context AS
SELECT *
FROM `looker-studio-pro-452620.master_stg.data_model`;

IF existing_v3_object_type = 'VIEW' THEN
  DROP VIEW `looker-studio-pro-452620.master_stg.data_model_v3`;
END IF;

CREATE OR REPLACE TABLE `looker-studio-pro-452620.master_stg.data_model_v3`
CLUSTER BY `_advertiser`
AS
WITH
stable AS (
  SELECT
    base.*,
    CAST(NULL AS STRING) AS conv_activity,
    CAST(NULL AS INT64) AS conv_total_conversions,
    CAST(NULL AS INT64) AS conv_source_impressions,
    CAST(NULL AS INT64) AS conv_source_clicks,
    CAST(NULL AS INT64) AS conv_source_row_count,
    CAST(NULL AS STRING) AS conv_site,
    CAST(NULL AS STRING) AS conv_site_cm360,
    CAST(NULL AS STRING) AS conv_campaign,
    CAST(NULL AS STRING) AS conv_campaign_id,
    CAST(NULL AS STRING) AS conv_package_roadblock,
    CAST(NULL AS STRING) AS conv_creative,
    CAST(NULL AS TIMESTAMP) AS conv_loaded_at,
    CAST(NULL AS STRING) AS conv_source_sheet_id,
    CAST(NULL AS STRING) AS conv_source_sheet_tab,
    CAST(NULL AS STRING) AS conv_source_sheet_gid
  FROM stable_v3_context AS base
),

stable_context AS (
  SELECT
    `_package_id`,
    `_date`,
    (ARRAY_AGG(
      s
      ORDER BY
        IF(qa_manual_edit_flag, 0, 1),
        IF(`_planned_spend` IS NOT NULL OR `_planned_impressions` IS NOT NULL, 0, 1),
        IF(`_advertiser_name` IS NOT NULL, 0, 1),
        IF(`_package_name` IS NOT NULL, 0, 1)
      LIMIT 1
    ))[OFFSET(0)] AS c
  FROM stable AS s
  GROUP BY `_package_id`, `_date`
),

stable_actual_flags AS (
  SELECT
    `_package_id`,
    `_date`,
    COUNTIF(`_spend` IS NOT NULL) > 0 AS has_stable_spend,
    COUNTIF(`_impressions` IS NOT NULL) > 0 AS has_stable_impressions,
    COUNTIF(`_clicks` IS NOT NULL) > 0 AS has_stable_clicks,
    COUNTIF(`_video_plays` IS NOT NULL) > 0 AS has_stable_video_plays,
    COUNTIF(`_video_views` IS NOT NULL) > 0 AS has_stable_video_views,
    COUNTIF(`_video_comps` IS NOT NULL) > 0 AS has_stable_video_comps
  FROM stable
  GROUP BY `_package_id`, `_date`
),

manual_delivery_daily AS (
  SELECT
    package_id AS `_package_id`,
    DATE(date) AS `_date`,
    SUM(man_daily_spend) AS man_daily_spend,
    SUM(man_daily_impressions) AS man_daily_impressions,
    SUM(man_daily_clicks) AS man_daily_clicks,
    SUM(man_daily_video_plays) AS man_daily_video_plays,
    SUM(man_daily_video_comps) AS man_daily_video_comps
  FROM `looker-studio-pro-452620.landing.master_data_model_manual_package_daily`
  WHERE DATE(date) >= DATE '2025-01-01'
    AND (
      man_daily_spend IS NOT NULL
      OR man_daily_impressions IS NOT NULL
      OR man_daily_clicks IS NOT NULL
      OR man_daily_video_plays IS NOT NULL
      OR man_daily_video_comps IS NOT NULL
    )
  GROUP BY package_id, DATE(date)
),

package_plan_daily AS (
  SELECT
    `_package_id`,
    `_date`,
    SUM(`_planned_spend`) AS planned_spend,
    SUM(`_planned_impressions`) AS planned_impressions
  FROM stable
  WHERE `_planned_spend` IS NOT NULL
    OR `_planned_impressions` IS NOT NULL
  GROUP BY `_package_id`, `_date`
),

manual_delivery_rows AS (
  SELECT
    'manual_delivery_override' AS qa_v3_row_type,
    'package_date' AS qa_v3_metric_grain,
    'manual_package_daily' AS qa_v3_source_detail_type,
    CAST(NULL AS STRING) AS qa_v3_ad_name,
    'manual delivery override wins; lower-grain final actuals suppressed' AS qa_v3_metric_behavior,
    p.planned_spend AS qa_v3_package_planned_spend_doNotSum,
    p.planned_impressions AS qa_v3_package_planned_impressions_doNotSum,
    ctx.c.* REPLACE(
      'manual_package_edits' AS qa_row_data_source_primary,
      'looker-studio-pro-452620.landing.master_data_model_manual_package_daily' AS qa_data_source,
      CONCAT(COALESCE(ctx.c.qa_row_data_sources_available, 'none'), ' | manual_package_edits') AS qa_row_data_sources_available,
      'manual_delivery_override' AS qa_data_issues,
      CAST(NULL AS STRING) AS `_placement_id`,
      CAST(NULL AS STRING) AS `_placement_name`,
      CAST(NULL AS STRING) AS fpd_factor,
      CAST(NULL AS STRING) AS fpd_creative,
      CAST(NULL AS STRING) AS `_creative_name`,
      p.planned_spend AS `_planned_spend`,
      p.planned_impressions AS `_planned_impressions`,
      m.man_daily_spend AS `_spend`,
      m.man_daily_impressions AS `_impressions`,
      m.man_daily_clicks AS `_clicks`,
      m.man_daily_video_plays AS `_video_plays`,
      m.man_daily_video_plays AS `_video_views`,
      m.man_daily_video_comps AS `_video_comps`,
      TRUE AS qa_manual_edit_flag,
      m.man_daily_spend AS man_daily_spend,
      m.man_daily_impressions AS man_daily_impressions,
      m.man_daily_clicks AS man_daily_clicks,
      m.man_daily_video_plays AS man_daily_video_plays,
      m.man_daily_video_comps AS man_daily_video_comps
    )
  FROM manual_delivery_daily AS m
  JOIN stable_context AS ctx
    USING (`_package_id`, `_date`)
  LEFT JOIN package_plan_daily AS p
    USING (`_package_id`, `_date`)
),

fpd_package_daily AS (
  SELECT
    package_id AS `_package_id`,
    DATE(date_final) AS `_date`,
    SUM(spend) AS fpd_orig_spend,
    SUM(impressions) AS fpd_orig_impressions,
    SUM(clicks) AS fpd_orig_clicks
  FROM `looker-studio-pro-452620.landing.fpd_data_ranged_shortcutsFolder`
  WHERE DATE(date_final) >= DATE '2025-01-01'
    AND package_id IS NOT NULL
  GROUP BY package_id, DATE(date_final)
),

fpd_updated_package_daily AS (
  SELECT
    package_id AS `_package_id`,
    DATE(date) AS `_date`,
    SUM(daily_fpd_spend) AS fpd_updated_spend,
    SUM(daily_fpd_impressions) AS fpd_updated_impressions
  FROM `looker-studio-pro-452620.landing.adif_updated_fpd_daily`
  WHERE DATE(date) >= DATE '2025-01-01'
    AND package_id IS NOT NULL
  GROUP BY package_id, DATE(date)
),

fpd_winners AS (
  SELECT
    COALESCE(o.`_package_id`, u.`_package_id`) AS `_package_id`,
    COALESCE(o.`_date`, u.`_date`) AS `_date`,
    COALESCE(o.fpd_orig_spend, 0) + COALESCE(u.fpd_updated_spend, 0) AS fpd_spend,
    COALESCE(o.fpd_orig_impressions, 0) + COALESCE(u.fpd_updated_impressions, 0) AS fpd_impressions,
    o.fpd_orig_clicks
  FROM fpd_package_daily AS o
  FULL OUTER JOIN fpd_updated_package_daily AS u
    USING (`_package_id`, `_date`)
),

dcm_detail AS (
  SELECT
    'dcm' AS source_detail_type,
    d.package_id AS `_package_id`,
    DATE(d.date) AS `_date`,
    CAST(d.placement_id AS STRING) AS `_placement_id`,
    ARRAY_AGG(CAST(d.placement AS STRING) IGNORE NULLS ORDER BY CAST(d.placement AS STRING) LIMIT 1)[SAFE_OFFSET(0)] AS `_placement_name`,
    CAST(d.ad AS STRING) AS qa_v3_ad_name,
    CAST(d.creative AS STRING) AS creative_name,
    CAST(NULL AS STRING) AS fpd_factor,
    CAST(NULL AS STRING) AS fpd_creative,
    CAST(NULL AS STRING) AS fpd_creative_img,
    SUM(d.daily_recalculated_cost) AS dcm_daily_recalculated_cost,
    SUM(d.daily_recalculated_imps) AS dcm_daily_recalculated_imps,
    SUM(d.impressions) AS dcm_impressions,
    SUM(d.media_cost) AS dcm_media_cost,
    SUM(d.clicks) AS dcm_clicks,
    SUM(d.rich_media_video_plays) AS dcm_video_plays,
    SUM(d.rich_media_video_completions) AS dcm_video_comps,
    CAST(NULL AS FLOAT64) AS fpd_impressions,
    CAST(NULL AS FLOAT64) AS fpd_spend,
    CAST(NULL AS FLOAT64) AS fpd_clicks
  FROM `looker-studio-pro-452620.DCM.20250505_costModel_v5` AS d
  WHERE DATE(d.date) >= DATE '2025-01-01'
    AND d.package_id IS NOT NULL
  GROUP BY
    d.package_id,
    DATE(d.date),
    CAST(d.placement_id AS STRING),
    CAST(d.ad AS STRING),
    CAST(d.creative AS STRING)
),

fpd_original_detail AS (
  SELECT
    'fpd_original' AS source_detail_type,
    f.package_id AS `_package_id`,
    DATE(f.date_final) AS `_date`,
    COALESCE(CAST(f.partner_placement_name AS STRING), CAST(f.partner_packagePlacement_name AS STRING), f.package_id) AS `_placement_id`,
    COALESCE(CAST(f.partner_placement_name AS STRING), CAST(f.partner_packagePlacement_name AS STRING), CAST(f.package_name AS STRING)) AS `_placement_name`,
    CAST(NULL AS STRING) AS qa_v3_ad_name,
    CAST(f.partner_creative_name AS STRING) AS creative_name,
    CAST(f.factor AS STRING) AS fpd_factor,
    CAST(f.partner_creative_name AS STRING) AS fpd_creative,
    CAST(f.creative_git_link AS STRING) AS fpd_creative_img,
    CAST(NULL AS FLOAT64) AS dcm_daily_recalculated_cost,
    CAST(NULL AS INT64) AS dcm_daily_recalculated_imps,
    CAST(NULL AS INT64) AS dcm_impressions,
    CAST(NULL AS FLOAT64) AS dcm_media_cost,
    CAST(NULL AS INT64) AS dcm_clicks,
    CAST(NULL AS INT64) AS dcm_video_plays,
    CAST(NULL AS INT64) AS dcm_video_comps,
    SUM(f.impressions) AS fpd_impressions,
    SUM(f.spend) AS fpd_spend,
    SUM(f.clicks) AS fpd_clicks
  FROM `looker-studio-pro-452620.landing.fpd_data_ranged_shortcutsFolder` AS f
  WHERE DATE(f.date_final) >= DATE '2025-01-01'
    AND f.package_id IS NOT NULL
  GROUP BY
    f.package_id,
    DATE(f.date_final),
    COALESCE(CAST(f.partner_placement_name AS STRING), CAST(f.partner_packagePlacement_name AS STRING), f.package_id),
    COALESCE(CAST(f.partner_placement_name AS STRING), CAST(f.partner_packagePlacement_name AS STRING), CAST(f.package_name AS STRING)),
    CAST(f.factor AS STRING),
    CAST(f.partner_creative_name AS STRING),
    CAST(f.creative_git_link AS STRING)
),

fpd_updated_detail AS (
  SELECT
    'fpd_updated_package' AS source_detail_type,
    u.package_id AS `_package_id`,
    DATE(u.date) AS `_date`,
    u.package_id AS `_placement_id`,
    ARRAY_AGG(CAST(u.package_name AS STRING) IGNORE NULLS ORDER BY CAST(u.package_name AS STRING) LIMIT 1)[SAFE_OFFSET(0)] AS `_placement_name`,
    CAST(NULL AS STRING) AS qa_v3_ad_name,
    CAST(NULL AS STRING) AS creative_name,
    CAST(NULL AS STRING) AS fpd_factor,
    CAST(NULL AS STRING) AS fpd_creative,
    CAST(NULL AS STRING) AS fpd_creative_img,
    CAST(NULL AS FLOAT64) AS dcm_daily_recalculated_cost,
    CAST(NULL AS INT64) AS dcm_daily_recalculated_imps,
    CAST(NULL AS INT64) AS dcm_impressions,
    CAST(NULL AS FLOAT64) AS dcm_media_cost,
    CAST(NULL AS INT64) AS dcm_clicks,
    CAST(NULL AS INT64) AS dcm_video_plays,
    CAST(NULL AS INT64) AS dcm_video_comps,
    SUM(u.daily_fpd_impressions) AS fpd_impressions,
    SUM(u.daily_fpd_spend) AS fpd_spend,
    CAST(NULL AS FLOAT64) AS fpd_clicks
  FROM `looker-studio-pro-452620.landing.adif_updated_fpd_daily` AS u
  WHERE DATE(u.date) >= DATE '2025-01-01'
    AND u.package_id IS NOT NULL
  GROUP BY u.package_id, DATE(u.date)
),

conversion_detail AS (
  SELECT
    REGEXP_EXTRACT(package_roadblock, r'\|([^|_]+)_') AS `_package_id`,
    DATE(date) AS `_date`,
    CAST(new_site AS STRING) AS conv_site,
    CAST(site_cm360 AS STRING) AS conv_site_cm360,
    CAST(campaign AS STRING) AS conv_campaign,
    CAST(campaign_id AS STRING) AS conv_campaign_id,
    CAST(package_roadblock AS STRING) AS conv_package_roadblock,
    CAST(creative AS STRING) AS conv_creative,
    CAST(activity AS STRING) AS conv_activity,
    SUM(CAST(total_conversions AS INT64)) AS conv_total_conversions,
    SUM(CAST(impressions AS INT64)) AS conv_source_impressions,
    SUM(CAST(clicks AS INT64)) AS conv_source_clicks,
    COUNT(*) AS conv_source_row_count,
    MAX(loaded_at) AS conv_loaded_at,
    ARRAY_AGG(CAST(source_sheet_id AS STRING) IGNORE NULLS ORDER BY loaded_at DESC LIMIT 1)[SAFE_OFFSET(0)] AS conv_source_sheet_id,
    ARRAY_AGG(CAST(source_sheet_tab AS STRING) IGNORE NULLS ORDER BY loaded_at DESC LIMIT 1)[SAFE_OFFSET(0)] AS conv_source_sheet_tab,
    ARRAY_AGG(CAST(source_sheet_gid AS STRING) IGNORE NULLS ORDER BY loaded_at DESC LIMIT 1)[SAFE_OFFSET(0)] AS conv_source_sheet_gid
  FROM `looker-studio-pro-452620.landing.rtl_conv_report`
  WHERE DATE(date) >= DATE '2025-01-01'
    AND REGEXP_EXTRACT(package_roadblock, r'\|([^|_]+)_') IS NOT NULL
  GROUP BY
    `_package_id`,
    `_date`,
    conv_site,
    conv_site_cm360,
    conv_campaign,
    conv_campaign_id,
    conv_package_roadblock,
    conv_creative,
    conv_activity
),

conversion_context_candidates AS (
  SELECT
    d.*,
    ANY_VALUE(exact.c) AS exact_c,
    ARRAY_AGG(
      s
      ORDER BY
        ABS(DATE_DIFF(s.`_date`, d.`_date`, DAY)),
        IF(s.`_date` <= d.`_date`, 0, 1),
        s.`_date` DESC
      LIMIT 1
    )[SAFE_OFFSET(0)] AS nearest_c
  FROM conversion_detail AS d
  LEFT JOIN stable_context AS exact
    USING (`_package_id`, `_date`)
  LEFT JOIN stable AS s
    ON s.`_package_id` = d.`_package_id`
  GROUP BY
    d.`_package_id`,
    d.`_date`,
    d.conv_site,
    d.conv_site_cm360,
    d.conv_campaign,
    d.conv_campaign_id,
    d.conv_package_roadblock,
    d.conv_creative,
    d.conv_activity,
    d.conv_total_conversions,
    d.conv_source_impressions,
    d.conv_source_clicks,
    d.conv_source_row_count,
    d.conv_loaded_at,
    d.conv_source_sheet_id,
    d.conv_source_sheet_tab,
    d.conv_source_sheet_gid
),

conversion_context AS (
  SELECT
    * EXCEPT(exact_c, nearest_c),
    exact_c IS NULL AS conv_context_from_nearest_package_date,
    IF(exact_c IS NULL, nearest_c, exact_c) AS c
  FROM conversion_context_candidates
),

digital_detail AS (
  SELECT * FROM dcm_detail
  UNION ALL
  SELECT * FROM fpd_original_detail
  UNION ALL
  SELECT * FROM fpd_updated_detail
),

digital_detail_ranked AS (
  SELECT
    d.*,
    ROW_NUMBER() OVER (
      PARTITION BY d.`_package_id`, d.`_date`
      ORDER BY
        CASE d.source_detail_type
          WHEN 'fpd_original' THEN 0
          WHEN 'fpd_updated_package' THEN 1
          WHEN 'dcm' THEN 2
          ELSE 3
        END,
        d.`_placement_id`,
        d.qa_v3_ad_name,
        d.creative_name,
        d.fpd_factor
    ) AS planned_carrier_rank
  FROM digital_detail AS d
),

digital_actual_rows AS (
  SELECT
    'source_actual' AS qa_v3_row_type,
    CASE
      WHEN d.source_detail_type = 'dcm' THEN 'package_date_placement_ad_creative'
      WHEN d.source_detail_type = 'fpd_original' THEN 'package_date_placement_factor_creative'
      ELSE 'package_date'
    END AS qa_v3_metric_grain,
    d.source_detail_type AS qa_v3_source_detail_type,
    d.qa_v3_ad_name,
    CASE
      WHEN m.`_package_id` IS NOT NULL THEN 'manual override exists; final actuals intentionally null'
      WHEN d.source_detail_type = 'dcm' AND (COALESCE(f.fpd_spend, 0) != 0 OR COALESCE(f.fpd_impressions, 0) != 0)
        THEN 'FPD wins spend/impressions; DCM keeps clicks/video only where FPD does not provide clicks'
      WHEN d.source_detail_type = 'dcm' THEN 'DCM actuals at source detail grain; impressions use source impressions'
      ELSE 'FPD actuals at source detail grain'
    END AS qa_v3_metric_behavior,
    p.planned_spend AS qa_v3_package_planned_spend_doNotSum,
    p.planned_impressions AS qa_v3_package_planned_impressions_doNotSum,
    ctx.c.* REPLACE(
      d.source_detail_type AS qa_row_data_source_primary,
      CASE
        WHEN d.source_detail_type = 'dcm' THEN 'giant-spoon-299605.data_model_2025.new_md'
        WHEN d.source_detail_type = 'fpd_original' THEN 'looker-studio-pro-452620.landing.fpd_data_ranged_shortcutsFolder'
        ELSE 'looker-studio-pro-452620.landing.adif_updated_fpd_daily'
      END AS qa_data_source,
      CONCAT(COALESCE(ctx.c.qa_row_data_sources_available, 'none'), ' | ', d.source_detail_type) AS qa_row_data_sources_available,
      IF(m.`_package_id` IS NOT NULL, 'manual_delivery_override_source_suppressed', COALESCE(ctx.c.qa_data_issues, 'no_issues')) AS qa_data_issues,
      d.`_placement_id` AS `_placement_id`,
      d.`_placement_name` AS `_placement_name`,
      d.fpd_factor AS fpd_factor,
      d.fpd_creative AS fpd_creative,
      d.fpd_creative_img AS fpd_creative_img,
      d.creative_name AS `_creative_name`,
      d.fpd_creative_img AS `_creative_img`,
      IF(
        m.`_package_id` IS NULL
        AND d.planned_carrier_rank = 1,
        p.planned_spend,
        NULL
      ) AS `_planned_spend`,
      IF(
        m.`_package_id` IS NULL
        AND d.planned_carrier_rank = 1,
        p.planned_impressions,
        NULL
      ) AS `_planned_impressions`,
      d.dcm_daily_recalculated_cost AS dcm_daily_recalculated_cost,
      d.dcm_daily_recalculated_imps AS dcm_daily_recalculated_imps,
      d.dcm_impressions AS dcm_impressions,
      d.dcm_media_cost AS dcm_media_cost,
      d.dcm_clicks AS dcm_clicks,
      d.dcm_video_plays AS dcm_video_plays,
      d.dcm_video_comps AS dcm_video_comps,
      d.fpd_impressions AS fpd_impressions,
      d.fpd_spend AS fpd_spend,
      d.fpd_clicks AS fpd_clicks,
      CASE
        WHEN m.`_package_id` IS NOT NULL THEN NULL
        WHEN NOT COALESCE(a.has_stable_spend, FALSE) THEN NULL
        WHEN d.source_detail_type = 'dcm' AND COALESCE(f.fpd_spend, 0) != 0 THEN NULL
        WHEN d.source_detail_type = 'dcm' THEN d.dcm_daily_recalculated_cost
        ELSE d.fpd_spend
      END AS `_spend`,
      CASE
        WHEN m.`_package_id` IS NOT NULL THEN NULL
        WHEN NOT COALESCE(a.has_stable_impressions, FALSE) THEN NULL
        WHEN d.source_detail_type = 'dcm' AND COALESCE(f.fpd_impressions, 0) != 0 THEN NULL
        WHEN d.source_detail_type = 'dcm' THEN CAST(d.dcm_impressions AS FLOAT64)
        ELSE d.fpd_impressions
      END AS `_impressions`,
      CASE
        WHEN m.`_package_id` IS NOT NULL THEN NULL
        WHEN NOT COALESCE(a.has_stable_clicks, FALSE) THEN NULL
        WHEN d.source_detail_type = 'dcm' AND f.fpd_orig_clicks IS NOT NULL THEN NULL
        WHEN d.source_detail_type = 'dcm' THEN CAST(d.dcm_clicks AS FLOAT64)
        ELSE d.fpd_clicks
      END AS `_clicks`,
      IF(m.`_package_id` IS NOT NULL OR NOT COALESCE(a.has_stable_video_plays, FALSE), NULL, CAST(d.dcm_video_plays AS FLOAT64)) AS `_video_plays`,
      IF(m.`_package_id` IS NOT NULL OR NOT COALESCE(a.has_stable_video_views, FALSE), NULL, CAST(d.dcm_video_plays AS FLOAT64)) AS `_video_views`,
      IF(m.`_package_id` IS NOT NULL OR NOT COALESCE(a.has_stable_video_comps, FALSE), NULL, CAST(d.dcm_video_comps AS FLOAT64)) AS `_video_comps`,
      IF(m.`_package_id` IS NOT NULL, TRUE, COALESCE(ctx.c.qa_manual_edit_flag, FALSE)) AS qa_manual_edit_flag
    )
  FROM digital_detail_ranked AS d
  LEFT JOIN stable_context AS ctx
    USING (`_package_id`, `_date`)
  LEFT JOIN manual_delivery_daily AS m
    USING (`_package_id`, `_date`)
  LEFT JOIN fpd_winners AS f
    USING (`_package_id`, `_date`)
  LEFT JOIN stable_actual_flags AS a
    USING (`_package_id`, `_date`)
  LEFT JOIN package_plan_daily AS p
    USING (`_package_id`, `_date`)
),

conversion_actual_rows AS (
  SELECT
    'conversion_outcome' AS qa_v3_row_type,
    'package_date_site_creative_activity' AS qa_v3_metric_grain,
    'conversion_activity' AS qa_v3_source_detail_type,
    CAST(NULL AS STRING) AS qa_v3_ad_name,
    IF(
      d.conv_context_from_nearest_package_date,
      'conversion outcome row uses nearest package context because no delivery row exists on the conversion date; delivery metrics intentionally null',
      'conversion outcome row; delivery metrics intentionally null'
    ) AS qa_v3_metric_behavior,
    p.planned_spend AS qa_v3_package_planned_spend_doNotSum,
    p.planned_impressions AS qa_v3_package_planned_impressions_doNotSum,
    d.c.* REPLACE(
      'conversion_activity' AS qa_row_data_source_primary,
      'looker-studio-pro-452620.landing.rtl_conv_report' AS qa_data_source,
      d.conv_loaded_at AS qa_data_source_refresh_at,
      CONCAT(COALESCE(d.c.qa_row_data_sources_available, 'none'), ' | conversion_activity') AS qa_row_data_sources_available,
      COALESCE(
        NULLIF(ARRAY_TO_STRING(ARRAY_CONCAT(
          IF(COALESCE(d.c.qa_data_issues, 'no_issues') = 'no_issues', [], [d.c.qa_data_issues]),
          IF(d.conv_context_from_nearest_package_date, ['conversion_date_without_delivery_row'], [])
        ), ' | '), ''),
        'no_issues'
      ) AS qa_data_issues,
      d.`_date` AS `_date`,
      COALESCE(NULLIF(d.conv_site_cm360, ''), NULLIF(d.conv_site, ''), d.`_package_id`) AS `_placement_id`,
      COALESCE(NULLIF(d.conv_site, ''), NULLIF(d.conv_site_cm360, ''), d.conv_package_roadblock) AS `_placement_name`,
      CAST(NULL AS STRING) AS fpd_factor,
      CAST(NULL AS STRING) AS fpd_creative,
      CAST(NULL AS STRING) AS fpd_creative_img,
      d.conv_creative AS `_creative_name`,
      CAST(NULL AS STRING) AS `_creative_img`,
      CAST(NULL AS FLOAT64) AS `_planned_spend`,
      CAST(NULL AS FLOAT64) AS `_planned_impressions`,
      CAST(NULL AS FLOAT64) AS `_spend`,
      CAST(NULL AS FLOAT64) AS `_impressions`,
      CAST(NULL AS FLOAT64) AS `_clicks`,
      CAST(NULL AS FLOAT64) AS `_video_plays`,
      CAST(NULL AS FLOAT64) AS `_video_views`,
      CAST(NULL AS FLOAT64) AS `_video_comps`,
      FALSE AS qa_manual_edit_flag,
      d.conv_activity AS conv_activity,
      d.conv_total_conversions AS conv_total_conversions,
      d.conv_source_impressions AS conv_source_impressions,
      d.conv_source_clicks AS conv_source_clicks,
      d.conv_source_row_count AS conv_source_row_count,
      d.conv_site AS conv_site,
      d.conv_site_cm360 AS conv_site_cm360,
      d.conv_campaign AS conv_campaign,
      d.conv_campaign_id AS conv_campaign_id,
      d.conv_package_roadblock AS conv_package_roadblock,
      d.conv_creative AS conv_creative,
      d.conv_loaded_at AS conv_loaded_at,
      d.conv_source_sheet_id AS conv_source_sheet_id,
      d.conv_source_sheet_tab AS conv_source_sheet_tab,
      d.conv_source_sheet_gid AS conv_source_sheet_gid
    )
  FROM conversion_context AS d
  LEFT JOIN package_plan_daily AS p
    USING (`_package_id`, `_date`)
  WHERE d.c IS NOT NULL
),

non_digital_source_ranked AS (
  SELECT
    s.*,
    ROW_NUMBER() OVER (
      PARTITION BY s.`_package_id`, s.`_date`
      ORDER BY
        CASE s.qa_row_data_source_primary
          WHEN 'tv_combined' THEN 0
          WHEN 'amazon_ads' THEN 1
          WHEN 'wp_search_data_template' THEN 2
          WHEN 'social' THEN 3
          ELSE 4
        END,
        s.`_placement_id`,
        s.`_creative_name`
    ) AS planned_carrier_rank
  FROM stable AS s
  WHERE s.qa_media_data_type NOT IN ('digital', 'manual')
    AND s.qa_row_data_source_primary NOT IN ('planned_only', 'manual_package_edits')
),

non_digital_source_rows AS (
  SELECT
    'source_actual' AS qa_v3_row_type,
    CASE
      WHEN s.qa_media_data_type = 'social' THEN 'source_social_daily'
      WHEN s.qa_media_data_type = 'amazon_ads' THEN 'source_amazon_daily'
      WHEN s.qa_media_data_type = 'tv' THEN 'source_tv_daily'
      ELSE 'stable_source_daily'
    END AS qa_v3_metric_grain,
    s.qa_row_data_source_primary AS qa_v3_source_detail_type,
    CAST(NULL AS STRING) AS qa_v3_ad_name,
    IF(
      m.`_package_id` IS NOT NULL,
      'manual override exists; final actuals intentionally null',
      'source actual row; planned metrics intentionally null'
    ) AS qa_v3_metric_behavior,
    p.planned_spend AS qa_v3_package_planned_spend_doNotSum,
    p.planned_impressions AS qa_v3_package_planned_impressions_doNotSum,
    s.* EXCEPT(planned_carrier_rank) REPLACE(
      IF(m.`_package_id` IS NULL AND s.planned_carrier_rank = 1, p.planned_spend, NULL) AS `_planned_spend`,
      IF(m.`_package_id` IS NULL AND s.planned_carrier_rank = 1, p.planned_impressions, NULL) AS `_planned_impressions`,
      IF(m.`_package_id` IS NOT NULL, NULL, s.`_spend`) AS `_spend`,
      IF(m.`_package_id` IS NOT NULL, NULL, s.`_impressions`) AS `_impressions`,
      IF(m.`_package_id` IS NOT NULL, NULL, s.`_clicks`) AS `_clicks`,
      IF(m.`_package_id` IS NOT NULL, NULL, s.`_video_plays`) AS `_video_plays`,
      IF(m.`_package_id` IS NOT NULL, NULL, s.`_video_views`) AS `_video_views`,
      IF(m.`_package_id` IS NOT NULL, NULL, s.`_video_comps`) AS `_video_comps`,
      IF(m.`_package_id` IS NOT NULL, TRUE, COALESCE(s.qa_manual_edit_flag, FALSE)) AS qa_manual_edit_flag
    )
  FROM non_digital_source_ranked AS s
  LEFT JOIN manual_delivery_daily AS m
    USING (`_package_id`, `_date`)
  LEFT JOIN package_plan_daily AS p
    USING (`_package_id`, `_date`)
),

planned_only_rows AS (
  SELECT
    'planned_only_package_date' AS qa_v3_row_type,
    'package_date' AS qa_v3_metric_grain,
    'planned_only' AS qa_v3_source_detail_type,
    CAST(NULL AS STRING) AS qa_v3_ad_name,
    'planned-only package/date row; no actual source row exists' AS qa_v3_metric_behavior,
    p.planned_spend AS qa_v3_package_planned_spend_doNotSum,
    p.planned_impressions AS qa_v3_package_planned_impressions_doNotSum,
    ctx.c.* REPLACE(
      'planned_only' AS qa_row_data_source_primary,
      'planned_only' AS qa_data_source,
      'planned_only' AS qa_row_data_sources_available,
      COALESCE(ctx.c.qa_data_issues, 'missing_actuals') AS qa_data_issues,
      CAST(NULL AS STRING) AS `_placement_id`,
      CAST(NULL AS STRING) AS `_placement_name`,
      CAST(NULL AS STRING) AS fpd_factor,
      CAST(NULL AS STRING) AS fpd_creative,
      CAST(NULL AS STRING) AS `_creative_name`,
      p.planned_spend AS `_planned_spend`,
      p.planned_impressions AS `_planned_impressions`,
      CAST(NULL AS FLOAT64) AS `_spend`,
      CAST(NULL AS FLOAT64) AS `_impressions`,
      CAST(NULL AS FLOAT64) AS `_clicks`,
      CAST(NULL AS FLOAT64) AS `_video_plays`,
      CAST(NULL AS FLOAT64) AS `_video_views`,
      CAST(NULL AS FLOAT64) AS `_video_comps`
    )
  FROM package_plan_daily AS p
  JOIN stable_context AS ctx
    USING (`_package_id`, `_date`)
  WHERE NOT EXISTS (
    SELECT 1
    FROM manual_delivery_daily AS m
    WHERE m.`_package_id` = p.`_package_id`
      AND m.`_date` = p.`_date`
  )
    AND NOT EXISTS (
      SELECT 1
      FROM digital_detail AS d
      WHERE d.`_package_id` = p.`_package_id`
        AND d.`_date` = p.`_date`
    )
    AND NOT EXISTS (
      SELECT 1
      FROM non_digital_source_ranked AS s
      WHERE s.`_package_id` = p.`_package_id`
        AND s.`_date` = p.`_date`
    )
),

final_rows AS (
  SELECT * FROM manual_delivery_rows
  UNION ALL
  SELECT * FROM digital_actual_rows
  UNION ALL
  SELECT * FROM conversion_actual_rows
  UNION ALL
  SELECT * FROM non_digital_source_rows
  UNION ALL
  SELECT * FROM planned_only_rows
)

SELECT
  -- V3 VIEW NOTE: Natural source rows are kept at their source grain. Package
  -- planned values repeat as doNotSum context like DCM, while summable
  -- `_planned_*` values are carried once per package/date.
  *
FROM final_rows
WHERE `_advertiser_name` != 'Highlights'
