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

apo_dcm_creative_image_map AS (
  -- Step 1 asset map: match Apollo DCM creative after final size suffix removal.
  SELECT
    advertiser,
    normalized_dcm_creative_name,
    published_image_url
  FROM `looker-studio-pro-452620.landing.apo_dcm_creative_image_asset_map`
  WHERE publication_status = 'published'
    AND published_image_url IS NOT NULL
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY advertiser, normalized_dcm_creative_name
    ORDER BY loaded_at DESC, published_image_url
  ) = 1
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
    ANY_VALUE(m.published_image_url) AS dcm_creative_img,
    SUM(d.daily_recalculated_cost) AS dcm_daily_recalculated_cost,
    SUM(d.daily_recalculated_imps) AS dcm_daily_recalculated_imps,
    SUM(d.impressions) AS dcm_impressions,
    SUM(d.media_cost) AS dcm_media_cost,
    SUM(d.clicks) AS dcm_clicks,
    SUM(d.rich_media_video_plays) AS dcm_video_plays,
    SUM(d.rich_media_video_completions) AS dcm_video_comps,
    CAST(NULL AS FLOAT64) AS fpd_impressions,
    CAST(NULL AS FLOAT64) AS fpd_spend,
    CAST(NULL AS FLOAT64) AS fpd_clicks,
    CAST(NULL AS FLOAT64) AS fpd_video_views,
    CAST(NULL AS FLOAT64) AS fpd_video_comps
  FROM `looker-studio-pro-452620.DCM.20250505_costModel_v5` AS d
  LEFT JOIN apo_dcm_creative_image_map AS m
    ON LOWER(TRIM(CAST(d.advertiser AS STRING))) LIKE '%apollo%'
   AND m.advertiser = 'Apollo'
   AND LOWER(REGEXP_REPLACE(
     CAST(d.creative AS STRING),
     r'_(?:\d+\s*x\s*\d+|\d+x\d+|0x0|NA)$',
     ''
   )) = m.normalized_dcm_creative_name
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
    CAST(NULL AS STRING) AS dcm_creative_img,
    CAST(NULL AS FLOAT64) AS dcm_daily_recalculated_cost,
    CAST(NULL AS INT64) AS dcm_daily_recalculated_imps,
    CAST(NULL AS INT64) AS dcm_impressions,
    CAST(NULL AS FLOAT64) AS dcm_media_cost,
    CAST(NULL AS INT64) AS dcm_clicks,
    CAST(NULL AS INT64) AS dcm_video_plays,
    CAST(NULL AS INT64) AS dcm_video_comps,
    SUM(f.impressions) AS fpd_impressions,
    SUM(f.spend) AS fpd_spend,
    SUM(f.clicks) AS fpd_clicks,
    SUM(f.views) AS fpd_video_views,
    SUM(f.completed_views) AS fpd_video_comps
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
    CAST(NULL AS STRING) AS dcm_creative_img,
    CAST(NULL AS FLOAT64) AS dcm_daily_recalculated_cost,
    CAST(NULL AS INT64) AS dcm_daily_recalculated_imps,
    CAST(NULL AS INT64) AS dcm_impressions,
    CAST(NULL AS FLOAT64) AS dcm_media_cost,
    CAST(NULL AS INT64) AS dcm_clicks,
    CAST(NULL AS INT64) AS dcm_video_plays,
    CAST(NULL AS INT64) AS dcm_video_comps,
    SUM(u.daily_fpd_impressions) AS fpd_impressions,
    SUM(u.daily_fpd_spend) AS fpd_spend,
    CAST(NULL AS FLOAT64) AS fpd_clicks,
    CAST(NULL AS FLOAT64) AS fpd_video_views,
    CAST(NULL AS FLOAT64) AS fpd_video_comps
  FROM `looker-studio-pro-452620.landing.adif_updated_fpd_daily` AS u
  WHERE DATE(u.date) >= DATE '2025-01-01'
    AND u.package_id IS NOT NULL
  GROUP BY u.package_id, DATE(u.date)
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
      COALESCE(d.dcm_creative_img, d.fpd_creative_img) AS `_creative_img`,
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
      CASE
        WHEN m.`_package_id` IS NOT NULL THEN NULL
        WHEN d.source_detail_type = 'fpd_original' THEN d.fpd_video_views
        WHEN NOT COALESCE(a.has_stable_video_views, FALSE) THEN NULL
        WHEN d.source_detail_type = 'dcm' THEN CAST(d.dcm_video_plays AS FLOAT64)
        ELSE NULL
      END AS `_video_views`,
      CASE
        WHEN m.`_package_id` IS NOT NULL THEN NULL
        WHEN d.source_detail_type = 'fpd_original' THEN d.fpd_video_comps
        WHEN NOT COALESCE(a.has_stable_video_comps, FALSE) THEN NULL
        WHEN d.source_detail_type = 'dcm' THEN CAST(d.dcm_video_comps AS FLOAT64)
        ELSE NULL
      END AS `_video_comps`,
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
      -- Offline package/date rows have no delivery-source row. For TV, Print,
      -- OOH, and dOOH, publish planned delivery only as the last fallback.
      -- Keep the plan visible and do not change any row with actual delivery.
      CASE
        WHEN p.planned_spend IS NOT NULL
          AND NULLIF(p.planned_impressions, 0) IS NOT NULL
          AND (
            ctx.c.qa_media_data_type = 'tv'
            OR ctx.c._channel_group IN ('linear', 'print', 'ooh', 'ooh_d')
            OR ctx.c._channel IN ('linear_tv', 'print', 'ooh', 'ooh_d')
            OR LOWER(COALESCE(ctx.c._media_name, '')) IN (
              'tv', 'print', 'magazine', 'newspaper', 'ooh'
            )
          )
          THEN p.planned_spend
        ELSE CAST(NULL AS FLOAT64)
      END AS `_spend`,
      CASE
        WHEN NULLIF(p.planned_impressions, 0) IS NOT NULL
          AND (
            ctx.c.qa_media_data_type = 'tv'
            OR ctx.c._channel_group IN ('linear', 'print', 'ooh', 'ooh_d')
            OR ctx.c._channel IN ('linear_tv', 'print', 'ooh', 'ooh_d')
            OR LOWER(COALESCE(ctx.c._media_name, '')) IN (
              'tv', 'print', 'magazine', 'newspaper', 'ooh'
            )
          )
          THEN p.planned_impressions
        ELSE CAST(NULL AS FLOAT64)
      END AS `_impressions`,
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

;

-- Direct CM360 conversion integration. This second write joins the persistent
-- source history to one unique DCM delivery row at package/date/placement/creative
-- grain and emits conversion-only evidence rows when no such delivery row exists.
CREATE TEMP TABLE v3_delivery_base AS
SELECT *
FROM `looker-studio-pro-452620.master_stg.data_model_v3`;

CREATE OR REPLACE TABLE `looker-studio-pro-452620.master_stg.data_model_v3`
CLUSTER BY _advertiser AS
WITH cm360_by_detail AS (
  SELECT
    model_detail_key,
    ANY_VALUE(date) AS conversion_date,
    ANY_VALUE(package_id) AS package_id,
    ANY_VALUE(placement_id) AS placement_id,
    ANY_VALUE(creative) AS creative,
    ANY_VALUE(advertiser) AS advertiser,
    ANY_VALUE(campaign) AS campaign,
    ANY_VALUE(site) AS site,
    ANY_VALUE(package_roadblock) AS package_roadblock,
    ARRAY_AGG(DISTINCT NULLIF(TRIM(activity_group), '') IGNORE NULLS ORDER BY NULLIF(TRIM(activity_group), '')) AS conv_activity_groups,
    ARRAY_AGG(DISTINCT NULLIF(TRIM(activity), '') IGNORE NULLS ORDER BY NULLIF(TRIM(activity), '')) AS conv_activities,
    SUM(total_conversions) AS conv_total_conversions,
    SUM(activity_click_through_conversions) AS conv_click_through_conversions,
    SUM(activity_view_through_conversions) AS conv_view_through_conversions,
    SUM(total_conversions_revenue) AS conv_total_revenue,
    SUM(activity_click_through_revenue) AS conv_click_through_revenue,
    SUM(activity_view_through_revenue) AS conv_view_through_revenue,
    SUM(IF(LOWER(TRIM(activity)) = 'site visit', total_conversions, 0)) AS conv_site_visits,
    SUM(IF(LOWER(TRIM(activity)) = 'view product', total_conversions, 0)) AS conv_view_products,
    SUM(IF(LOWER(TRIM(activity)) = 'add to cart', total_conversions, 0)) AS conv_add_to_carts,
    SUM(IF(LOWER(TRIM(activity)) = 'begin checkout', total_conversions, 0)) AS conv_begin_checkouts,
    SUM(IF(LOWER(TRIM(activity)) = 'purchase', total_conversions, 0)) AS conv_purchases,
    COUNT(*) AS conv_source_activity_record_count,
    ARRAY_AGG(DISTINCT source_table_name ORDER BY source_table_name) AS source_table_names,
    MAX(source_exported_at) AS conv_source_exported_at,
    MAX(data_refresh_date) AS conv_data_refresh_date,
    MAX(staged_at) AS conv_staged_at
  FROM `looker-studio-pro-452620.master_stg.rtl_cm360_direct_conversions`
  GROUP BY model_detail_key
),
dcm_detail_counts AS (
  SELECT
    CONCAT(
      LOWER(TRIM(_package_id)), '|', FORMAT_DATE('%F', _date), '|',
      LOWER(TRIM(_placement_id)), '|', LOWER(TRIM(_creative_name))
    ) AS model_detail_key,
    COUNT(*) AS dcm_detail_match_count
  FROM v3_delivery_base
  WHERE qa_v3_source_detail_type = 'dcm'
  GROUP BY model_detail_key
),
cm360_ready AS (
  SELECT
    c.*,
    COALESCE(d.dcm_detail_match_count, 0) AS dcm_detail_match_count,
    CASE
      WHEN COALESCE(d.dcm_detail_match_count, 0) = 1 THEN 'matched_unique'
      WHEN COALESCE(d.dcm_detail_match_count, 0) = 0 THEN 'conversion_only'
      ELSE 'ambiguous_delivery_match'
    END AS conv_model_detail_join_status
  FROM cm360_by_detail AS c
  LEFT JOIN dcm_detail_counts AS d USING (model_detail_key)
),
base_with_direct AS (
  SELECT
    b.* REPLACE (
      IF(c.model_detail_key IS NOT NULL, 'dcm | cm360_direct_conversion', b.qa_data_source) AS qa_data_source,
      IF(c.model_detail_key IS NOT NULL, c.conv_staged_at, b.qa_data_source_refresh_at) AS qa_data_source_refresh_at,
      IF(c.model_detail_key IS NOT NULL, 'dcm | cm360_direct_conversion', b.qa_row_data_sources_available) AS qa_row_data_sources_available,
      IF(c.model_detail_key IS NOT NULL, CAST(NULL AS STRING), b.conv_activity) AS conv_activity,
      IF(c.model_detail_key IS NOT NULL, CAST(c.conv_total_conversions AS INT64), b.conv_total_conversions) AS conv_total_conversions,
      IF(c.model_detail_key IS NOT NULL, CAST(NULL AS FLOAT64), b.conv_source_impressions) AS conv_source_impressions,
      IF(c.model_detail_key IS NOT NULL, CAST(NULL AS FLOAT64), b.conv_source_clicks) AS conv_source_clicks,
      IF(c.model_detail_key IS NOT NULL, c.conv_source_activity_record_count, b.conv_source_row_count) AS conv_source_row_count,
      IF(c.model_detail_key IS NOT NULL, c.site, b.conv_site) AS conv_site,
      IF(c.model_detail_key IS NOT NULL, c.site, b.conv_site_cm360) AS conv_site_cm360,
      IF(c.model_detail_key IS NOT NULL, c.campaign, b.conv_campaign) AS conv_campaign,
      IF(c.model_detail_key IS NOT NULL, c.package_roadblock, b.conv_package_roadblock) AS conv_package_roadblock,
      IF(c.model_detail_key IS NOT NULL, c.creative, b.conv_creative) AS conv_creative,
      IF(c.model_detail_key IS NOT NULL, c.conv_staged_at, b.conv_loaded_at) AS conv_loaded_at,
      IF(c.model_detail_key IS NOT NULL, CAST(NULL AS STRING), b.conv_source_sheet_id) AS conv_source_sheet_id,
      IF(c.model_detail_key IS NOT NULL, CAST(NULL AS STRING), b.conv_source_sheet_tab) AS conv_source_sheet_tab,
      IF(c.model_detail_key IS NOT NULL, CAST(NULL AS STRING), b.conv_source_sheet_gid) AS conv_source_sheet_gid
    ),
    c.advertiser AS conv_advertiser,
    c.conv_activity_groups,
    c.conv_activities,
    c.placement_id AS conv_placement,
    c.conv_click_through_conversions,
    c.conv_view_through_conversions,
    c.conv_total_revenue,
    c.conv_click_through_revenue,
    c.conv_view_through_revenue,
    c.conv_site_visits,
    c.conv_view_products,
    c.conv_add_to_carts,
    c.conv_begin_checkouts,
    c.conv_purchases,
    c.model_detail_key AS conv_model_detail_key,
    c.conv_model_detail_join_status,
    c.dcm_detail_match_count AS conv_model_detail_match_count,
    c.conv_source_activity_record_count,
    c.source_table_names AS conv_source_table_names,
    c.conv_source_exported_at,
    c.conv_data_refresh_date,
    c.conv_staged_at,
    TO_HEX(SHA256(CONCAT('base|', TO_JSON_STRING(b)))) AS qa_cm360_record_key
  FROM v3_delivery_base AS b
  LEFT JOIN cm360_ready AS c
    ON b.qa_v3_source_detail_type = 'dcm'
   AND CONCAT(
     LOWER(TRIM(b._package_id)), '|', FORMAT_DATE('%F', b._date), '|',
     LOWER(TRIM(b._placement_id)), '|', LOWER(TRIM(b._creative_name))
   ) = c.model_detail_key
   AND c.conv_model_detail_join_status = 'matched_unique'
),
conversion_only_rows AS (
  SELECT
    b.* REPLACE (
      'source_actual' AS qa_v3_row_type,
      'package_date_placement_creative_conversion' AS qa_v3_metric_grain,
      'cm360_direct_conversion' AS qa_v3_source_detail_type,
      c.advertiser AS qa_v3_ad_name,
      'Direct CM360 conversions without a unique delivery-row match' AS qa_v3_metric_behavior,
      'digital' AS qa_media_data_type,
      'cm360_direct_conversion' AS qa_row_data_source_primary,
      ARRAY_TO_STRING(c.source_table_names, ' | ') AS qa_data_source,
      c.conv_staged_at AS qa_data_source_refresh_at,
      'cm360_direct_conversion' AS qa_row_data_sources_available,
      c.conv_model_detail_join_status AS qa_data_issues,
      c.package_id AS _package_id,
      c.conversion_date AS _date,
      c.conversion_date AS _start_date,
      c.conversion_date AS _end_date,
      c.advertiser AS _advertiser_name,
      c.advertiser AS _advertiser_short_name,
      c.advertiser AS _advertiser,
      c.campaign AS _campaign_name,
      c.campaign AS _campaign_friendly,
      c.package_roadblock AS _package_name,
      c.package_roadblock AS _package_name_friendly,
      c.placement_id AS _placement_id,
      c.placement_id AS _placement_name,
      c.creative AS _creative_name,
      c.package_roadblock AS initiative,
      CAST(NULL AS STRING) AS conv_activity,
      CAST(c.conv_total_conversions AS INT64) AS conv_total_conversions,
      CAST(NULL AS FLOAT64) AS conv_source_impressions,
      CAST(NULL AS FLOAT64) AS conv_source_clicks,
      c.conv_source_activity_record_count AS conv_source_row_count,
      c.site AS conv_site,
      c.site AS conv_site_cm360,
      c.campaign AS conv_campaign,
      c.package_roadblock AS conv_package_roadblock,
      c.creative AS conv_creative,
      c.conv_staged_at AS conv_loaded_at,
      CAST(NULL AS STRING) AS conv_source_sheet_id,
      CAST(NULL AS STRING) AS conv_source_sheet_tab,
      CAST(NULL AS STRING) AS conv_source_sheet_gid
    ),
    c.advertiser AS conv_advertiser,
    c.conv_activity_groups,
    c.conv_activities,
    c.placement_id AS conv_placement,
    c.conv_click_through_conversions,
    c.conv_view_through_conversions,
    c.conv_total_revenue,
    c.conv_click_through_revenue,
    c.conv_view_through_revenue,
    c.conv_site_visits,
    c.conv_view_products,
    c.conv_add_to_carts,
    c.conv_begin_checkouts,
    c.conv_purchases,
    c.model_detail_key AS conv_model_detail_key,
    c.conv_model_detail_join_status,
    c.dcm_detail_match_count AS conv_model_detail_match_count,
    c.conv_source_activity_record_count,
    c.source_table_names AS conv_source_table_names,
    c.conv_source_exported_at,
    c.conv_data_refresh_date,
    c.conv_staged_at,
    TO_HEX(SHA256(CONCAT('cm360|', c.model_detail_key))) AS qa_cm360_record_key
  FROM cm360_ready AS c
  LEFT JOIN v3_delivery_base AS b ON FALSE
  WHERE c.conv_model_detail_join_status != 'matched_unique'
)
-- Direct CM360 source values are joined only at the approved delivery detail grain.
SELECT * FROM base_with_direct
UNION ALL
SELECT * FROM conversion_only_rows;

ALTER TABLE `looker-studio-pro-452620.master_stg.data_model_v3`
SET OPTIONS (
  description = 'V3 master evidence model with direct CM360 RTL conversion history. Built by model/final_model/create_master_stg_data_model_v3.sql. The builder does not read the legacy compatibility table; direct CM360 conversions join delivery only at package/date/placement/creative grain and otherwise remain conversion-only evidence rows.'
);
