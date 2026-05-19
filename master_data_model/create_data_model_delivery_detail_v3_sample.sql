-- Sample delivery detail model for the creative-grain recovery.
--
-- Purpose:
--   Preserve delivery at the source/detail grain instead of collapsing creative
--   into package/date rows. Package budget fields are included only as
--   non-summable context.

CREATE OR REPLACE VIEW `looker-studio-pro-452620.master_stg.data_model_delivery_detail_v3_sample` AS
WITH
package_context_src AS (
  SELECT
    `_package_id`,
    `_date`,
    `_advertiser_name`,
    `_advertiser_short_name`,
    `_campaign_name`,
    `_campaign_friendly`,
    `_product_code`,
    `_product_name`,
    `_package_type`,
    `_package_name`,
    `_package_name_friendly`,
    ADIF_channel,
    `_placement_id`,
    `_placement_name`,
    `_supplier_code`,
    `_supplier_name`,
    `_supplier_logo`,
    p_buy_type,
    p_buy_category,
    `_channel`,
    qa_channel_raw,
    `_channel_group`,
    `_media_name`,
    p_cost_method,
    `_planned_spend` AS planned_daily_spend_doNotSum,
    `_planned_impressions` AS planned_daily_impressions_doNotSum,
    p_planned_amount_doNotSum,
    p_planned_impressions_doNotSum,
    p_planned_units_doNotSum,
    p_unit_type,
    p_rate,
    p_max_report_date
  FROM `looker-studio-pro-452620.master_stg.data_model`
),

package_context AS (
  SELECT * EXCEPT(context_rank)
  FROM (
    SELECT
      package_context_src.*,
      ROW_NUMBER() OVER (
        PARTITION BY `_package_id`, `_date`
        ORDER BY
          IF(planned_daily_spend_doNotSum IS NOT NULL OR planned_daily_impressions_doNotSum IS NOT NULL, 0, 1),
          IF(`_advertiser_name` IS NOT NULL, 0, 1),
          IF(`_package_name` IS NOT NULL, 0, 1)
      ) AS context_rank
    FROM package_context_src
  )
  WHERE context_rank = 1
),

prisma_meta AS (
  SELECT x.*
  FROM (
    SELECT
      (ARRAY_AGG(p ORDER BY p.report_date DESC NULLS LAST))[OFFSET(0)] AS x
    FROM `looker-studio-pro-452620.20250327_data_model.prisma_expanded_full` AS p
    WHERE p.package_type != 'Child'
      AND p.start_date >= DATE '2025-01-01'
    GROUP BY p.package_id
  )
),

dcm_detail AS (
  SELECT
    'dcm' AS source_row_type,
    d.package_id AS `_package_id`,
    DATE(d.date) AS `_date`,
    d.placement_id AS `_placement_id`,
    ARRAY_AGG(d.placement IGNORE NULLS ORDER BY d.placement LIMIT 1)[SAFE_OFFSET(0)] AS `_placement_name`,
    CAST(d.ad AS STRING) AS ad_name,
    CAST(d.creative AS STRING) AS creative,
    ARRAY_AGG(d.advertiser IGNORE NULLS ORDER BY d.advertiser LIMIT 1)[SAFE_OFFSET(0)] AS source_advertiser_name,
    ARRAY_AGG(d.campaign IGNORE NULLS ORDER BY d.campaign LIMIT 1)[SAFE_OFFSET(0)] AS source_campaign_name,
    ARRAY_AGG(d.package_roadblock IGNORE NULLS ORDER BY d.package_roadblock LIMIT 1)[SAFE_OFFSET(0)] AS source_package_name,
    ARRAY_AGG(d.site IGNORE NULLS ORDER BY d.site LIMIT 1)[SAFE_OFFSET(0)] AS source_supplier_name,
    ARRAY_AGG(d.p_channel_group IGNORE NULLS ORDER BY d.p_channel_group LIMIT 1)[SAFE_OFFSET(0)] AS source_channel_group,
    SUM(d.daily_recalculated_cost) AS dcm_daily_recalculated_cost,
    SUM(d.daily_recalculated_imps) AS dcm_daily_recalculated_imps,
    SUM(d.impressions) AS dcm_impressions,
    SUM(d.media_cost) AS dcm_media_cost,
    SUM(d.clicks) AS dcm_clicks,
    SUM(d.rich_media_video_plays) AS dcm_video_plays,
    SUM(d.rich_media_video_completions) AS dcm_video_comps,
    CAST(NULL AS FLOAT64) AS fpd_orig_impressions,
    CAST(NULL AS FLOAT64) AS fpd_orig_spend,
    CAST(NULL AS FLOAT64) AS fpd_orig_clicks,
    CAST(NULL AS INT64) AS fpd_orig_sends,
    CAST(NULL AS INT64) AS fpd_orig_opens,
    CAST(NULL AS STRING) AS creative_git_link,
    CAST(NULL AS FLOAT64) AS fpd_updated_impressions,
    CAST(NULL AS FLOAT64) AS fpd_updated_spend,
    CAST(NULL AS STRING) AS fpd_updated_suppliers,
    CAST(NULL AS STRING) AS fpd_updated_initiatives,
    SUM(d.daily_recalculated_cost) AS final_spend,
    CAST(SUM(d.impressions) AS FLOAT64) AS final_impressions,
    CAST(SUM(d.clicks) AS FLOAT64) AS final_clicks,
    CAST(SUM(d.rich_media_video_plays) AS FLOAT64) AS final_video_plays,
    CAST(SUM(d.rich_media_video_completions) AS FLOAT64) AS final_video_comps
  FROM `looker-studio-pro-452620.DCM.20250505_costModel_v5` AS d
  WHERE DATE(d.date) >= DATE '2025-01-01'
    AND d.package_id IS NOT NULL
  GROUP BY
    d.package_id,
    DATE(d.date),
    d.placement_id,
    CAST(d.ad AS STRING),
    CAST(d.creative AS STRING)
),

fpd_original_detail AS (
  SELECT
    'fpd_original' AS source_row_type,
    f.package_id AS `_package_id`,
    DATE(f.date_final) AS `_date`,
    COALESCE(f.partner_placement_name, f.partner_packagePlacement_name, f.package_id) AS `_placement_id`,
    COALESCE(f.partner_placement_name, f.partner_packagePlacement_name, f.package_name) AS `_placement_name`,
    CAST(NULL AS STRING) AS ad_name,
    CAST(f.partner_creative_name AS STRING) AS creative,
    ARRAY_AGG(f.client IGNORE NULLS ORDER BY f.client LIMIT 1)[SAFE_OFFSET(0)] AS source_advertiser_name,
    ARRAY_AGG(f.campaign IGNORE NULLS ORDER BY f.campaign LIMIT 1)[SAFE_OFFSET(0)] AS source_campaign_name,
    ARRAY_AGG(f.package_name IGNORE NULLS ORDER BY f.package_name LIMIT 1)[SAFE_OFFSET(0)] AS source_package_name,
    ARRAY_AGG(f.site IGNORE NULLS ORDER BY f.site LIMIT 1)[SAFE_OFFSET(0)] AS source_supplier_name,
    ARRAY_AGG(f.channel IGNORE NULLS ORDER BY f.channel LIMIT 1)[SAFE_OFFSET(0)] AS source_channel_group,
    CAST(NULL AS FLOAT64) AS dcm_daily_recalculated_cost,
    CAST(NULL AS INT64) AS dcm_daily_recalculated_imps,
    CAST(NULL AS INT64) AS dcm_impressions,
    CAST(NULL AS FLOAT64) AS dcm_media_cost,
    CAST(NULL AS INT64) AS dcm_clicks,
    CAST(NULL AS INT64) AS dcm_video_plays,
    CAST(NULL AS INT64) AS dcm_video_comps,
    SUM(f.impressions) AS fpd_orig_impressions,
    SUM(f.spend) AS fpd_orig_spend,
    SUM(f.clicks) AS fpd_orig_clicks,
    SAFE_CAST(ROUND(SUM(f.sends)) AS INT64) AS fpd_orig_sends,
    SAFE_CAST(ROUND(SUM(f.opens)) AS INT64) AS fpd_orig_opens,
    CAST(f.creative_git_link AS STRING) AS creative_git_link,
    CAST(NULL AS FLOAT64) AS fpd_updated_impressions,
    CAST(NULL AS FLOAT64) AS fpd_updated_spend,
    CAST(NULL AS STRING) AS fpd_updated_suppliers,
    CAST(NULL AS STRING) AS fpd_updated_initiatives,
    SUM(f.spend) AS final_spend,
    SUM(f.impressions) AS final_impressions,
    SUM(f.clicks) AS final_clicks,
    CAST(NULL AS FLOAT64) AS final_video_plays,
    CAST(NULL AS FLOAT64) AS final_video_comps
  FROM `looker-studio-pro-452620.landing.fpd_data_ranged_shortcutsFolder` AS f
  WHERE DATE(f.date_final) >= DATE '2025-01-01'
    AND f.package_id IS NOT NULL
  GROUP BY
    f.package_id,
    DATE(f.date_final),
    COALESCE(f.partner_placement_name, f.partner_packagePlacement_name, f.package_id),
    COALESCE(f.partner_placement_name, f.partner_packagePlacement_name, f.package_name),
    CAST(f.partner_creative_name AS STRING),
    CAST(f.creative_git_link AS STRING)
),

fpd_updated_package_detail AS (
  SELECT
    'fpd_updated_package' AS source_row_type,
    u.package_id AS `_package_id`,
    DATE(u.date) AS `_date`,
    u.package_id AS `_placement_id`,
    ARRAY_AGG(u.package_name IGNORE NULLS ORDER BY u.package_name LIMIT 1)[SAFE_OFFSET(0)] AS `_placement_name`,
    CAST(NULL AS STRING) AS ad_name,
    CAST(NULL AS STRING) AS creative,
    CAST(NULL AS STRING) AS source_advertiser_name,
    CAST(NULL AS STRING) AS source_campaign_name,
    ARRAY_AGG(u.package_name IGNORE NULLS ORDER BY u.package_name LIMIT 1)[SAFE_OFFSET(0)] AS source_package_name,
    STRING_AGG(DISTINCT u.supplier_name, ', ' ORDER BY u.supplier_name) AS source_supplier_name,
    CAST(NULL AS STRING) AS source_channel_group,
    CAST(NULL AS FLOAT64) AS dcm_daily_recalculated_cost,
    CAST(NULL AS INT64) AS dcm_daily_recalculated_imps,
    CAST(NULL AS INT64) AS dcm_impressions,
    CAST(NULL AS FLOAT64) AS dcm_media_cost,
    CAST(NULL AS INT64) AS dcm_clicks,
    CAST(NULL AS INT64) AS dcm_video_plays,
    CAST(NULL AS INT64) AS dcm_video_comps,
    CAST(NULL AS FLOAT64) AS fpd_orig_impressions,
    CAST(NULL AS FLOAT64) AS fpd_orig_spend,
    CAST(NULL AS FLOAT64) AS fpd_orig_clicks,
    CAST(NULL AS INT64) AS fpd_orig_sends,
    CAST(NULL AS INT64) AS fpd_orig_opens,
    CAST(NULL AS STRING) AS creative_git_link,
    SUM(u.daily_fpd_impressions) AS fpd_updated_impressions,
    SUM(u.daily_fpd_spend) AS fpd_updated_spend,
    STRING_AGG(DISTINCT u.supplier_name, ', ' ORDER BY u.supplier_name) AS fpd_updated_suppliers,
    STRING_AGG(DISTINCT u.initiative, ', ' ORDER BY u.initiative) AS fpd_updated_initiatives,
    SUM(u.daily_fpd_spend) AS final_spend,
    SUM(u.daily_fpd_impressions) AS final_impressions,
    CAST(NULL AS FLOAT64) AS final_clicks,
    CAST(NULL AS FLOAT64) AS final_video_plays,
    CAST(NULL AS FLOAT64) AS final_video_comps
  FROM `looker-studio-pro-452620.landing.adif_updated_fpd_daily` AS u
  WHERE DATE(u.date) >= DATE '2025-01-01'
    AND u.package_id IS NOT NULL
  GROUP BY u.package_id, DATE(u.date)
),

detail_union AS (
  SELECT * FROM dcm_detail
  UNION ALL
  SELECT * FROM fpd_original_detail
  UNION ALL
  SELECT * FROM fpd_updated_package_detail
)

SELECT
  d.source_row_type,
  d.`_package_id`,
  d.`_date`,
  COALESCE(pc.`_advertiser_name`, pm.advertiser_name, d.source_advertiser_name) AS `_advertiser_name`,
  COALESCE(pc.`_advertiser_short_name`, pm.advertiser_short_name) AS `_advertiser_short_name`,
  COALESCE(pc.`_campaign_name`, pm.campaign_name, d.source_campaign_name) AS `_campaign_name`,
  COALESCE(pc.`_campaign_friendly`, pm.campaign_friendly, pm.campaign_name, d.source_campaign_name) AS `_campaign_friendly`,
  COALESCE(pc.`_product_code`, pm.product_code) AS `_product_code`,
  COALESCE(pc.`_product_name`, pm.product_name) AS `_product_name`,
  COALESCE(pc.`_package_type`, pm.package_type, 'UnmatchedDigitalPackage') AS `_package_type`,
  COALESCE(pc.`_package_name`, pm.package_name, d.source_package_name, d.`_package_id`) AS `_package_name`,
  COALESCE(pc.`_package_name_friendly`, pm.p_package_friendly) AS `_package_name_friendly`,
  COALESCE(pm.initative, pc.`_package_name`, pm.package_name) AS initiative,
  COALESCE(pc.ADIF_channel, 'Unmapped') AS ADIF_channel,
  d.`_placement_id`,
  COALESCE(d.`_placement_name`, pm.placement_name, pc.`_placement_name`) AS `_placement_name`,
  d.ad_name,
  d.creative,
  COALESCE(pc.`_supplier_code`, pm.supplier_code) AS `_supplier_code`,
  COALESCE(pc.`_supplier_name`, pm.supplier_name, d.source_supplier_name) AS `_supplier_name`,
  pc.`_supplier_logo`,
  COALESCE(pc.p_buy_type, pm.buy_type, d.source_channel_group) AS p_buy_type,
  COALESCE(pc.p_buy_category, pm.buy_category, d.source_channel_group) AS p_buy_category,
  COALESCE(pc.`_channel`, pm.channel, d.source_channel_group) AS `_channel`,
  COALESCE(pc.qa_channel_raw, pm.channel_raw, d.source_channel_group) AS qa_channel_raw,
  COALESCE(pc.`_channel_group`, pm.channel_group, d.source_channel_group) AS `_channel_group`,
  COALESCE(pc.`_media_name`, pm.media_name, 'Digital') AS `_media_name`,
  pc.planned_daily_spend_doNotSum,
  pc.planned_daily_impressions_doNotSum,
  pc.p_planned_amount_doNotSum,
  pc.p_planned_impressions_doNotSum,
  pc.p_planned_units_doNotSum,
  pc.p_unit_type,
  pc.p_rate,
  d.dcm_daily_recalculated_cost,
  d.dcm_daily_recalculated_imps,
  d.dcm_impressions,
  d.dcm_media_cost,
  d.dcm_clicks,
  d.dcm_video_plays,
  d.dcm_video_comps,
  d.fpd_orig_impressions,
  d.fpd_orig_spend,
  d.fpd_orig_clicks,
  d.fpd_orig_sends,
  d.fpd_orig_opens,
  d.creative_git_link,
  d.fpd_updated_impressions,
  d.fpd_updated_spend,
  d.fpd_updated_suppliers,
  d.fpd_updated_initiatives,
  d.final_spend AS `_spend`,
  d.final_impressions AS `_impressions`,
  d.final_clicks AS `_clicks`,
  d.final_video_plays AS `_video_plays`,
  d.final_video_comps AS `_video_comps`,
  CURRENT_TIMESTAMP() AS model_view_runtime_timestamp
FROM detail_union AS d
LEFT JOIN package_context AS pc
  ON d.`_package_id` = pc.`_package_id`
 AND d.`_date` = pc.`_date`
LEFT JOIN prisma_meta AS pm
  ON d.`_package_id` = pm.package_id;
