-- Build reporting mart over the generalized package/date data model.
--
-- Purpose:
--   Keep the master model as the evidence layer, then apply reporting-only row
--   exclusions and recalculate package rollups after those exclusions.

CREATE OR REPLACE VIEW `looker-studio-pro-452620.master_stg.data_model_mart` AS
WITH
filtered_rows AS (
  SELECT *
  FROM `looker-studio-pro-452620.master_stg.data_model`
  WHERE NOT CONTAINS_SUBSTR(`qa_data_issues`, 'low_signal_dcm')
),

with_rollups AS (
  SELECT
    * EXCEPT(
      `qa_pkg_est_spend_doNotSum`,
      `qa_pkg_est_impressions_doNotSum`,
      `qa_pkg_act_spend_doNotSum`,
      `qa_pkg_act_impressions_doNotSum`,
      `qa_pkg_act_clicks_doNotSum`,
      `qa_pkg_fpd_impressions_doNotSum`,
      `qa_pkg_fpd_spend_doNotSum`,
      `qa_package_spend_over_plan_flag`,
      `qa_pkg_primary_data_source`,
      `qa_pkg_data_sources_available`
    ),
    SUM(COALESCE(`_planned_spend`, 0)) OVER (PARTITION BY `_package_id`) AS `qa_pkg_est_spend_doNotSum`,
    SUM(COALESCE(`_planned_impressions`, 0)) OVER (PARTITION BY `_package_id`) AS `qa_pkg_est_impressions_doNotSum`,
    SUM(COALESCE(`_spend`, 0)) OVER (PARTITION BY `_package_id`) AS `qa_pkg_act_spend_doNotSum`,
    SUM(COALESCE(`_impressions`, 0)) OVER (PARTITION BY `_package_id`) AS `qa_pkg_act_impressions_doNotSum`,
    SUM(COALESCE(`_clicks`, 0)) OVER (PARTITION BY `_package_id`) AS `qa_pkg_act_clicks_doNotSum`,
    SUM(COALESCE(`fpd_impressions`, 0)) OVER (PARTITION BY `_package_id`) AS `qa_pkg_fpd_impressions_doNotSum`,
    SUM(COALESCE(`fpd_spend`, 0)) OVER (PARTITION BY `_package_id`) AS `qa_pkg_fpd_spend_doNotSum`
  FROM filtered_rows
),

row_source_contributors AS (
  SELECT
    with_rollups.*,
    ARRAY(
      SELECT DISTINCT source_name
      FROM UNNEST(ARRAY_CONCAT(
        IF(`man_daily_spend` IS NOT NULL, ['manual_package_edits'], []),
        IF(`man_daily_impressions` IS NOT NULL, ['manual_package_edits'], []),
        IF(`man_daily_clicks` IS NOT NULL, ['manual_package_edits'], []),
        IF(`man_daily_video_plays` IS NOT NULL, ['manual_package_edits'], []),
        IF(`man_daily_video_comps` IS NOT NULL, ['manual_package_edits'], []),
        IF(
          `man_daily_spend` IS NULL AND `_spend` IS NOT NULL,
          [CASE
            WHEN `qa_media_data_type` = 'digital' AND NULLIF(`fpd_spend`, 0) IS NOT NULL THEN 'fpd'
            WHEN `qa_media_data_type` = 'digital' AND `dcm_daily_recalculated_cost` IS NOT NULL THEN 'dcm'
            WHEN `qa_media_data_type` = 'social' AND STARTS_WITH(COALESCE(`s_record_source`, ''), 'wp_') THEN 'wp_search_data_template'
            WHEN `qa_media_data_type` = 'social' THEN 'social'
            WHEN `qa_media_data_type` = 'tv' THEN 'tv_combined'
            WHEN `qa_media_data_type` = 'amazon_ads' THEN 'amazon_ads'
            ELSE NULL
          END],
          []
        ),
        IF(
          `man_daily_impressions` IS NULL AND `_impressions` IS NOT NULL,
          [CASE
            WHEN `qa_media_data_type` = 'digital' AND NULLIF(`fpd_impressions`, 0) IS NOT NULL THEN 'fpd'
            WHEN `qa_media_data_type` = 'digital' AND `dcm_impressions` IS NOT NULL THEN 'dcm'
            WHEN `qa_media_data_type` = 'social' AND STARTS_WITH(COALESCE(`s_record_source`, ''), 'wp_') THEN 'wp_search_data_template'
            WHEN `qa_media_data_type` = 'social' THEN 'social'
            WHEN `qa_media_data_type` = 'tv' THEN 'tv_combined'
            WHEN `qa_media_data_type` = 'amazon_ads' THEN 'amazon_ads'
            ELSE NULL
          END],
          []
        ),
        IF(
          `man_daily_clicks` IS NULL AND `_clicks` IS NOT NULL,
          [CASE
            WHEN `qa_media_data_type` = 'digital' AND `fpd_clicks` IS NOT NULL THEN 'fpd'
            WHEN `qa_media_data_type` = 'digital' AND `dcm_clicks` IS NOT NULL THEN 'dcm'
            WHEN `qa_media_data_type` = 'social' AND STARTS_WITH(COALESCE(`s_record_source`, ''), 'wp_') THEN 'wp_search_data_template'
            WHEN `qa_media_data_type` = 'social' THEN 'social'
            WHEN `qa_media_data_type` = 'amazon_ads' THEN 'amazon_ads'
            ELSE NULL
          END],
          []
        ),
        IF(
          `man_daily_video_plays` IS NULL AND `_video_plays` IS NOT NULL,
          [CASE
            WHEN `qa_media_data_type` = 'digital' AND `dcm_video_plays` IS NOT NULL THEN 'dcm'
            WHEN `qa_media_data_type` = 'social' AND STARTS_WITH(COALESCE(`s_record_source`, ''), 'wp_') THEN 'wp_search_data_template'
            WHEN `qa_media_data_type` = 'social' THEN 'social'
            WHEN `qa_media_data_type` = 'amazon_ads' THEN 'amazon_ads'
            ELSE NULL
          END],
          []
        ),
        IF(
          `_video_views` IS NOT NULL,
          [CASE
            WHEN `qa_media_data_type` = 'digital' AND `dcm_video_plays` IS NOT NULL THEN 'dcm'
            WHEN `qa_media_data_type` = 'social' AND STARTS_WITH(COALESCE(`s_record_source`, ''), 'wp_') THEN 'wp_search_data_template'
            WHEN `qa_media_data_type` = 'social' THEN 'social'
            WHEN `qa_media_data_type` = 'amazon_ads' THEN 'amazon_ads'
            ELSE NULL
          END],
          []
        ),
        IF(
          `man_daily_video_comps` IS NULL AND `_video_comps` IS NOT NULL,
          [CASE
            WHEN `qa_media_data_type` = 'digital' AND `dcm_video_comps` IS NOT NULL THEN 'dcm'
            WHEN `qa_media_data_type` = 'social' AND STARTS_WITH(COALESCE(`s_record_source`, ''), 'wp_') THEN 'wp_search_data_template'
            WHEN `qa_media_data_type` = 'social' THEN 'social'
            WHEN `qa_media_data_type` = 'amazon_ads' THEN 'amazon_ads'
            ELSE NULL
          END],
          []
        )
      )) AS source_name
      WHERE source_name IS NOT NULL
      ORDER BY source_name
    ) AS qa_internal_metric_source_contributors
  FROM with_rollups
),

package_list AS (
  SELECT DISTINCT `_package_id`
  FROM row_source_contributors
),

package_contributors AS (
  SELECT
    `_package_id`,
    ARRAY_AGG(DISTINCT source_name ORDER BY source_name) AS contributing_sources
  FROM row_source_contributors,
  UNNEST(qa_internal_metric_source_contributors) AS source_name
  GROUP BY `_package_id`
),

package_available_source_tokens AS (
  SELECT DISTINCT
    `_package_id`,
    TRIM(source_name) AS source_name
  FROM row_source_contributors,
  UNNEST(SPLIT(COALESCE(`qa_row_data_sources_available`, ''), ' | ')) AS source_name
  WHERE TRIM(source_name) NOT IN ('', 'none')
),

package_available_sources AS (
  SELECT
    `_package_id`,
    STRING_AGG(source_name, ' | ' ORDER BY source_name) AS available_sources
  FROM package_available_source_tokens
  GROUP BY `_package_id`
),

package_qa AS (
  SELECT
    package_list.`_package_id`,
    CASE
      WHEN COALESCE(ARRAY_LENGTH(package_contributors.contributing_sources), 0) = 0 THEN 'none'
      WHEN ARRAY_LENGTH(package_contributors.contributing_sources) = 1
        THEN package_contributors.contributing_sources[SAFE_OFFSET(0)]
      ELSE 'multiple'
    END AS qa_pkg_primary_data_source,
    COALESCE(package_available_sources.available_sources, 'none') AS qa_pkg_data_sources_available
  FROM package_list
  LEFT JOIN package_contributors USING (`_package_id`)
  LEFT JOIN package_available_sources USING (`_package_id`)
)

SELECT
  -- LIVE VIEW NOTE: Reporting mart over the canonical evidence model. Applies
  -- reporting-only exclusions and recalculates package totals and package
  -- source attribution after filtering.
  row_source_contributors.* EXCEPT(qa_internal_metric_source_contributors),
  CASE
    WHEN `qa_pkg_est_spend_doNotSum` = 0 THEN NULL
    ELSE `qa_pkg_act_spend_doNotSum` > `qa_pkg_est_spend_doNotSum`
  END AS `qa_package_spend_over_plan_flag`,
  package_qa.qa_pkg_primary_data_source,
  package_qa.qa_pkg_data_sources_available
FROM row_source_contributors
LEFT JOIN package_qa USING (`_package_id`);
