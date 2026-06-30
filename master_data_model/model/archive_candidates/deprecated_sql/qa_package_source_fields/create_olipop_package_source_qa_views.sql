-- Build isolated Olipop QA views for package-level source attribution.
--
-- Scope:
--   Advertiser: Olipop
--   Dates: 2026-03-01 through 2026-05-31, inclusive
--
-- Produces:
--   Two unchanged baseline views and two candidate views with:
--   - qa_pkg_primary_data_source
--   - qa_pkg_data_sources_available
--
-- Safety:
--   This script does not replace production objects. All four QA views are
--   temporary validation surfaces and are safe to delete after review.

CREATE OR REPLACE VIEW
  `looker-studio-pro-452620.master_stg.qa_pkg_sources_olipop_20260301_20260531_evidence_base`
OPTIONS (
  description = 'Temporary Olipop baseline for package-source QA, limited to 2026-03-01 through 2026-05-31. Reads master_stg.data_model unchanged. Safe to delete after package-source validation. Cleanup owner: master_data_model package-source QA.'
) AS
SELECT
  -- QA BASELINE: Unchanged evidence-model rows for comparison with the package
  -- source candidate. Safe to delete after validation.
  *,
  TO_HEX(MD5(TO_JSON_STRING(STRUCT(
    `_package_id`,
    `_date`,
    `s_platform`,
    `s_campaign_id`,
    `s_ad_group_id`,
    `s_ad_id`,
    `_placement_id`
  )))) AS qa_validation_row_key
FROM `looker-studio-pro-452620.master_stg.data_model`
WHERE LOWER(`_advertiser_name`) = 'olipop'
  AND `_date` BETWEEN DATE '2026-03-01' AND DATE '2026-05-31';

CREATE OR REPLACE VIEW
  `looker-studio-pro-452620.master_stg.qa_pkg_sources_olipop_20260301_20260531_mart_base`
OPTIONS (
  description = 'Temporary Olipop reporting-mart baseline for package-source QA, limited to 2026-03-01 through 2026-05-31. Reads master_stg.data_model_mart unchanged. Safe to delete after package-source validation. Cleanup owner: master_data_model package-source QA.'
) AS
SELECT
  -- QA BASELINE: Unchanged reporting-mart rows for comparison with the package
  -- source candidate. Safe to delete after validation.
  *,
  TO_HEX(MD5(TO_JSON_STRING(STRUCT(
    `_package_id`,
    `_date`,
    `s_platform`,
    `s_campaign_id`,
    `s_ad_group_id`,
    `s_ad_id`,
    `_placement_id`
  )))) AS qa_validation_row_key
FROM `looker-studio-pro-452620.master_stg.data_model_mart`
WHERE LOWER(`_advertiser_name`) = 'olipop'
  AND `_date` BETWEEN DATE '2026-03-01' AND DATE '2026-05-31';

CREATE OR REPLACE VIEW
  `looker-studio-pro-452620.master_stg.qa_pkg_sources_olipop_20260301_20260531_evidence_candidate`
OPTIONS (
  description = 'Temporary Olipop evidence-model candidate adding package-level primary and available source fields for 2026-03-01 through 2026-05-31. Versus qa_pkg_sources_olipop_20260301_20260531_evidence_base, only qa_pkg_primary_data_source and qa_pkg_data_sources_available are added. Source file: master_data_model/qa_package_source_fields/create_olipop_package_source_qa_views.sql. Safe to delete after validation. Cleanup owner: master_data_model package-source QA.'
) AS
WITH
scoped_rows AS (
  SELECT
    *,
    TO_HEX(MD5(TO_JSON_STRING(STRUCT(
      `_package_id`,
      `_date`,
      `s_platform`,
      `s_campaign_id`,
      `s_ad_group_id`,
      `s_ad_id`,
      `_placement_id`
    )))) AS qa_validation_row_key
  FROM `looker-studio-pro-452620.master_stg.data_model`
  WHERE LOWER(`_advertiser_name`) = 'olipop'
    AND `_date` BETWEEN DATE '2026-03-01' AND DATE '2026-05-31'
),

row_source_contributors AS (
  SELECT
    scoped_rows.*,
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
  FROM scoped_rows
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
  -- QA CANDIDATE: Adds package-level actual-source attribution and the complete
  -- package source inventory. Existing rows and metrics remain unchanged.
  row_source_contributors.* EXCEPT(qa_internal_metric_source_contributors),
  package_qa.qa_pkg_primary_data_source,
  package_qa.qa_pkg_data_sources_available
FROM row_source_contributors
LEFT JOIN package_qa USING (`_package_id`);

CREATE OR REPLACE VIEW
  `looker-studio-pro-452620.master_stg.qa_pkg_sources_olipop_20260301_20260531_mart_candidate`
OPTIONS (
  description = 'Temporary Olipop reporting-mart candidate adding package-level primary and available source fields for 2026-03-01 through 2026-05-31. Versus qa_pkg_sources_olipop_20260301_20260531_mart_base, only qa_pkg_primary_data_source and qa_pkg_data_sources_available are added after mart filtering. Source file: master_data_model/qa_package_source_fields/create_olipop_package_source_qa_views.sql. Safe to delete after validation. Cleanup owner: master_data_model package-source QA.'
) AS
WITH
scoped_rows AS (
  SELECT
    *,
    TO_HEX(MD5(TO_JSON_STRING(STRUCT(
      `_package_id`,
      `_date`,
      `s_platform`,
      `s_campaign_id`,
      `s_ad_group_id`,
      `s_ad_id`,
      `_placement_id`
    )))) AS qa_validation_row_key
  FROM `looker-studio-pro-452620.master_stg.data_model_mart`
  WHERE LOWER(`_advertiser_name`) = 'olipop'
    AND `_date` BETWEEN DATE '2026-03-01' AND DATE '2026-05-31'
),

row_source_contributors AS (
  SELECT
    scoped_rows.*,
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
  FROM scoped_rows
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
  -- QA CANDIDATE: Recalculates package source attribution from the mart's
  -- filtered rows. Existing rows and metrics remain unchanged.
  row_source_contributors.* EXCEPT(qa_internal_metric_source_contributors),
  package_qa.qa_pkg_primary_data_source,
  package_qa.qa_pkg_data_sources_available
FROM row_source_contributors
LEFT JOIN package_qa USING (`_package_id`);
