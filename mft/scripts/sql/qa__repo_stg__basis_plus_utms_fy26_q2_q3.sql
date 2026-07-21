-- Purpose: validate FY26 Q2/Q3 Basis UTM enrichment after normalization changes.
-- Reads: repo_stg.basis_delivery and repo_stg.basis_plus_utms_v4_PnS_table.
-- Safe use: read-only grouped checks; each result is intentionally small.

-- 1. UTM completeness by channel.
SELECT
  COALESCE(REGEXP_EXTRACT(placement, r'\[CPM\]_([^_]+)_'), 'Other') AS channel,
  COUNT(*) AS record_count,
  COUNTIF(NULLIF(TRIM(utm_source), '') IS NULL) AS blank_utm_record_count,
  SUM(impressions) AS impressions,
  SUM(IF(NULLIF(TRIM(utm_source), '') IS NULL, impressions, 0)) AS blank_utm_impressions,
  ROUND(SUM(media_cost), 2) AS media_cost,
  ROUND(SUM(IF(NULLIF(TRIM(utm_source), '') IS NULL, media_cost, 0)), 2) AS blank_utm_media_cost,
  SUM(clicks) AS clicks,
  SUM(IF(NULLIF(TRIM(utm_source), '') IS NULL, clicks, 0)) AS blank_utm_clicks
FROM `looker-studio-pro-452620.repo_stg.basis_plus_utms_v4_PnS_table`
WHERE REGEXP_CONTAINS(placement, r'MASSMUTUAL005CP_')
GROUP BY channel
ORDER BY channel
LIMIT 20;

-- 2. The output must retain one row per date and delivery key.
SELECT
  COUNT(*) AS duplicate_key_groups
FROM (
  SELECT date, master_key
  FROM `looker-studio-pro-452620.repo_stg.basis_plus_utms_v4_PnS_table`
  WHERE REGEXP_CONTAINS(placement, r'MASSMUTUAL005CP_')
  GROUP BY date, master_key
  HAVING COUNT(*) > 1
)
LIMIT 1;

-- 3. Compare the deduplicated source metrics with the enriched output.
WITH expected_delivery AS (
  SELECT *
  FROM `looker-studio-pro-452620.repo_stg.basis_delivery`
  WHERE REGEXP_CONTAINS(placement, r'MASSMUTUAL005CP_')
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY date, del_key
    ORDER BY placement
  ) = 1
),
enriched AS (
  SELECT *
  FROM `looker-studio-pro-452620.repo_stg.basis_plus_utms_v4_PnS_table`
  WHERE REGEXP_CONTAINS(placement, r'MASSMUTUAL005CP_')
)
SELECT
  (SELECT COUNT(*) FROM expected_delivery) AS expected_record_count,
  (SELECT COUNT(*) FROM enriched) AS enriched_record_count,
  (SELECT SUM(impressions) FROM expected_delivery) AS expected_impressions,
  (SELECT SUM(impressions) FROM enriched) AS enriched_impressions,
  ROUND((SELECT SUM(media_cost) FROM expected_delivery), 2) AS expected_media_cost,
  ROUND((SELECT SUM(media_cost) FROM enriched), 2) AS enriched_media_cost,
  (SELECT SUM(clicks) FROM expected_delivery) AS expected_clicks,
  (SELECT SUM(clicks) FROM enriched) AS enriched_clicks
LIMIT 1;

-- 4. The report-level missing set must match the approved pre-refresh export.
WITH missing_keys AS (
  SELECT DISTINCT CONCAT(placement, ' -- ', creative_name) AS placement_name
  FROM `looker-studio-pro-452620.repo_stg.basis_plus_utms_v4_PnS_table`
  WHERE campaign = 'Massachusetts Mutual Connected Funnel FY26 - Q2/Q3'
    AND impressions > 10
    AND NULLIF(TRIM(utm_content), '') IS NULL
),
missing_summary AS (
  SELECT
    COUNT(*) AS missing_key_count,
    TO_HEX(
      SHA256(STRING_AGG(placement_name, '\n' ORDER BY placement_name))
    ) AS missing_key_set_sha256
  FROM missing_keys
)
SELECT
  missing_key_count,
  missing_key_set_sha256,
  missing_key_count = 117
    AND missing_key_set_sha256 = '9eafbdf6d846c3534e156936cbaa78d01e05513d5fdf06d113b62f88f818b427'
    AS matches_approved_pre_refresh_export
FROM missing_summary
LIMIT 1;
