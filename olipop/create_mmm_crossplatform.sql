-- Build the OLIPOP weekly cross-platform reporting view.
--
-- Purpose:
--   Combine the existing weekly social summary with non-social OLIPOP rows
--   from the canonical master data model.
--
-- Safe modification:
--   Keep social output columns aligned with the master branch before changing
--   either source. The master branch uses the public QA field names.

CREATE OR REPLACE VIEW `looker-studio-pro-452620.Olipop.MMM_crossplatform` AS
WITH social AS (
  SELECT
    'social' AS data_source,
    '' AS package,
    'Olipop, Inc' AS Product,
    Tactic,
    date_week,
    SAFE_CAST(impressions AS FLOAT64) AS impressions,
    SAFE_CAST(clicks AS FLOAT64) AS clicks,
    SAFE_CAST(spend AS FLOAT64) AS spend
  FROM `looker-studio-pro-452620.repo_mart.olipop_MMM`
),
master_non_social AS (
  SELECT
    qa_media_data_type AS data_source,
    _package_name AS package,
    'Olipop, Inc' AS Product,
    _supplier_name AS Tactic,
    DATE_TRUNC(_date, WEEK(SUNDAY)) AS date_week,
    SUM(_impressions) AS impressions,
    SUM(_clicks) AS clicks,
    SUM(_spend) AS spend
  FROM `looker-studio-pro-452620.master_stg.data_model`
  WHERE UPPER(COALESCE(_advertiser_short_name, '')) LIKE '%OLI%'
    AND COALESCE(_campaign_name, '') LIKE '%26%'
    AND qa_media_data_type IN ('digital', 'tv', 'manual')
  GROUP BY 1, 2, 3, 4, 5
)
SELECT
  -- LIVE VIEW NOTE: OLIPOP weekly cross-platform output combining the existing
  -- social summary with non-social rows from the canonical master model.
  *
FROM social
UNION ALL
SELECT * FROM master_non_social;
