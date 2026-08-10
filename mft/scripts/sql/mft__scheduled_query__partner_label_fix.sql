-- Purpose: rebuild the MFT current and final reporting tables with the approved,
-- narrow Basis Partner correction.
-- Reads: repo_mart.mft_view and mass_mutual_mft_ext.mft_data_pre_2025.
-- Replaces: mass_mutual_mft_ext.mft_data_current and mass_mutual_mft_ext.mft_data.
-- Business rule: only Basis rows currently labeled S or UpperFunnel use PMP(...)
-- as Partner. All other existing Partner labels remain unchanged.

CREATE OR REPLACE TABLE `looker-studio-pro-452620.mass_mutual_mft_ext.mft_data_current` AS
SELECT
  date,
  campaign,
  CASE
    WHEN utm_source = 'basis'
      AND COALESCE(REGEXP_EXTRACT(placement_name, r'\(\s*([A-Za-z]+)'), utm_source) IN ('S', 'UpperFunnel')
      THEN COALESCE(
        NULLIF(REGEXP_EXTRACT(placement_name, r'_PMP\(([^)]*)\)'), ''),
        COALESCE(REGEXP_EXTRACT(placement_name, r'\(\s*([A-Za-z]+)'), utm_source)
      )
    ELSE COALESCE(REGEXP_EXTRACT(placement_name, r'\(\s*([A-Za-z]+)'), utm_source)
  END AS partner,
  placement_name,
  utm_source,
  utm_medium,
  utm_campaign,
  utm_content,
  utm_term,
  SUM(cost) AS Cost,
  SUM(impressions) AS Impressions,
  SUM(clicks) AS Clicks,
  SUM(video_audio_plays) AS video_audio_plays,
  SUM(video_audio_fully_played) AS video_audio_fully_played
FROM `looker-studio-pro-452620.repo_mart.mft_view`
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9
ORDER BY date;

CREATE OR REPLACE TABLE `looker-studio-pro-452620.mass_mutual_mft_ext.mft_data` AS
WITH current_rows AS (
  SELECT *
  FROM `looker-studio-pro-452620.mass_mutual_mft_ext.mft_data_current`
),
historical_rows AS (
  SELECT
    *,
    NULL AS video_audio_plays,
    NULL AS video_audio_fully_played
  FROM `looker-studio-pro-452620.mass_mutual_mft_ext.mft_data_pre_2025`
),
unioned AS (
  SELECT * FROM current_rows
  UNION ALL
  SELECT * FROM historical_rows
)
SELECT
  *,
  NULL AS destination_url
FROM unioned
ORDER BY date;
