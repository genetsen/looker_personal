-- Purpose: model the scheduled MFT current-table build with the approved,
-- narrow Basis Partner correction before the live scheduled query is updated.
-- Reads: repo_mart.mft_view. Produces: one current-MFT row per reporting grain.
-- Safe use: read-only candidate query for SQL Change Guard; it does not write tables.
-- Business rule: only Basis rows currently labeled S or UpperFunnel are relabeled
-- from the exact publisher portion of PMP(...). All other Partner labels stay unchanged.

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
