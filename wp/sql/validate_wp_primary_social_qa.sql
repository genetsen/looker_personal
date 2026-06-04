-- @description: Runs read-only QA checks for the WP-first shared-social
--               candidate and its reporting-shaped social impact view.
-- @sources:     repo_stg.stg__wp__search_data_template_daily_qa,
--               repo_stg.stg__crossplatform_wp_primary_qa,
--               master_stg.data_model_social_wp_primary_qa,
--               repo_stg.stg__olipop__crossplatform_raw_tbl.
-- @usage:       Run after creating both QA views. Review all result sets before
--               any production source or master-model replacement.

-- * CHECK [1]: WP INPUT CLASSIFICATION AND CREATIVE COVERAGE
SELECT
  platform,
  media_name,
  wp_classification_source,
  wp_publication_status,
  COUNT(*) AS row_count,
  SUM(spend) AS spend,
  SUM(impressions) AS impressions,
  SUM(clicks) AS clicks,
  SUM(video_view) AS video_views,
  COUNTIF(spend = 0) AS zero_spend_rows,
  COUNTIF(spend IS NULL) AS blank_spend_rows,
  COUNTIF(wp_creative_img IS NOT NULL) AS rows_with_creative_img
FROM `looker-studio-pro-452620.repo_stg.stg__wp__search_data_template_daily_qa`
GROUP BY 1,2,3,4
ORDER BY 1,2,3,4;

-- * CHECK [2]: NO DUPLICATE INCLUDED WP CAMPAIGN-GRAIN RECORD KEYS
SELECT
  platform,
  date_day,
  wp_row_key,
  COUNT(*) AS duplicate_rows
FROM `looker-studio-pro-452620.repo_stg.stg__wp__search_data_template_daily_qa`
WHERE wp_publication_status IN ('publish', 'publish_pending_source_owner_review')
GROUP BY 1,2,3
HAVING COUNT(*) > 1
ORDER BY duplicate_rows DESC, date_day DESC
LIMIT 20;

-- * CHECK [3]: PENDING SOURCE-OWNER ROWS ARE INCLUDED AND REMAIN VISIBLE
WITH pending_input AS (
  SELECT
    COUNT(*) AS row_count,
    SUM(spend) AS spend,
    SUM(impressions) AS impressions,
    SUM(clicks) AS clicks
  FROM `looker-studio-pro-452620.repo_stg.stg__wp__search_data_template_daily_qa`
  WHERE wp_publication_status = 'publish_pending_source_owner_review'
),
pending_candidate AS (
  SELECT
    COUNT(*) AS row_count,
    SUM(spend) AS spend,
    SUM(impressions) AS impressions,
    SUM(clicks) AS clicks
  FROM `looker-studio-pro-452620.repo_stg.stg__crossplatform_wp_primary_qa`
  WHERE wp_publication_status = 'publish_pending_source_owner_review'
)
SELECT
  'staging_pending_input' AS check_name,
  row_count,
  spend,
  impressions,
  clicks
FROM pending_input
UNION ALL
SELECT
  'merged_candidate_pending_included',
  row_count,
  spend,
  impressions,
  clicks
FROM pending_candidate;

-- * CHECK [4]: RECORD-SOURCE AND CHANNEL IMPACT IN THE MERGED CANDIDATE
SELECT
  `_advertiser`,
  `_media_name`,
  s_record_source,
  COUNT(*) AS row_count,
  SUM(`_spend`) AS spend,
  SUM(`_impressions`) AS impressions,
  SUM(`_clicks`) AS clicks,
  SUM(`_video_views`) AS video_views,
  COUNTIF(man_creative_img IS NOT NULL) AS rows_with_man_creative_img,
  COUNTIF(`_creative_img` IS NOT NULL) AS rows_with_canonical_creative_img
FROM `looker-studio-pro-452620.master_stg.data_model_social_wp_primary_qa`
WHERE `_advertiser` = 'Apollo'
GROUP BY 1,2,3
ORDER BY 2,3;

-- * CHECK [5]: HISTORICAL OVERLAP DIFFERENCES WHERE WP WINS
SELECT
  a.platform,
  a.date_day,
  a.ad_id,
  s.spend AS standard_spend,
  a.spend AS wp_spend,
  s.impressions AS standard_impressions,
  a.impressions AS wp_impressions,
  s.clicks AS standard_clicks,
  a.clicks AS wp_clicks
FROM `looker-studio-pro-452620.repo_stg.stg__wp__search_data_template_daily_qa` AS a
JOIN `looker-studio-pro-452620.repo_stg.stg__olipop__crossplatform_raw_tbl` AS s
  ON a.date_day = s.date_day
 AND LOWER(a.platform) = LOWER(s.platform)
 AND CAST(a.ad_id AS STRING) = CAST(s.ad_id AS STRING)
 AND LOWER(TRIM(COALESCE(a.campaign_name, ''))) = LOWER(TRIM(COALESCE(s.campaign_name, '')))
WHERE a.wp_publication_status IN ('publish', 'publish_pending_source_owner_review')
  AND (
    a.spend IS DISTINCT FROM s.spend
    OR a.impressions IS DISTINCT FROM s.impressions
    OR a.clicks IS DISTINCT FROM s.clicks
  )
ORDER BY a.date_day DESC, a.ad_id
LIMIT 100;

-- * CHECK [6]: NEW WP ROWS ABSENT FROM THE STANDARD SOURCE
SELECT
  a.platform,
  a.media_name,
  COUNT(*) AS new_row_count,
  SUM(a.spend) AS new_spend,
  SUM(a.impressions) AS new_impressions,
  SUM(a.clicks) AS new_clicks
FROM `looker-studio-pro-452620.repo_stg.stg__wp__search_data_template_daily_qa` AS a
LEFT JOIN `looker-studio-pro-452620.repo_stg.stg__olipop__crossplatform_raw_tbl` AS s
  ON a.date_day = s.date_day
 AND LOWER(a.platform) = LOWER(s.platform)
 AND CAST(a.ad_id AS STRING) = CAST(s.ad_id AS STRING)
 AND LOWER(TRIM(COALESCE(a.campaign_name, ''))) = LOWER(TRIM(COALESCE(s.campaign_name, '')))
WHERE a.wp_publication_status IN ('publish', 'publish_pending_source_owner_review')
  AND s.ad_id IS NULL
GROUP BY 1,2
ORDER BY 1,2;

-- * CHECK [7]: FILTERED DOWNSTREAM SCOPES MUST KEEP THE SAME TOTALS
WITH scoped_rows AS (
  SELECT
    'olipop_account_filter' AS check_name,
    'production' AS source_name,
    spend,
    impressions,
    clicks
  FROM `looker-studio-pro-452620.repo_stg.stg__olipop__crossplatform_raw_tbl`
  WHERE LOWER(account_name) LIKE '%olipop%'

  UNION ALL

  SELECT
    'olipop_account_filter',
    'qa_candidate',
    spend,
    impressions,
    clicks
  FROM `looker-studio-pro-452620.repo_stg.stg__crossplatform_wp_primary_qa`
  WHERE LOWER(account_name) LIKE '%olipop%'

  UNION ALL

  SELECT
    'adif_wp_campaign_filter',
    'production',
    spend,
    impressions,
    clicks
  FROM `looker-studio-pro-452620.repo_stg.stg__olipop__crossplatform_raw_tbl`
  WHERE TRIM(account_name) IN (
    'ADIF USA',
    'A Diamond is Forever - US',
    'A Diamond is Forever',
    'De Beers Group'
  )
    AND REGEXP_CONTAINS(IFNULL(campaign_name, ''), r'WP_')

  UNION ALL

  SELECT
    'adif_wp_campaign_filter',
    'qa_candidate',
    spend,
    impressions,
    clicks
  FROM `looker-studio-pro-452620.repo_stg.stg__crossplatform_wp_primary_qa`
  WHERE TRIM(account_name) IN (
    'ADIF USA',
    'A Diamond is Forever - US',
    'A Diamond is Forever',
    'De Beers Group'
  )
    AND REGEXP_CONTAINS(IFNULL(campaign_name, ''), r'WP_')
)
SELECT
  check_name,
  source_name,
  COUNT(*) AS row_count,
  SUM(spend) AS spend,
  SUM(impressions) AS impressions,
  SUM(clicks) AS clicks
FROM scoped_rows
GROUP BY 1,2
ORDER BY 1,2;
