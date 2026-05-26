-- @description: Runs read-only QA checks for the APO-first shared-social
--               candidate and its reporting-shaped social impact view.
-- @sources:     repo_stg.stg__apo__search_data_template_daily_qa,
--               repo_stg.stg__crossplatform_apo_primary_qa,
--               master_stg.data_model_social_apo_primary_qa,
--               repo_stg.stg__olipop__crossplatform_raw_tbl.
-- @usage:       Run after creating both QA views. Review all result sets before
--               any production source or master-model replacement.

-- * CHECK [1]: APO INPUT CLASSIFICATION AND CREATIVE COVERAGE
SELECT
  platform,
  media_name,
  apo_classification_source,
  apo_publication_status,
  COUNT(*) AS row_count,
  SUM(spend) AS spend,
  SUM(impressions) AS impressions,
  SUM(clicks) AS clicks,
  COUNTIF(spend = 0) AS zero_spend_rows,
  COUNTIF(spend IS NULL) AS blank_spend_rows,
  COUNTIF(NULLIF(apo_creative_box_link, '-') IS NOT NULL) AS rows_with_creative_link
FROM `looker-studio-pro-452620.repo_stg.stg__apo__search_data_template_daily_qa`
GROUP BY 1,2,3,4
ORDER BY 1,2,3,4;

-- * CHECK [2]: NO DUPLICATE PUBLISHABLE APO DAILY-AD KEYS
SELECT
  platform,
  date_day,
  ad_id,
  COUNT(*) AS duplicate_rows
FROM `looker-studio-pro-452620.repo_stg.stg__apo__search_data_template_daily_qa`
WHERE apo_publication_status = 'publish'
GROUP BY 1,2,3
HAVING COUNT(*) > 1
ORDER BY duplicate_rows DESC, date_day DESC
LIMIT 20;

-- * CHECK [3]: HELD-OUT CONFLICTING SOURCE KEYS REMAIN VISIBLE FOR DECISION
SELECT
  platform,
  media_name,
  COUNT(*) AS held_out_row_count,
  COUNT(DISTINCT apo_row_key) AS held_out_key_count,
  SUM(spend) AS held_out_spend,
  SUM(impressions) AS held_out_impressions,
  SUM(clicks) AS held_out_clicks
FROM `looker-studio-pro-452620.repo_stg.stg__apo__search_data_template_daily_qa`
WHERE apo_publication_status = 'exclude_duplicate_daily_ad_key'
GROUP BY 1,2
ORDER BY 1,2;

-- * CHECK [4]: RECORD-SOURCE AND CHANNEL IMPACT IN THE MERGED CANDIDATE
SELECT
  `_advertiser`,
  `_media_name`,
  s_record_source,
  COUNT(*) AS row_count,
  SUM(`_spend`) AS spend,
  SUM(`_impressions`) AS impressions,
  SUM(`_clicks`) AS clicks,
  COUNTIF(s_creative_box_link IS NOT NULL) AS rows_with_creative_link
FROM `looker-studio-pro-452620.master_stg.data_model_social_apo_primary_qa`
WHERE `_advertiser` = 'Apollo'
GROUP BY 1,2,3
ORDER BY 2,3;

-- * CHECK [5]: HISTORICAL OVERLAP DIFFERENCES WHERE APO WINS
SELECT
  a.platform,
  a.date_day,
  a.ad_id,
  s.spend AS standard_spend,
  a.spend AS apo_spend,
  s.impressions AS standard_impressions,
  a.impressions AS apo_impressions,
  s.clicks AS standard_clicks,
  a.clicks AS apo_clicks
FROM `looker-studio-pro-452620.repo_stg.stg__apo__search_data_template_daily_qa` AS a
JOIN `looker-studio-pro-452620.repo_stg.stg__olipop__crossplatform_raw_tbl` AS s
  ON a.date_day = s.date_day
 AND LOWER(a.platform) = LOWER(s.platform)
 AND CAST(a.ad_id AS STRING) = CAST(s.ad_id AS STRING)
WHERE a.apo_publication_status = 'publish'
  AND (
    a.spend IS DISTINCT FROM s.spend
    OR a.impressions IS DISTINCT FROM s.impressions
    OR a.clicks IS DISTINCT FROM s.clicks
  )
ORDER BY a.date_day DESC, a.ad_id
LIMIT 100;

-- * CHECK [6]: NEW APO ROWS ABSENT FROM THE STANDARD SOURCE
SELECT
  a.platform,
  a.media_name,
  COUNT(*) AS new_row_count,
  SUM(a.spend) AS new_spend,
  SUM(a.impressions) AS new_impressions,
  SUM(a.clicks) AS new_clicks
FROM `looker-studio-pro-452620.repo_stg.stg__apo__search_data_template_daily_qa` AS a
LEFT JOIN `looker-studio-pro-452620.repo_stg.stg__olipop__crossplatform_raw_tbl` AS s
  ON a.date_day = s.date_day
 AND LOWER(a.platform) = LOWER(s.platform)
 AND CAST(a.ad_id AS STRING) = CAST(s.ad_id AS STRING)
WHERE a.apo_publication_status = 'publish'
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
  FROM `looker-studio-pro-452620.repo_stg.stg__crossplatform_apo_primary_qa`
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
  FROM `looker-studio-pro-452620.repo_stg.stg__crossplatform_apo_primary_qa`
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
