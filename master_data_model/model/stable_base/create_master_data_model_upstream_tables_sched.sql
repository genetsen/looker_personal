-- Build master data model upstream table siblings.
--
-- Purpose:
--   Refresh the stored upstream tables that the master data model should read
--   before `master_stg.data_model` is rebuilt or queried.
--
-- Outputs:
--   - `looker-studio-pro-452620.repo_int.crossplatform_pacing_tbl`
--   - `looker-studio-pro-452620.landing.tv_combined_tbl`
--
-- Safe usage:
--   This script intentionally leaves the same-name source views untouched:
--   `repo_int.crossplatform_pacing` and `landing.tv_combined` remain available
--   for lineage/debugging, while the `_tbl` siblings hold scheduled snapshots.

CREATE OR REPLACE TABLE `looker-studio-pro-452620.repo_int.crossplatform_pacing_tbl`
OPTIONS (
  description = "Stored master-model social pacing snapshot. Refreshed by the master upstream scheduled query from TikTok, Facebook, and Google Ads pacing logic; sibling view repo_int.crossplatform_pacing remains untouched."
) AS
WITH tt AS (
  SELECT
    SAFE_CAST(a_ad_id AS STRING) AS a_id,
    SAFE_CAST(a_ad_name AS STRING) AS ad_name,
    SAFE_CAST(ag_adgroup_id AS STRING) AS ag_id,
    ag_adgroup_name AS adgroup_name,
    ag_start_date,
    ag_end_date,
    ag_budget,
    SAFE_CAST(c_campaign_id AS STRING) AS c_id,
    c_campaign_name AS campaign_name,
    SAFE_CAST(NULL AS DATE) AS c_start_date,
    SAFE_CAST(NULL AS DATE) AS c_end_date,
    ag_start_date AS start_date,
    ag_end_date AS end_date,
    c_budget,
    CASE WHEN c_budget > 0 THEN c_budget ELSE ag_budget END AS final_budget,
    'tiktok' AS platform
  FROM `looker-studio-pro-452620.repo_tables.int__tiktok__combined_history_dedupe_view`
  WHERE CONTAINS_SUBSTR(c_campaign_name, '_gs_')
),

fb AS (
  SELECT
    SAFE_CAST(a_id AS STRING) AS a_id,
    a_name AS ad_name,
    SAFE_CAST(ag_id AS STRING) AS ag_id,
    ag_name AS adgroup_name,
    ag_start_date,
    ag_end_date,
    SAFE_DIVIDE(ag_lifetime_budget, 100) AS ag_budget,
    SAFE_CAST(c_id AS STRING) AS c_id,
    campaign_name,
    c_start_date,
    c_end_date,
    COALESCE(c_start_date, ag_start_date) AS start_date,
    COALESCE(c_end_date, ag_end_date) AS end_date,
    SAFE_DIVIDE(c_budget, 100) AS c_budget,
    CASE
      WHEN c_budget > 0 THEN SAFE_DIVIDE(c_budget, 100)
      ELSE SAFE_DIVIDE(ag_lifetime_budget, 100)
    END AS final_budget,
    'facebook' AS platform
  FROM `looker-studio-pro-452620.repo_facebook.stg__fb_combined_history`
),

ga AS (
  SELECT
    SAFE_CAST(a_id AS STRING) AS a_id,
    ad_name,
    SAFE_CAST(ag_id AS STRING) AS ag_id,
    adgroup_name,
    SAFE_CAST(NULL AS DATE) AS ag_start_date,
    SAFE_CAST(NULL AS DATE) AS ag_end_date,
    SAFE_CAST(NULL AS INT64) AS ag_budget,
    SAFE_CAST(c_id AS STRING) AS c_id,
    campaign_name,
    c_start_date,
    c_end_date,
    c_start_date AS start_date,
    c_end_date AS end_date,
    final_budget AS c_budget,
    final_budget AS final_budget,
    'google_ads' AS platform
  FROM `looker-studio-pro-452620.repo_google_ads.stg__ga_combined_history`
)
SELECT * FROM tt
UNION ALL
SELECT * FROM fb
UNION ALL
SELECT * FROM ga;

CREATE OR REPLACE TABLE `looker-studio-pro-452620.landing.tv_combined_tbl`
OPTIONS (
  description = "Stored master-model TV combined snapshot. Refreshed by the master upstream scheduled query from local and national TV estimate inputs; sibling view landing.tv_combined remains untouched."
) AS
WITH unioned AS (
  SELECT *
  FROM `looker-studio-pro-452620.landing.tv_local_estimates`

  UNION ALL

  SELECT *
  FROM `looker-studio-pro-452620.landing.tv_national_estimates`
)
SELECT *
FROM unioned
WHERE media_outlet IS NOT NULL;
