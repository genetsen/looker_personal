-- ────────────────────────────────────────────────────────────────────────────────
-- @title:        Cross-Platform Pacing
-- @description:  Combines TikTok, Facebook, and Google Ads unified history models 
--                into a single cross-platform pacing dataset. Ensures consistent 
--                fields (ad, ad group, campaign, budget, dates).
--
-- @author:       [Your Name]
-- @last_updated: [YYYY-MM-DD]
-- @target:       repo_stg.crossplatform_pacing
-- @notes:
--   - TikTok reads both the standard and ADIF connectors.
--   - TikTok campaign names must contain "_gs_" or start with "WP_".
--   - Final budget is derived from campaign budget where possible, 
--     otherwise ad group lifetime budget.
--   - Dynamic daily campaign budgets are converted to an equivalent flight
--     total so existing downstream daily pacing math remains unchanged.
-- ────────────────────────────────────────────────────────────────────────────────

CREATE OR REPLACE VIEW `looker-studio-pro-452620.repo_int.crossplatform_pacing` AS

-- ============================================================================
-- TikTok Subset: Combined history with campaign name filter
-- ============================================================================
WITH tt_standard AS (
  SELECT 
    safe_cast(a_ad_id as string) as a_id,
    safe_cast(a_ad_name as string)                      AS ad_name,
    safe_cast(ag_adgroup_id as string)                  AS ag_id,
    ag_adgroup_name                AS adgroup_name,
    ag_start_date,
    ag_end_date,
    ag_budget,
    safe_cast(c_campaign_id as string)                  AS c_id,
    c_campaign_name                AS campaign_name,
    SAFE_CAST(NULL AS DATE)        AS c_start_date,
    SAFE_CAST(NULL AS DATE)        AS c_end_date,
    ag_start_date                  AS start_date,
    ag_end_date                    AS end_date,
    c_budget,
    CASE WHEN c_budget > 0 THEN c_budget ELSE ag_budget END AS final_budget,
    'tiktok' AS platform
  FROM `looker-studio-pro-452620.repo_tables.int__tiktok__combined_history_dedupe_view`
  WHERE REGEXP_CONTAINS(LOWER(c_campaign_name), r'(_gs_|^wp_)')
),

-- Smart+ ads are stored as creative rows in the separate ADIF connector.
-- Keep the creative ID as a_id because delivery and pacing join at that grain.
tt_adif_creative AS (
  SELECT * EXCEPT(row_num)
  FROM (
    SELECT
      creative_id,
      creative_name,
      adgroup_id,
      campaign_id,
      ROW_NUMBER() OVER (
        PARTITION BY creative_id
        ORDER BY updated_at DESC, _fivetran_synced DESC
      ) AS row_num
    FROM `giant-spoon-299605.tiktok_ads_adif.creative_history`
  )
  WHERE row_num = 1
),

tt_adif_adgroup AS (
  SELECT * EXCEPT(row_num)
  FROM (
    SELECT
      adgroup_id,
      adgroup_name,
      CAST(schedule_start_time AS DATE) AS start_date,
      CAST(schedule_end_time AS DATE) AS raw_end_date,
      budget,
      budget_mode,
      operation_status,
      ROW_NUMBER() OVER (
        PARTITION BY adgroup_id
        ORDER BY updated_at DESC, _fivetran_synced DESC
      ) AS row_num
    FROM `giant-spoon-299605.tiktok_ads_adif.adgroup_history`
  )
  WHERE row_num = 1
),

tt_adif_campaign AS (
  SELECT * EXCEPT(row_num)
  FROM (
    SELECT
      campaign_id,
      campaign_name,
      budget,
      budget_mode,
      operation_status,
      ROW_NUMBER() OVER (
        PARTITION BY campaign_id
        ORDER BY updated_at DESC, _fivetran_synced DESC
      ) AS row_num
    FROM `giant-spoon-299605.tiktok_ads_adif.campaign_history`
  )
  WHERE row_num = 1
),

tt_adif_normalized AS (
  SELECT
    c.*,
    ag.adgroup_name,
    ag.start_date,
    CASE
      WHEN ag.operation_status = 'ENABLE'
        AND cp.operation_status = 'ENABLE'
        AND ag.raw_end_date >= DATE_ADD(ag.start_date, INTERVAL 365 DAY)
        THEN CURRENT_DATE()
      ELSE ag.raw_end_date
    END AS effective_end_date,
    ag.budget AS adgroup_budget,
    ag.budget_mode AS adgroup_budget_mode,
    cp.campaign_name,
    cp.budget AS campaign_budget,
    cp.budget_mode AS campaign_budget_mode
  FROM tt_adif_creative AS c
  JOIN tt_adif_adgroup AS ag USING (adgroup_id)
  JOIN tt_adif_campaign AS cp USING (campaign_id)
  WHERE REGEXP_CONTAINS(LOWER(cp.campaign_name), r'(_gs_|^wp_)')
),

tt_adif AS (
  SELECT
    CAST(creative_id AS STRING) AS a_id,
    CAST(creative_name AS STRING) AS ad_name,
    CAST(adgroup_id AS STRING) AS ag_id,
    adgroup_name,
    start_date AS ag_start_date,
    effective_end_date AS ag_end_date,
    adgroup_budget AS ag_budget,
    CAST(campaign_id AS STRING) AS c_id,
    campaign_name,
    CAST(NULL AS DATE) AS c_start_date,
    CAST(NULL AS DATE) AS c_end_date,
    start_date,
    effective_end_date AS end_date,
    campaign_budget AS c_budget,
    CASE
      WHEN campaign_budget_mode = 'BUDGET_MODE_DYNAMIC_DAILY_BUDGET'
        AND campaign_budget > 0
        THEN campaign_budget * (DATE_DIFF(effective_end_date, start_date, DAY) + 1)
      WHEN campaign_budget > 0 THEN campaign_budget
      ELSE adgroup_budget
    END AS final_budget,
    'tiktok' AS platform
  FROM tt_adif_normalized
  WHERE start_date IS NOT NULL
    AND effective_end_date >= start_date
),

tt AS (
  SELECT * FROM tt_standard
  UNION ALL
  SELECT * FROM tt_adif
),

-- ============================================================================
-- Facebook Subset: Combined history with budget fallback logic
-- ============================================================================
fb AS (
  SELECT 
    safe_cast(a_id as string) as a_id,
    a_name                         AS ad_name, 
    safe_cast(ag_id as string) as ag_id, 
    ag_name                        AS adgroup_name,
    ag_start_date,
    ag_end_date,
    safe_divide(ag_lifetime_budget,100)             AS ag_budget,
    safe_cast(c_id as string) as c_id,
    campaign_name,
    c_start_date,
    c_end_date,
    COALESCE(c_start_date, ag_start_date) AS start_date,
    COALESCE(c_end_date,   ag_end_date)   AS end_date,
    safe_divide(c_budget,100)            as c_budget ,
    CASE WHEN c_budget > 0 THEN safe_divide(c_budget,100)  ELSE safe_divide(ag_lifetime_budget,100)  END AS final_budget,
    'facebook' AS platform 
  FROM `looker-studio-pro-452620.repo_facebook.stg__fb_combined_history`
),

-- ============================================================================
-- Google Ads Subset: Combined history standardized to pacing schema
-- ============================================================================
ga AS (
  SELECT
    safe_cast(a_id as string) as a_id,
    ad_name,
    safe_cast(ag_id as string) as ag_id,
    adgroup_name,
    SAFE_CAST(NULL AS DATE)        AS ag_start_date,
    SAFE_CAST(NULL AS DATE)        AS ag_end_date,
    SAFE_CAST(NULL AS INT64)       AS ag_budget,
    safe_cast(c_id as string) as c_id,
    campaign_name,
    c_start_date,
    c_end_date,
    c_start_date                   AS start_date,
    c_end_date                     AS end_date,
    final_budget                   AS c_budget,
    final_budget                   AS final_budget,
    'google_ads' AS platform
  FROM `looker-studio-pro-452620.repo_google_ads.stg__ga_combined_history`
)

-- ============================================================================
-- Final Union: Consolidate TikTok, Facebook, Google Ads into pacing table
-- ============================================================================
SELECT * FROM tt
UNION ALL
SELECT * FROM fb
UNION ALL
SELECT * FROM ga;
