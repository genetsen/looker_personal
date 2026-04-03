-- ============================================================================
-- ADIF Prisma Expanded Plus DCM - UNIFIED SQL SCRIPT FOR BIGQUERY
-- Complete reproduction with all custom dimensions, metrics, and parameters
-- ============================================================================

-- Define parameters (adjust these values as needed)
DECLARE data_type_param STRING DEFAULT 'estimated_delivery';
DECLARE dimension_2_param STRING DEFAULT 'Site';
DECLARE breakdown_dimension_param STRING DEFAULT 'Site';
DECLARE datatype_param STRING DEFAULT 'Delivered';

-- Main query
WITH source_data AS (
  SELECT
    package_id_joined,
    date,
    flight_status_flag,
    d_daily_recalculated_cost,
    d_daily_recalculated_imps,
    d_impressions,
    d_media_cost,
    d_clicks,
    d_video_plays,
    d_video_comps,
    d_min_date,
    d_max_date,
    d_prorated_planned_cost_pk,
    d_prorated_planned_imps_pk,
    d_min_flight_date,
    d_max_flight_date,
    d_daily_cpm,
    d_total_delivered_imps,
    d_total_del_inflight_imps,
    fpd_impressions,
    fpd_clicks,
    fpd_spend,
    fpd_sends,
    fpd_opens,
    fpd_benchmark,
    fpd_benchmark_metric,
    fpd_creative,
    planned_daily_spend_pk,
    planned_daily_impressions_pk,
    max_prismaEXP_report_date,
    final_spend,
    final_impressions,
    final_clicks,
    final_sends,
    final_opens,
    pkg_est_spend,
    pkg_act_spend,
    pkg_est_imps,
    pkg_act_imps,
    pkg_over_bool,
    pkg_over_flag,
    package_type,
    placement_type,
    placement_name,
    start_date,
    end_date,
    cost_method,
    buy_type,
    buy_category,
    click_through_url,
    advertiser_name,
    campaign_name,
    supplier_code,
    supplier_name,
    planned_actions,
    planned_amount,
    planned_clicks,
    planned_impressions,
    planned_units,
    unit_type,
    dimension,
    media_name,
    product_code,
    product_name,
    external_entity_id,
    provider_name,
    package_group_name,
    out_of_home_end_date,
    placement_cap_cost,
    served_by,
    payable_rate,
    channel_if_buy_category_custom_45,
    placement_type_site,
    device_type,
    audience_type,
    audience,
    initative,
    media_type,
    line_item_name,
    ad_size_asset,
    funnel,
    kpi,
    geo_market,
    ad_server_placement_id,
    placement_id,
    placement_comments,
    days_in_out_of_home_end_date,
    package_id,
    package_name,
    PlacementName,
    report_date,
    script_run_date,
    channel,
    channel_raw,
    channel_group,
    campaign_friendly,
    p_package_friendly,
    n_of_placements,
    planned_imps_pk,
    planned_cost_pk,
    total_days,
    daily_spend,
    daily_impressions,
    channel_any_pkg_over,
    site_any_pkg_over,
    gsMediaTeam_channel,
    final_table_refresh_date,
    initiative
  FROM `looker-studio-pro-452620.repo_stg.adif__prisma_expanded_plus_dcm_view_v3_test`
),

-- ============================================================================
-- CUSTOM DIMENSIONS (All prefixed fields from Looker Studio)
-- ============================================================================

with_custom_dimensions AS (
  SELECT
    *,
    
    -- Dimension: __CPM (CPM as display dimension)
    CAST(ROUND(
      CASE 
        WHEN final_impressions = 0 OR final_impressions IS NULL THEN NULL
        ELSE (final_spend / final_impressions) * 1000
      END, 2) AS STRING) AS __CPM,
    
    -- Dimension: _actual impressions
    CAST(COALESCE(final_impressions, d_impressions, fpd_impressions, 0) AS STRING) 
      AS _actual_impressions,
    
    -- Dimension: _Channel (Primary channel grouping)
    CASE
      WHEN channel IS NOT NULL THEN channel
      WHEN channel_group IS NOT NULL THEN channel_group
      WHEN channel_raw IS NOT NULL THEN channel_raw
      ELSE 'Unknown'
    END AS _Channel,
    
    -- Dimension: _channel_gs (GS Media Team channel mapping)
    CASE
      WHEN package_name IS NULL OR package_name = '' THEN NULL
      WHEN REGEXP_CONTAINS(package_name, r'(?i)(iHeart|SiriusXM|WeAreAOk|Wonder)') THEN 'Audio'
      WHEN REGEXP_CONTAINS(package_name, r'(?i)(Peacock|DISNED|ESPN|Hulu|Roku|HBO|Paramount|Tubi|YouTube|NBCU)') THEN 'Video'
      WHEN REGEXP_CONTAINS(package_name, r'(?i)(NBC|WBD|CBS|Playfly|Audience Express|Disney)') THEN 'Linear'
      WHEN REGEXP_CONTAINS(supplier_code, r'(?i)(PROJEX|Quan|QUAN)') THEN 'OOH Regional'
      WHEN REGEXP_CONTAINS(package_name, r'(?i)(PROJEX|Quan|QUAN)') THEN 'OOH Regional'
      WHEN REGEXP_CONTAINS(package_name, r'(?i)(ScreenVision|NCM)') THEN 'Cinema Regional'
      WHEN REGEXP_CONTAINS(package_name, r'(?i)(Vox|CondeNast|NYT|Meredi|TINYBE|NATVLY)') THEN 'Publisher Partnership'
      WHEN REGEXP_CONTAINS(package_name, r'(?i)People First') THEN 'Influencer'
      WHEN REGEXP_CONTAINS(package_name, r'(?i)People') THEN 'Publisher Partnership'
      WHEN REGEXP_CONTAINS(package_name, r'(?i)(Jeweler|JCK|Instore|Gem|AGS|Centurion|Zimnisky|KENIL|RELX)') THEN 'Trade'
      WHEN REGEXP_CONTAINS(package_name, r'(?i)MIQ') THEN 'Programmatic'
      WHEN REGEXP_CONTAINS(package_name, r'(?i)feeorder') THEN 'fee'
      WHEN REGEXP_CONTAINS(package_type, r'(?i)Print') THEN 'Publisher Partnership'
      ELSE 'Unmapped'
    END AS _channel_gs,
    
    -- Dimension: _estimated imps
    CAST(COALESCE(planned_daily_impressions_pk, planned_impressions, 0) AS STRING) 
      AS _estimated_imps,
    
    -- Dimension: _estimated spend
    CAST(COALESCE(planned_daily_spend_pk, planned_amount, 0) AS STRING) 
      AS _estimated_spend,
    
    -- Dimension: _Impressions (Total impressions display)
    CAST(COALESCE(final_impressions, d_impressions, fpd_impressions, 0) AS STRING) 
      AS _Impressions,
    
    -- Dimension: _Spend (Total spend display)
    CAST(COALESCE(final_spend, d_media_cost, fpd_spend, 0) AS STRING) 
      AS _Spend,
    
    -- Dimension: _spend_type (Source of spend)
    CASE
      WHEN fpd_spend IS NOT NULL AND fpd_spend > 0 THEN 'FPD'
      WHEN d_media_cost IS NOT NULL AND d_media_cost > 0 THEN 'DCM'
      WHEN final_spend IS NOT NULL AND final_spend > 0 THEN 'Final'
      ELSE 'Unknown'
    END AS _spend_type,
    
    -- Dimension: *actual spend
    CAST(COALESCE(final_spend, d_media_cost, fpd_spend, 0) AS STRING) 
      AS _actual_spend,
    
    -- Dimension: *Dimension 1 (Package grouping)
    CASE
      WHEN package_group_name IS NOT NULL THEN package_group_name
      WHEN p_package_friendly IS NOT NULL THEN p_package_friendly
      ELSE package_name
    END AS _Dimension_1,
    
    -- Dimension: *Dimension 2 (Media type)
    CASE
      WHEN media_type IS NOT NULL THEN media_type
      WHEN media_name IS NOT NULL THEN media_name
      WHEN placement_type IS NOT NULL THEN placement_type
      ELSE 'Unknown'
    END AS _Dimension_2,
    
    -- Dimension: *site (Site/Supplier identifier)
    CASE
      WHEN supplier_name IS NOT NULL THEN supplier_name
      WHEN supplier_code IS NOT NULL THEN supplier_code
      WHEN placement_name IS NOT NULL THEN placement_name
      ELSE 'Unknown'
    END AS _site,
    
    -- Additional dimensions
    CASE
      WHEN pkg_over_flag = 1 THEN 'Over'
      WHEN pkg_over_flag = 0 AND pkg_est_spend > 0 THEN 'Under/On'
      ELSE 'Unknown'
    END AS delivery_status,
    
    CASE
      WHEN flight_status_flag = 'Active' THEN 'Active'
      WHEN flight_status_flag = 'Completed' THEN 'Completed'
      WHEN flight_status_flag IS NULL THEN 'Untracked'
      ELSE flight_status_flag
    END AS tracking_flag
    
  FROM source_data
),

-- ============================================================================
-- CUSTOM METRICS (All calculated metrics from Looker Studio)
-- ============================================================================

with_custom_metrics AS (
  SELECT
    *,
    
    -- Metric 1: CPM (Cost Per Thousand Impressions)
    CASE 
      WHEN SUM(final_impressions) OVER (PARTITION BY package_id_joined, date) = 0 
        OR SUM(final_impressions) OVER (PARTITION BY package_id_joined, date) IS NULL
      THEN NULL
      ELSE ROUND((SUM(final_spend) OVER (PARTITION BY package_id_joined, date) / 
                  SUM(final_impressions) OVER (PARTITION BY package_id_joined, date)) * 1000, 2)
    END AS CPM,
    
    -- Metric 2: Pacing_imps (Impressions pacing - uses parameter)
    CASE 
      WHEN data_type_param = 'estimated_delivery' THEN
        CASE
          WHEN SUM(planned_daily_impressions_pk) OVER (PARTITION BY package_id_joined) IS NULL 
            OR SUM(planned_daily_impressions_pk) OVER (PARTITION BY package_id_joined) = 0
          THEN NULL
          ELSE ROUND((SAFE_DIVIDE(
                        SUM(fpd_impressions) OVER (PARTITION BY package_id_joined, date),
                        SUM(planned_daily_impressions_pk) OVER (PARTITION BY package_id_joined)
                      )) * 100, 2)
        END
      ELSE
        CASE 
          WHEN SUM(planned_daily_impressions_pk) OVER (PARTITION BY package_id_joined) IS NULL
            OR SUM(planned_daily_impressions_pk) OVER (PARTITION BY package_id_joined) = 0
          THEN NULL
          ELSE ROUND((SAFE_DIVIDE(
                        SUM(d_impressions) OVER (PARTITION BY package_id_joined, date) +
                        SUM(fpd_impressions) OVER (PARTITION BY package_id_joined, date),
                        SUM(planned_daily_impressions_pk) OVER (PARTITION BY package_id_joined)
                      )) * 100, 2)
        END
    END AS Pacing_imps,
    
    -- Metric 3: *est_pacing_imps (Estimated pacing impressions)
    CASE 
      WHEN SUM(planned_daily_impressions_pk) OVER (PARTITION BY package_id_joined) IS NULL 
        OR SUM(planned_daily_impressions_pk) OVER (PARTITION BY package_id_joined) = 0
      THEN NULL
      ELSE ROUND((SUM(fpd_impressions) OVER (PARTITION BY package_id_joined, date) / 
                  SUM(planned_daily_impressions_pk) OVER (PARTITION BY package_id_joined)) * 100, 2)
    END AS _est_pacing_imps,
    
    -- Metric 4: **est_spend_pacing (Estimated spend pacing)
    CASE 
      WHEN SUM(planned_daily_spend_pk) OVER (PARTITION BY package_id_joined) IS NULL 
        OR SUM(planned_daily_spend_pk) OVER (PARTITION BY package_id_joined) = 0
      THEN NULL
      ELSE ROUND((SUM(fpd_spend) OVER (PARTITION BY package_id_joined, date) / 
                  SUM(planned_daily_spend_pk) OVER (PARTITION BY package_id_joined)) * 100, 2)
    END AS _est_spend_pacing,
    
    -- Metric 5: *pacing_imps_act (Actual impressions pacing)
    CASE 
      WHEN SUM(planned_daily_impressions_pk) OVER (PARTITION BY package_id_joined) IS NULL 
        OR SUM(planned_daily_impressions_pk) OVER (PARTITION BY package_id_joined) = 0
      THEN NULL
      ELSE ROUND(((SUM(d_impressions) OVER (PARTITION BY package_id_joined, date) + 
                   SUM(fpd_impressions) OVER (PARTITION BY package_id_joined, date)) / 
                  SUM(planned_daily_impressions_pk) OVER (PARTITION BY package_id_joined)) * 100, 2)
    END AS _pacing_imps_act,
    
    -- Metric 6: *spend_pacing (Actual spend pacing)
    CASE 
      WHEN SUM(planned_daily_spend_pk) OVER (PARTITION BY package_id_joined) IS NULL 
        OR SUM(planned_daily_spend_pk) OVER (PARTITION BY package_id_joined) = 0
      THEN NULL
      ELSE ROUND(((SUM(d_media_cost) OVER (PARTITION BY package_id_joined, date) + 
                   SUM(fpd_spend) OVER (PARTITION BY package_id_joined, date)) / 
                  SUM(planned_daily_spend_pk) OVER (PARTITION BY package_id_joined)) * 100, 2)
    END AS _spend_pacing,
    
    -- Metric 7: Any_Package_Untracked (Packages with no delivery data)
    CASE 
      WHEN COUNT(DISTINCT date) OVER (PARTITION BY package_id_joined) > 0 
        AND SUM(CASE WHEN final_impressions IS NOT NULL AND final_impressions > 0 THEN 1 ELSE 0 END) 
            OVER (PARTITION BY package_id_joined) = 0
      THEN 1
      ELSE 0
    END AS Any_Package_Untracked,
    
    -- Metric 8: est_imps2 (Remaining impressions gap)
    CASE 
      WHEN (SUM(planned_daily_impressions_pk) OVER (PARTITION BY package_id_joined) - 
            COALESCE(SUM(final_impressions) OVER (PARTITION BY package_id_joined), 0)) IS NULL
      THEN 0
      ELSE ROUND(SUM(planned_daily_impressions_pk) OVER (PARTITION BY package_id_joined) - 
                 COALESCE(SUM(final_impressions) OVER (PARTITION BY package_id_joined), 0), 2)
    END AS est_imps2,
    
    -- Metric 9: Record Count (for aggregation)
    1 AS Record_Count
    
  FROM with_custom_dimensions
)

-- ============================================================================
-- FINAL OUTPUT - All Dimensions, Metrics, and Parameters
-- ============================================================================

SELECT
  -- Key identifiers
  package_id_joined,
  date,
  package_id,
  package_name,
  PlacementName,
  
  -- Custom Dimensions (Prefixed)
  __CPM,
  _actual_impressions,
  _Channel,
  _channel_gs,
  _estimated_imps,
  _estimated_spend,
  _Impressions,
  _Spend,
  _spend_type,
  _actual_spend,
  _Dimension_1,
  _Dimension_2,
  _site,
  tracking_flag,
  delivery_status,
  
  -- All source dimensions
  flight_status_flag,
  package_type,
  placement_type,
  placement_name,
  start_date,
  end_date,
  cost_method,
  buy_type,
  buy_category,
  click_through_url,
  advertiser_name,
  campaign_name,
  supplier_code,
  supplier_name,
  planned_actions,
  planned_amount,
  planned_clicks,
  planned_impressions,
  planned_units,
  unit_type,
  dimension,
  media_name,
  product_code,
  product_name,
  external_entity_id,
  provider_name,
  package_group_name,
  out_of_home_end_date,
  placement_cap_cost,
  served_by,
  payable_rate,
  channel_if_buy_category_custom_45,
  placement_type_site,
  device_type,
  audience_type,
  audience,
  initative,
  media_type,
  line_item_name,
  ad_size_asset,
  funnel,
  kpi,
  geo_market,
  ad_server_placement_id,
  placement_id,
  placement_comments,
  days_in_out_of_home_end_date,
  report_date,
  script_run_date,
  channel,
  channel_raw,
  channel_group,
  campaign_friendly,
  p_package_friendly,
  n_of_placements,
  planned_imps_pk,
  planned_cost_pk,
  total_days,
  daily_spend,
  daily_impressions,
  channel_any_pkg_over,
  site_any_pkg_over,
  gsMediaTeam_channel,
  final_table_refresh_date,
  initiative,
  
  -- Source metrics (DCM)
  d_daily_recalculated_cost,
  d_daily_recalculated_imps,
  d_impressions,
  d_media_cost,
  d_clicks,
  d_video_plays,
  d_video_comps,
  d_min_date,
  d_max_date,
  d_prorated_planned_cost_pk,
  d_prorated_planned_imps_pk,
  d_min_flight_date,
  d_max_flight_date,
  d_daily_cpm,
  d_total_delivered_imps,
  d_total_del_inflight_imps,
  
  -- Source metrics (FPD)
  fpd_impressions,
  fpd_clicks,
  fpd_spend,
  fpd_sends,
  fpd_opens,
  fpd_benchmark,
  fpd_benchmark_metric,
  fpd_creative,
  
  -- Source metrics (Prisma)
  planned_daily_spend_pk,
  planned_daily_impressions_pk,
  max_prismaEXP_report_date,
  
  -- Coalesced metrics
  final_spend,
  final_impressions,
  final_clicks,
  final_sends,
  final_opens,
  pkg_est_spend,
  pkg_act_spend,
  pkg_est_imps,
  pkg_act_imps,
  pkg_over_bool,
  pkg_over_flag,
  
  -- Custom Calculated Metrics (Prefixed)
  CPM,
  Pacing_imps,
  _est_pacing_imps,
  _est_spend_pacing,
  _pacing_imps_act,
  _spend_pacing,
  Any_Package_Untracked,
  est_imps2,
  Record_Count,
  
  -- Parameters (for reference in output)
  data_type_param AS param_data_type,
  dimension_2_param AS param_dimension_2,
  breakdown_dimension_param AS param_breakdown_dimension,
  datatype_param AS param_datatype
  
FROM with_custom_metrics
ORDER BY package_id_joined, date;
