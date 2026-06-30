-- ============================================================
-- int_universal_compat_view.sql
--
-- Compatibility-first candidate view that mirrors every column
-- from master_stg.data_model while adding prefixed universal
-- fields (univ_row_grain, univ_source_system, etc.).
--
-- This view is the foundation for Epics 2.1 and 2.2:
--   - 2.1: Universal row contract (univ_ prefixed fields)
--   - 2.2: Every legacy column preserved
--   - 2.3: Explicit placeholder semantics
-- ============================================================

CREATE OR REPLACE VIEW `looker-studio-pro-452620.mdm_int.int_universal_compat_view`
OPTIONS(
  description='Compatibility-first candidate universal view for master data model redesign. Mirrors every master_stg.data_model column while adding prefixed univ_* universal fields, placeholder semantics, and source lineage. Built by int_universal_compat_view.sql. Does NOT replace any production object.'
)
AS

-- ============================================================
-- Step 1: Row identity and universal contract fields
-- ============================================================
WITH
base AS (
  SELECT
    -- Universal row contract (Story 2.1)
    'package_date' AS univ_row_grain,
    'master_stg.data_model' AS univ_source_system,
    FORMAT('%s|%s', _package_id, SAFE_CAST(_date AS STRING)) AS univ_source_row_id,
    'master_stg.data_model > mdm_int.int_universal_compat_view' AS univ_source_lineage,
    CURRENT_DATE() AS univ_record_date,

    -- Legacy QA prefix fields
    qa_media_data_type,
    qa_row_data_source_primary,
    qa_row_data_sources_available,
    qa_data_issues,

    -- Core identity fields
    _package_id,
    _date,
    _start_date,
    _end_date,

    -- Advertiser fields
    _advertiser_name,
    _advertiser_short_name,
    _advertiser,

    -- Campaign fields
    _campaign_name,
    _campaign_friendly,
    _product_code,
    _product_name,

    -- Package fields
    _package_type,
    _package_name,
    _package_name_friendly,
    ADIF_channel,

    -- Placement fields
    _placement_id,
    _placement_name,

    -- Supplier fields
    _supplier_code,
    _supplier_name,
    _supplier_logo,

    -- Buy type fields
    p_buy_type,
    p_buy_category,

    -- Channel fields
    _channel,
    qa_channel_raw,
    _channel_group,
    _media_name,

    -- Cost method
    p_cost_method,

    -- Planned / doNotSum fields
    p_planned_amount_doNotSum,
    p_planned_impressions_doNotSum,
    p_planned_units_doNotSum,
    p_unit_type,
    p_rate,
    _planned_spend,
    _planned_impressions,
    p_planned_clicks,
    p_max_report_date,

    -- DCM delivery fields
    dcm_daily_recalculated_cost,
    dcm_daily_recalculated_imps,
    dcm_impressions,
    dcm_media_cost,
    dcm_clicks,
    dcm_video_plays,
    dcm_video_comps,
    dcm_min_date,
    dcm_max_date,
    dcm_min_flight_date,
    dcm_max_flight_date,
    dcm_daily_cpm,
    dcm_total_delivered_imps,
    dcm_total_del_inflight_imps,

    -- Consolidated FPD fields
    fpd_impressions,
    fpd_spend,
    fpd_clicks,
    fpd_sends,
    fpd_opens,
    fpd_benchmark,
    fpd_benchmark_metric,
    fpd_factor,
    fpd_creative,
    fpd_creative_img,
    _creative_img,
    man_creative_img,
    fpd_source_name,
    fpd_source_url,
    fpd_source_content_modified_at,
    fpd_data_timestamp,

    -- Final universal metrics
    _spend,
    _impressions,
    _clicks,
    _video_plays,
    _video_views,
    _video_comps,

    -- TV fields
    tv_data_refresh_date,
    tv_media_outlet,
    tv_type,
    tv_program_name,
    tv_market,
    tv_quarter,
    tv_year,
    tv_net_impressions,
    tv_net_cost,
    tv_total_units,

    -- Social fields
    s_platform,
    s_campaign_id,
    s_ad_group_id,
    s_ad_id,
    s_account_name,
    s_pacing_planned_spend,
    s_spend,
    s_impressions,
    s_clicks,
    s_video_plays,
    s_video_views,
    s_video_comps,
    s_creative_name,
    s_channel_classification_source,
    s_publication_status,
    s_source_sheet_url,
    s_loaded_at,
    s_wp_row_key,
    s_record_source,
    s_fallback_fields,

    -- Amazon fields
    amzn_campaign_id,
    amzn_campaign_name,
    amzn_campaign_budget_amount,
    amzn_campaign_start_date,
    amzn_campaign_end_date,
    amzn_campaign_bid_strategy,
    amzn_campaign_cost_type,
    amzn_date,
    amzn_advertiser_account_id,
    amzn_advertiser_account_name,
    amzn_budget_currency,
    amzn_portfolio_id,
    amzn_portfolio_name,
    amzn_ad_group_id,
    amzn_ad_group_name,
    amzn_ad_group_start_date,
    amzn_ad_group_end_date,
    amzn_ad_group_cost_type,
    amzn_ad_group_budget_amount,
    amzn_ad_id,
    amzn_ad_name,
    amzn_ad_format,
    amzn_deal_id,
    amzn_deal_name,
    amzn_impressions,
    amzn_clicks,
    amzn_purchases_combined,
    amzn_starts_video_ad,
    amzn_complete_views_video_ad,
    amzn_impressions_video_ad,
    amzn_purchases,
    amzn_sales,
    amzn_supply_cost,
    amzn_units_sold,
    amzn_branded_searches,
    amzn_sales_combined,
    amzn_units_sold_combined,
    amzn_report_generated_date,
    amzn_source_email_timestamp,
    amzn_source_email_id,
    amzn_source_filename,
    amzn_source_file_size_bytes,
    amzn_loaded_at,
    amzn_row_hash,

    -- Manual edit fields
    man_edit_id,
    man_edit_reason,
    man_editor_email,
    man_source_sheet_url,
    man_source_sheet_modified_time,
    man_loaded_at,
    man_start_date,
    man_end_date,
    man_daily_spend,
    man_daily_impressions,
    man_daily_planned_spend,
    man_daily_planned_impressions,
    man_daily_clicks,
    man_daily_video_plays,
    man_daily_video_comps,
    man_total_spend_doNotSum,
    man_total_impressions_doNotSum,
    man_total_planned_spend_doNotSum,
    man_total_planned_impressions_doNotSum,
    man_total_clicks_doNotSum,
    man_total_video_plays_doNotSum,
    man_total_video_comps_doNotSum,

    -- QA package fields
    qa_pkg_est_spend_doNotSum,
    qa_pkg_est_impressions_doNotSum,
    qa_pkg_act_spend_doNotSum,
    qa_pkg_act_impressions_doNotSum,
    qa_pkg_act_clicks_doNotSum,
    qa_pkg_fpd_impressions_doNotSum,
    qa_pkg_fpd_spend_doNotSum,

    -- QA row metadata
    qa_package_spend_over_plan_flag,
    initiative

  FROM `looker-studio-pro-452620.master_stg.data_model`
)

-- Add placeholder semantics (Story 2.3)
-- Unavailable lower-grain dimensions use clear placeholders
SELECT
  -- LIVE VIEW NOTE: Universal compatibility view over the canonical master
  -- model, preserving the public schema and adding universal placeholders.
  base.*,

  -- Placeholder dimension fields
  CASE
    WHEN _placement_id IS NULL THEN 'not_available_at_source'
    ELSE _placement_id
  END AS uni_placement_id_placeholder,

  CASE
    WHEN dcm_min_date IS NULL AND _date IS NOT NULL THEN 'unknown_from_source'
    ELSE SAFE_CAST(dcm_min_date AS STRING)
  END AS uni_placement_available

FROM base;
