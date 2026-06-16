-- ============================================================
-- int_pilot_dcm_creative_detail.sql
--
-- Story 2.4: Prove A Pilot Lower-Grain Slice
--
-- Pilot DCM creative-grain detail view demonstrating that
-- the flexible-grain approach works with DCM delivery data
-- before onboarding every source.
--
-- Package totals are marked doNotSum on creative detail rows.
-- ============================================================

CREATE OR REPLACE VIEW `looker-studio-pro-452620.mdm_int.int_pilot_dcm_creative_detail`
OPTIONS(
  description='Pilot DCM creative-grain detail view. Proves the flexible-grain approach using DCM delivery data. Package totals are doNotSum on creative detail rows. Story 2.4. Does not replace production objects.'
)
AS
SELECT
  -- Universal row contract
  'creative_date' AS univ_row_grain,
  'master_stg.data_model (DCM delivery slice)' AS univ_source_system,
  FORMAT('%s|%s|%s', _package_id, SAFE_CAST(_date AS STRING), COALESCE(fpd_orig_creative, 'unknown')) AS univ_source_row_id,
  'master_stg.data_model > mdm_int.int_pilot_dcm_creative_detail' AS univ_source_lineage,
  CURRENT_DATE() AS univ_record_date,

  -- Legacy identity fields
  _package_id,
  _date,
  _campaign_name,
  _package_name,
  _package_name_friendly,

  -- Creative detail
  COALESCE(fpd_orig_creative, 'not_available_at_source') AS creative_name,
  COALESCE(fpd_creative_img, 'not_available_at_source') AS creative_image,

  -- Source metadata
  fpd_orig_source_files,
  fpd_orig_source_urls,

  -- ============================================================
  -- Additive metrics (safe to sum at creative/date grain)
  -- ============================================================
  dcm_daily_recalculated_cost AS creative_daily_cost,
  dcm_daily_recalculated_imps AS creative_daily_impressions,
  dcm_impressions AS creative_dcm_impressions,
  dcm_media_cost AS creative_dcm_media_cost,
  dcm_clicks AS creative_dcm_clicks,
  dcm_video_plays AS creative_dcm_video_plays,
  dcm_video_comps AS creative_dcm_video_comps,

  -- ============================================================
  -- doNotSum fields (repeated package totals — NOT additive here)
  -- ============================================================
  p_planned_amount_doNotSum AS pkg_planned_amount_doNotSum,
  p_planned_impressions_doNotSum AS pkg_planned_impressions_doNotSum,
  p_planned_units_doNotSum AS pkg_planned_units_doNotSum,
  _planned_spend AS pkg_planned_spend_doNotSum,
  _planned_impressions AS pkg_planned_impressions_filled_doNotSum,
  _spend AS pkg_actual_spend_doNotSum,
  _impressions AS pkg_actual_impressions_doNotSum,
  _clicks AS pkg_actual_clicks_doNotSum,

  -- Metadata
  dcm_min_date,
  dcm_max_date,
  dcm_min_flight_date,
  dcm_max_flight_date,
  dcm_daily_cpm

FROM `looker-studio-pro-452620.master_stg.data_model`
WHERE dcm_impressions IS NOT NULL
   OR dcm_media_cost IS NOT NULL;
