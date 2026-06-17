-- ============================================================
-- mart_package_daily.sql
--
-- Story 5.2: Build Shortcut Marts — Package/Daily Mart
--
-- Derived from the universal compatibility view.
-- Package/daily grain — stable shortcut for dashboard reporting.
-- ============================================================

CREATE OR REPLACE VIEW `looker-studio-pro-452620.mdm_mart.mart_package_daily`
OPTIONS(
  description='Package/daily shortcut mart derived from the universal evidence table. Stable package/daily grain view for dashboard reporting. Story 5.2. Does not replace production master_stg objects.'
)
AS
SELECT
  _package_id,
  _date,
  _package_name,
  _package_name_friendly,
  _campaign_name,
  _advertiser,
  _channel,
  _channel_group,
  _media_name,
  _supplier_name,
  _placement_name,
  _planned_spend,
  _planned_impressions,
  _spend,
  _impressions,
  _clicks,
  _video_plays,
  _video_views,
  _video_comps,
  dcm_impressions,
  dcm_media_cost,
  dcm_clicks,
  fpd_impressions,
  fpd_spend,
  fpd_clicks,
  p_planned_amount_doNotSum,
  p_planned_impressions_doNotSum,
  initiative,
  qa_row_data_source_primary,
  qa_row_data_issue_category
FROM `looker-studio-pro-452620.mdm_int.int_universal_compat_view`;
