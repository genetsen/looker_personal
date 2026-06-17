-- ============================================================
-- mart_creative_daily.sql
--
-- Story 5.2: Build Shortcut Marts — Creative/Daily Mart
-- ============================================================

CREATE OR REPLACE VIEW `looker-studio-pro-452620.mdm_mart.mart_creative_daily`
OPTIONS(
  description='Creative/daily shortcut mart derived from the universal evidence table. Used for creative-level dashboard reporting. Story 5.2. Does not replace production objects.'
)
AS
SELECT
  creative_name,
  creative_image,
  _package_id,
  _date,
  _package_name,
  _campaign_name,
  creative_daily_cost,
  creative_daily_impressions,
  creative_dcm_impressions,
  creative_dcm_media_cost,
  creative_dcm_clicks,
  pkg_planned_amount_doNotSum,
  pkg_planned_impressions_doNotSum,
  pkg_actual_spend_doNotSum,
  pkg_actual_impressions_doNotSum
FROM `looker-studio-pro-452620.mdm_int.int_pilot_dcm_creative_detail`;

-- ============================================================
-- mart_bi_stable.sql
--
-- Story 5.1/5.2: Stable BI-facing view — published output
-- ============================================================
CREATE OR REPLACE VIEW `looker-studio-pro-452620.mdm_publish.v_master_evidence`
OPTIONS(
  description='Stable BI-facing view for the master data model redesign. Published output contract object — source-of-truth view for dashboards. Story 5.1/5.2. Does not replace production master_stg objects.'
)
AS
SELECT
  univ_row_grain,
  univ_source_system,
  univ_source_row_id,
  univ_source_lineage,
  univ_record_date,

  -- Legacy identity columns
  _package_id,
  _date,
  _campaign_name,
  _package_name_friendly,
  _advertiser,
  _channel,
  _channel_group,
  _media_name,

  -- Placeholder dimensions
  univ_creative_placeholder,
  univ_dma_placeholder,
  univ_placement_placeholder,
  univ_campaign_placeholder,
  univ_advertiser_placeholder,
  univ_supplier_placeholder,
  univ_date_placeholder,

  -- Safely addable metrics
  univ_spend_safe,
  univ_impressions_safe,
  univ_clicks_safe,

  -- doNotSum metrics
  univ_planned_spend_safe,
  univ_planned_impressions_safe,

  -- Metric status / summability
  univ_spend_value_status,
  univ_spend_summability,
  univ_impressions_value_status,
  univ_impressions_summability,
  univ_clicks_value_status,
  univ_clicks_summability,
  univ_planned_spend_value_status,
  univ_planned_spend_summability,
  univ_planned_impressions_value_status,
  univ_planned_impressions_summability,

  -- Source lineage & audit
  univ_flight_dates_source,
  univ_flight_dates_reason,
  univ_planned_spend_source,
  univ_planned_impressions_source

FROM `looker-studio-pro-452620.mdm_int.int_metric_status`;
