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
  -- LIVE VIEW NOTE: Creative/day reporting shortcut derived from the pilot
  -- creative evidence view; package totals remain non-summable context.
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
-- v_master_evidence
--
-- Story 5.1/5.2: Stable BI-facing view - published output.
--
-- Important correction:
-- The published final view must remain one row stream. Do not join
-- sibling package/date views back together on _package_id + _date,
-- because that key is not unique in the current master model and can
-- multiply rows.
--
-- BI-facing correction:
-- Publish from the compatibility view, not the internal metric-status
-- view. The final table should be readable: all master columns plus
-- a small curated set of univ_* fields. Internal helper/status fields
-- stay in mdm_int views.
-- ============================================================
CREATE OR REPLACE VIEW `looker-studio-pro-452620.mdm_publish.v_master_evidence`
OPTIONS(
  description='Stable BI-facing view for the master data model redesign. Preserves all current master columns and adds a compact curated set of univ_* fields. Internal audit/status helpers stay outside the published final view. Story 5.1/5.2. Does not replace production master_stg objects.'
)
AS
WITH
base AS (
  SELECT
    compat.*,

    -- Delivery facts used only as fill-blanks fallback evidence.
    MIN(IF(compat._spend IS NOT NULL OR compat._impressions IS NOT NULL, compat._date, NULL))
      OVER (PARTITION BY compat._package_id) AS univ_delivery_start,
    MAX(IF(compat._spend IS NOT NULL OR compat._impressions IS NOT NULL, compat._date, NULL))
      OVER (PARTITION BY compat._package_id) AS univ_delivery_end,
    SUM(COALESCE(compat._spend, 0))
      OVER (PARTITION BY compat._package_id) AS univ_delivery_total_spend,
    SUM(COALESCE(compat._impressions, 0))
      OVER (PARTITION BY compat._package_id) AS univ_delivery_total_impressions
  FROM `looker-studio-pro-452620.mdm_int.int_universal_compat_view` compat
)
SELECT
  -- LIVE VIEW NOTE: Stable BI-facing evidence view with compact universal
  -- fields and source-backed fill-blanks semantics.
  base.* EXCEPT (
    uni_placement_id_placeholder,
    uni_placement_available,
    univ_delivery_start,
    univ_delivery_end,
    univ_delivery_total_spend,
    univ_delivery_total_impressions
  ),

  -- Placeholder dimensions
  CASE
    WHEN base._creative_img IS NULL AND base.fpd_creative IS NULL
    THEN 'not_available_at_source'
    ELSE COALESCE(base._creative_img, base.fpd_creative, 'not_available_at_source')
  END AS univ_creative_placeholder,
  'not_available_at_source' AS univ_dma_placeholder,
  COALESCE(base._placement_id, 'not_available_at_source') AS univ_placement_placeholder,
  COALESCE(base._campaign_name, 'not_available_at_source') AS univ_campaign_placeholder,
  COALESCE(base._advertiser, 'not_available_at_source') AS univ_advertiser_placeholder,
  COALESCE(base._supplier_code, 'not_available_at_source') AS univ_supplier_placeholder,
  COALESCE(SAFE_CAST(base._date AS STRING), 'unknown_from_source') AS univ_date_placeholder,

  -- Fill-blanks-only inferred metadata
  COALESCE(base._start_date, base.man_start_date, base.univ_delivery_start) AS univ_flight_start,
  CASE
    WHEN base._start_date IS NOT NULL THEN 'actual'
    WHEN base.man_start_date IS NOT NULL THEN 'manual'
    WHEN base.univ_delivery_start IS NOT NULL THEN 'inferred_from_delivery'
    ELSE 'not_available'
  END AS univ_flight_start_source,
  COALESCE(base._end_date, base.man_end_date, base.univ_delivery_end) AS univ_flight_end,
  CASE
    WHEN base._end_date IS NOT NULL THEN 'actual'
    WHEN base.man_end_date IS NOT NULL THEN 'manual'
    WHEN base.univ_delivery_end IS NOT NULL THEN 'inferred_from_delivery'
    ELSE 'not_available'
  END AS univ_flight_end_source,
  COALESCE(base._planned_spend, base.man_daily_planned_spend, base.univ_delivery_total_spend) AS univ_planned_spend_filled,
  CASE
    WHEN base._planned_spend IS NOT NULL THEN 'actual'
    WHEN base.man_daily_planned_spend IS NOT NULL THEN 'manual'
    WHEN base.univ_delivery_total_spend IS NOT NULL THEN 'inferred_from_delivery'
    ELSE 'not_available'
  END AS univ_planned_spend_source,
  COALESCE(base._planned_impressions, base.man_daily_planned_impressions, base.univ_delivery_total_impressions) AS univ_planned_impressions_filled,
  CASE
    WHEN base._planned_impressions IS NOT NULL THEN 'actual'
    WHEN base.man_daily_planned_impressions IS NOT NULL THEN 'manual'
    WHEN base.univ_delivery_total_impressions IS NOT NULL THEN 'inferred_from_delivery'
    ELSE 'not_available'
  END AS univ_planned_impressions_source

FROM base;
