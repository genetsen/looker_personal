-- ============================================================
-- fallback_rules.sql
--
-- Story 3.2: Add Delivery-Derived Fallback Rules
--
-- Infers missing flight dates and plan-like totals from delivery
-- evidence using fill-blanks-only logic:
--   - Flight start = MIN(delivery date)
--   - Flight end   = MAX(delivery date)
--   - Planned spend = SUM(delivery spend) when actual missing
--   - Planned impressions = SUM(delivery impressions) when actual missing
--
-- Actual/manual metadata is NEVER overwritten.
-- ============================================================

CREATE OR REPLACE VIEW `looker-studio-pro-452620.mdm_int.int_fallback_rules`
OPTIONS(
  description='View applying delivery-derived fallback rules for missing flight dates and plan-like totals. Fill-blanks-only logic that never overwrites actual or manual metadata. Story 3.2. Does not replace production objects.'
)
AS
WITH
delivery_summary AS (
  SELECT
    _package_id,
    MIN(_date) AS delivery_start,
    MAX(_date) AS delivery_end,
    SUM(_spend) AS delivery_total_spend,
    SUM(_impressions) AS delivery_total_impressions,
    SUM(_clicks) AS delivery_total_clicks
  FROM `looker-studio-pro-452620.master_stg.data_model`
  WHERE _spend IS NOT NULL OR _impressions IS NOT NULL
  GROUP BY _package_id
)
SELECT
  base.*,
  ds.delivery_start,
  ds.delivery_end,
  ds.delivery_total_spend,
  ds.delivery_total_impressions,
  ds.delivery_total_clicks,

  -- ============================================================
  -- Flight start: actual > manual > inferred(MIN delivery date)
  -- ============================================================
  COALESCE(
    base._start_date,
    base.man_start_date,
    ds.delivery_start
  ) AS univ_flight_start,

  CASE
    WHEN base._start_date IS NOT NULL THEN 'actual'
    WHEN base.man_start_date IS NOT NULL THEN 'manual'
    WHEN ds.delivery_start IS NOT NULL THEN 'inferred_from_delivery'
    ELSE 'not_available'
  END AS univ_flight_start_source,

  -- ============================================================
  -- Flight end: actual > manual > inferred(MAX delivery date)
  -- ============================================================
  COALESCE(
    base._end_date,
    base.man_end_date,
    ds.delivery_end
  ) AS univ_flight_end,

  CASE
    WHEN base._end_date IS NOT NULL THEN 'actual'
    WHEN base.man_end_date IS NOT NULL THEN 'manual'
    WHEN ds.delivery_end IS NOT NULL THEN 'inferred_from_delivery'
    ELSE 'not_available'
  END AS univ_flight_end_source,

  -- ============================================================
  -- Planned spend: actual > manual > inferred(sum delivery spend)
  -- ============================================================
  COALESCE(
    base._planned_spend,
    base.man_daily_planned_spend,
    ds.delivery_total_spend
  ) AS univ_planned_spend_filled,

  CASE
    WHEN base._planned_spend IS NOT NULL THEN 'actual'
    WHEN base.man_daily_planned_spend IS NOT NULL THEN 'manual'
    WHEN ds.delivery_total_spend IS NOT NULL THEN 'inferred_from_delivery'
    ELSE 'not_available'
  END AS univ_planned_spend_filled_source,

  -- ============================================================
  -- Planned impressions: actual > manual > inferred(sum delivery)
  -- ============================================================
  COALESCE(
    base._planned_impressions,
    base.man_daily_planned_impressions,
    CAST(ds.delivery_total_impressions AS FLOAT64)
  ) AS univ_planned_impressions_filled,

  CASE
    WHEN base._planned_impressions IS NOT NULL THEN 'actual'
    WHEN base.man_daily_planned_impressions IS NOT NULL THEN 'manual'
    WHEN ds.delivery_total_impressions IS NOT NULL THEN 'inferred_from_delivery'
    ELSE 'not_available'
  END AS univ_planned_impressions_filled_source

FROM `looker-studio-pro-452620.mdm_int.int_universal_compat_view` base
LEFT JOIN delivery_summary ds ON base._package_id = ds._package_id;
