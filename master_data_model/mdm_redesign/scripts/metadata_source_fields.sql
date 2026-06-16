-- ============================================================
-- metadata_source_fields.sql
--
-- Story 3.1: Add Metadata Source Fields
--
-- Adds audit fields to every governed metadata value explaining
-- where it came from: actual, manual, inferred, mixed, or
-- placeholder.  Actual/manual values are never overwritten.
-- ============================================================

CREATE OR REPLACE VIEW `looker-studio-pro-452620.mdm_int.int_metadata_audit`
OPTIONS(
  description='Metadata audit view tracking provenance of every governed field. Each metadata value has a _source, _reason, _evidence, and _confidence indicator. Story 3.1. Does not replace production objects.'
)
AS
SELECT
  base.*,

  -- ============================================================
  -- Flight dates metadata audit
  -- ============================================================
  CASE
    WHEN _start_date IS NOT NULL AND _end_date IS NOT NULL THEN 'actual'
    WHEN _start_date IS NOT NULL THEN 'actual'
    WHEN _end_date IS NOT NULL THEN 'actual'
    ELSE 'placeholder'
  END AS univ_flight_dates_source,

  CASE
    WHEN _start_date IS NOT NULL AND _end_date IS NOT NULL THEN 'Source-provided start and end dates'
    WHEN _start_date IS NOT NULL THEN 'Source-provided start date only'
    WHEN _end_date IS NOT NULL THEN 'Source-provided end date only'
    ELSE 'No source metadata available'
  END AS univ_flight_dates_reason,

  CASE
    WHEN _start_date IS NOT NULL OR _end_date IS NOT NULL THEN 'source_metadata_direct'
    ELSE 'no_source_data'
  END AS univ_flight_dates_evidence,

  CASE
    WHEN _start_date IS NOT NULL OR _end_date IS NOT NULL THEN 'high'
    ELSE 'none'
  END AS univ_flight_dates_confidence,

  -- ============================================================
  -- Spend metadata audit
  -- ============================================================
  CASE
    WHEN _planned_spend IS NOT NULL THEN 'actual'
    WHEN man_daily_planned_spend IS NOT NULL THEN 'manual'
    ELSE 'placeholder'
  END AS univ_planned_spend_source,

  CASE
    WHEN _planned_spend IS NOT NULL THEN 'Source-provided planned spend'
    WHEN man_daily_planned_spend IS NOT NULL THEN 'Manual override via Manual Package Editor'
    ELSE 'No planned spend available'
  END AS univ_planned_spend_reason,

  CASE
    WHEN _planned_spend IS NOT NULL THEN 'direct_source_field'
    WHEN man_daily_planned_spend IS NOT NULL THEN 'manual_editor_entry'
    ELSE 'not_provided'
  END AS univ_planned_spend_evidence,

  CASE
    WHEN _planned_spend IS NOT NULL THEN 'high'
    WHEN man_daily_planned_spend IS NOT NULL THEN 'high'
    ELSE 'none'
  END AS univ_planned_spend_confidence,

  -- ============================================================
  -- Impressions metadata audit
  -- ============================================================
  CASE
    WHEN _planned_impressions IS NOT NULL THEN 'actual'
    WHEN man_daily_planned_impressions IS NOT NULL THEN 'manual'
    ELSE 'placeholder'
  END AS univ_planned_impressions_source,

  CASE
    WHEN _planned_impressions IS NOT NULL THEN 'Source-provided planned impressions'
    WHEN man_daily_planned_impressions IS NOT NULL THEN 'Manual override via Manual Package Editor'
    ELSE 'No planned impressions available'
  END AS univ_planned_impressions_reason,

  CASE
    WHEN _planned_impressions IS NOT NULL THEN 'direct_source_field'
    WHEN man_daily_planned_impressions IS NOT NULL THEN 'manual_editor_entry'
    ELSE 'not_provided'
  END AS univ_planned_impressions_evidence,

  CASE
    WHEN _planned_impressions IS NOT NULL THEN 'high'
    WHEN man_daily_planned_impressions IS NOT NULL THEN 'high'
    ELSE 'none'
  END AS univ_planned_impressions_confidence,

  -- ============================================================
  -- Metadata status for final actual metrics
  -- ============================================================
  CASE
    WHEN _spend IS NOT NULL THEN 'direct'
    ELSE 'unavailable'
  END AS univ_actual_spend_status,

  CASE
    WHEN _impressions IS NOT NULL THEN 'direct'
    ELSE 'unavailable'
  END AS univ_actual_impressions_status,

  CASE
    WHEN _clicks IS NOT NULL THEN 'direct'
    ELSE 'unavailable'
  END AS univ_actual_clicks_status

FROM `looker-studio-pro-452620.mdm_int.int_universal_compat_view` base;
