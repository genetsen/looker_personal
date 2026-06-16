-- ============================================================
-- metric_status_summability.sql
--
-- Story 3.3: Add Metric Value Status and Summability
--
-- Every metric gets a value-status (direct, inferred, allocated,
-- unavailable) and a summability label (additive, doNotSum,
-- blocked). Repeated package/context totals are doNotSum.
-- ============================================================

CREATE OR REPLACE VIEW `looker-studio-pro-452620.mdm_int.int_metric_status`
OPTIONS(
  description='Metric value-status and summability view for the master data model redesign. Every metric explains where it came from and whether it is safe to sum. Repeated package/context totals are doNotSum on lower-grain rows. Story 3.3. Does not replace production objects.'
)
AS
SELECT
  base.*,

  -- ============================================================
  -- Final metric: _spend (additive at package/date grain)
  -- ============================================================
  CASE WHEN base._spend IS NOT NULL THEN 'direct' ELSE 'unavailable' END AS univ_spend_value_status,
  'additive' AS univ_spend_summability,
  CASE WHEN base._spend IS NOT NULL THEN base._spend ELSE 0 END AS univ_spend_safe,

  -- ============================================================
  -- Final metric: _impressions (additive at package/date grain)
  -- ============================================================
  CASE WHEN base._impressions IS NOT NULL THEN 'direct' ELSE 'unavailable' END AS univ_impressions_value_status,
  'additive' AS univ_impressions_summability,
  CASE WHEN base._impressions IS NOT NULL THEN base._impressions ELSE 0 END AS univ_impressions_safe,

  -- ============================================================
  -- Final metric: _clicks (additive at package/date grain)
  -- ============================================================
  CASE WHEN base._clicks IS NOT NULL THEN 'direct' ELSE 'unavailable' END AS univ_clicks_value_status,
  'additive' AS univ_clicks_summability,
  CASE WHEN base._clicks IS NOT NULL THEN base._clicks ELSE 0 END AS univ_clicks_safe,

  -- ============================================================
  -- Planned metrics: doNotSum (package-level context, not additive)
  -- ============================================================
  CASE WHEN base._planned_spend IS NOT NULL THEN 'direct' ELSE 'unavailable' END AS univ_planned_spend_value_status,
  'doNotSum' AS univ_planned_spend_summability,

  CASE WHEN base._planned_impressions IS NOT NULL THEN 'direct' ELSE 'unavailable' END AS univ_planned_impressions_value_status,
  'doNotSum' AS univ_planned_impressions_summability,

  CASE WHEN base.p_planned_amount_doNotSum IS NOT NULL THEN 'direct' ELSE 'unavailable' END AS univ_pkg_planned_amount_value_status,
  'doNotSum' AS univ_pkg_planned_amount_summability,

  CASE WHEN base.p_planned_impressions_doNotSum IS NOT NULL THEN 'direct' ELSE 'unavailable' END AS univ_pkg_planned_impressions_value_status,
  'doNotSum' AS univ_pkg_planned_impressions_summability,

  -- ============================================================
  -- DCM delivery metrics (additive at package/date grain)
  -- ============================================================
  CASE WHEN base.dcm_impressions IS NOT NULL THEN 'direct' ELSE 'unavailable' END AS univ_dcm_impressions_value_status,
  'additive' AS univ_dcm_impressions_summability,

  CASE WHEN base.dcm_media_cost IS NOT NULL THEN 'direct' ELSE 'unavailable' END AS univ_dcm_media_cost_value_status,
  'additive' AS univ_dcm_media_cost_summability,

  CASE WHEN base.dcm_clicks IS NOT NULL THEN 'direct' ELSE 'unavailable' END AS univ_dcm_clicks_value_status,
  'additive' AS univ_dcm_clicks_summability,

  -- ============================================================
  -- FPD metrics (varies by source — mixed status)
  -- ============================================================
  CASE
    WHEN base.fpd_impressions IS NOT NULL THEN 'direct'
    WHEN base.fpd_orig_impressions IS NOT NULL THEN 'direct'
    ELSE 'unavailable'
  END AS univ_fpd_impressions_value_status,
  'additive' AS univ_fpd_impressions_summability,

  -- ============================================================
  -- Manual edit doNotSum metrics
  -- ============================================================
  CASE WHEN base.man_total_spend_doNotSum IS NOT NULL THEN 'manual' ELSE 'unavailable' END AS univ_man_total_spend_value_status,
  'doNotSum' AS univ_man_total_spend_summability,

  CASE WHEN base.man_total_impressions_doNotSum IS NOT NULL THEN 'manual' ELSE 'unavailable' END AS univ_man_total_impressions_value_status,
  'doNotSum' AS univ_man_total_impressions_summability,

  -- ============================================================
  -- QA package doNotSum metrics
  -- ============================================================
  CASE WHEN base.qa_pkg_act_spend_doNotSum IS NOT NULL THEN 'allocated' ELSE 'unavailable' END AS univ_qa_pkg_spend_value_status,
  'doNotSum' AS univ_qa_pkg_spend_summability,

  CASE WHEN base.qa_pkg_act_impressions_doNotSum IS NOT NULL THEN 'allocated' ELSE 'unavailable' END AS univ_qa_pkg_impressions_value_status,
  'doNotSum' AS univ_qa_pkg_impressions_summability

FROM `looker-studio-pro-452620.mdm_int.int_universal_compat_view` base;
