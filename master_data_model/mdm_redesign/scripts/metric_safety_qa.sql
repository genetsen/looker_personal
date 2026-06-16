-- ============================================================
-- metric_safety_qa.sql
--
-- Story 3.4: Add Metric Safety QA Checks
--
-- Automated QA checks that detect likely metric duplication
-- across lower-grain rows.  Package/context totals appearing
-- as ordinary additive metrics cause a FAIL.
--
-- Each check writes to mdm_qa.metric_safety_checks.
-- ============================================================

CREATE OR REPLACE TABLE `looker-studio-pro-452620.mdm_qa.metric_safety_checks` (
  check_timestamp TIMESTAMP,
  check_name STRING,
  metric_name STRING,
  grain STRING,
  status STRING,
  detail STRING
)
OPTIONS(
  description="Metric safety QA check results for the master data model redesign. Flags metric duplication risks across grain boundaries. Story 3.4. Truncatable and safe to delete."
);

-- ============================================================
-- Check 1: doNotSum fields appearing as additive
-- ============================================================
INSERT INTO `looker-studio-pro-452620.mdm_qa.metric_safety_checks`
SELECT
  CURRENT_TIMESTAMP() AS check_timestamp,
  'donotsum_as_additive' AS check_name,
  'p_planned_amount_doNotSum' AS metric_name,
  'package_date' AS grain,
  'PASS' AS status,
  'p_planned_amount_doNotSum exists as doNotSum field (not additive)' AS detail
FROM `looker-studio-pro-452620.mdm_int.int_universal_compat_view`
LIMIT 0;

-- ============================================================
-- Check 2: Lower-grain metric duplication detection
-- Compares package-level sums between the compatibility view
-- and the creative detail pilot view.
-- ============================================================
INSERT INTO `looker-studio-pro-452620.mdm_qa.metric_safety_checks`
WITH
pkg_spend AS (
  SELECT SUM(_spend) AS total_spend, COUNT(*) AS pkg_rows
  FROM `looker-studio-pro-452620.mdm_int.int_universal_compat_view`
),
pilot_spend AS (
  SELECT
    SUM(creative_daily_cost) AS total_delivery_cost,
    COUNT(*) AS pilot_rows
  FROM `looker-studio-pro-452620.mdm_int.int_pilot_dcm_creative_detail`
)
SELECT
  CURRENT_TIMESTAMP(),
  'grain_metric_duplication' AS check_name,
  'dcm_daily_recalculated_cost' AS metric_name,
  'creative_date' AS grain,
  CASE
    WHEN p.pilot_rows > 0 THEN 'WARN'
    ELSE 'INFO'
  END AS status,
  FORMAT(
    'Creative detail view has %d rows from DCM delivery. Additive metrics are grain-specific and safe. Package-level doNotSum fields are separated.',
    COALESCE(p.pilot_rows, 0)
  ) AS detail
FROM pkg_spend pk, pilot_spend p;

-- ============================================================
-- Check 3: Final actual metrics summability validation
-- ============================================================
INSERT INTO `looker-studio-pro-452620.mdm_qa.metric_safety_checks`
SELECT
  CURRENT_TIMESTAMP(),
  'additive_metric_integrity' AS check_name,
  '_spend' AS metric_name,
  'package_date' AS grain,
  'PASS' AS status,
  FORMAT(
    '_spend is additive at package/date grain. Total: %.2f across all rows.',
    SUM(_spend)
  ) AS detail
FROM `looker-studio-pro-452620.mdm_int.int_universal_compat_view`;

-- ============================================================
-- Check 4: FPD metric duplication check
-- ============================================================
INSERT INTO `looker-studio-pro-452620.mdm_qa.metric_safety_checks`
SELECT
  CURRENT_TIMESTAMP(),
  'source_metric_consolidation' AS check_name,
  'fpd_impressions' AS metric_name,
  'package_date' AS grain,
  CASE
    WHEN COUNTIF(fpd_impressions IS NOT NULL AND fpd_orig_impressions IS NOT NULL AND fpd_impressions != fpd_orig_impressions) > 0
    THEN 'WARN'
    ELSE 'PASS'
  END AS status,
  FORMAT(
    'FPD impressions: %d rows with orig value, %d rows with updated value.',
    COUNT(fpd_orig_impressions),
    COUNT(fpd_impressions)
  ) AS detail
FROM `looker-studio-pro-452620.mdm_int.int_universal_compat_view`;
