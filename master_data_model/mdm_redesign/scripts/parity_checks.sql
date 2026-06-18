-- ============================================================
-- parity_checks.sql
--
-- Read-only parity checks for the master data model redesign.
--
-- This script validates the published candidate against the live
-- master table contract. Row count is reported only as a diagnostic
-- because grain changes can legitimately change record counts.
--
-- Pass/fail validation uses:
--   1. Master-column coverage.
--   2. Overall spend, impressions, and clicks reconciliation.
--   3. Package-level spend, impressions, and clicks reconciliation.
--
-- If a stage intentionally filters, allocates, excludes, or changes
-- source scope, apply the same intended transformation to both the
-- baseline and candidate CTEs before judging the result.
-- ============================================================

-- ============================================================
-- CHECK 1: Schema coverage.
-- Every current master column must exist in the published candidate.
-- ============================================================
WITH
baseline_cols AS (
  SELECT
    column_name,
    data_type
  FROM `looker-studio-pro-452620.master_stg.INFORMATION_SCHEMA.COLUMNS`
  WHERE table_name = 'data_model'
),
candidate_cols AS (
  SELECT
    column_name,
    data_type
  FROM `looker-studio-pro-452620.mdm_publish.INFORMATION_SCHEMA.COLUMNS`
  WHERE table_name = 'v_master_evidence'
),
schema_issues AS (
  SELECT
    'schema_coverage' AS check_name,
    'FAIL' AS status,
    'column' AS metric_scope,
    b.column_name AS metric_name,
    b.data_type AS expected_value,
    COALESCE(c.data_type, 'MISSING') AS actual_value,
    CASE
      WHEN c.column_name IS NULL THEN 'Master column is missing from published candidate'
      WHEN UPPER(b.data_type) != UPPER(c.data_type) THEN 'Master column type differs in published candidate'
      ELSE 'OK'
    END AS detail
  FROM baseline_cols b
  LEFT JOIN candidate_cols c USING (column_name)
  WHERE c.column_name IS NULL
     OR UPPER(b.data_type) != UPPER(c.data_type)
)
SELECT * FROM schema_issues
UNION ALL
SELECT
  'schema_coverage' AS check_name,
  'PASS' AS status,
  'column' AS metric_scope,
  'all_master_columns' AS metric_name,
  CAST((SELECT COUNT(*) FROM baseline_cols) AS STRING) AS expected_value,
  CAST((SELECT COUNT(*) FROM baseline_cols b JOIN candidate_cols c USING (column_name)) AS STRING) AS actual_value,
  'All current master columns exist in the published candidate' AS detail
WHERE NOT EXISTS (SELECT 1 FROM schema_issues);

-- ============================================================
-- CHECK 2: Overall metric reconciliation.
-- This is the high-level validation that replaces row-count gating.
-- ============================================================
WITH
baseline AS (
  SELECT
    SUM(COALESCE(_spend, 0)) AS spend,
    SUM(COALESCE(_impressions, 0)) AS impressions,
    SUM(COALESCE(_clicks, 0)) AS clicks,
    COUNT(*) AS diagnostic_record_count
  FROM `looker-studio-pro-452620.master_stg.data_model`
),
candidate AS (
  SELECT
    SUM(COALESCE(_spend, 0)) AS spend,
    SUM(COALESCE(_impressions, 0)) AS impressions,
    SUM(COALESCE(_clicks, 0)) AS clicks,
    COUNT(*) AS diagnostic_record_count
  FROM `looker-studio-pro-452620.mdm_publish.v_master_evidence`
),
metric_diff AS (
  SELECT 'spend' AS metric_name, b.spend AS expected_value, c.spend AS actual_value, GREATEST(0.01, 0.001 * ABS(b.spend)) AS tolerance FROM baseline b, candidate c
  UNION ALL
  SELECT 'impressions', b.impressions, c.impressions, GREATEST(1, 0.001 * ABS(b.impressions)) FROM baseline b, candidate c
  UNION ALL
  SELECT 'clicks', b.clicks, c.clicks, GREATEST(1, 0.001 * ABS(b.clicks)) FROM baseline b, candidate c
),
metric_issues AS (
  SELECT
    'overall_metric_reconciliation' AS check_name,
    'FAIL' AS status,
    'overall' AS metric_scope,
    metric_name,
    CAST(expected_value AS STRING) AS expected_value,
    CAST(actual_value AS STRING) AS actual_value,
    FORMAT('%s mismatch: expected %.2f actual %.2f diff %.2f tolerance %.2f', metric_name, expected_value, actual_value, actual_value - expected_value, tolerance) AS detail
  FROM metric_diff
  WHERE ABS(actual_value - expected_value) > tolerance
)
SELECT * FROM metric_issues
UNION ALL
SELECT
  'overall_metric_reconciliation' AS check_name,
  'PASS' AS status,
  'overall' AS metric_scope,
  'spend_impressions_clicks' AS metric_name,
  'master totals' AS expected_value,
  'candidate totals' AS actual_value,
  'Overall spend, impressions, and clicks reconcile within tolerance' AS detail
WHERE NOT EXISTS (SELECT 1 FROM metric_issues)
UNION ALL
SELECT
  'row_count_diagnostic' AS check_name,
  'INFO' AS status,
  'overall' AS metric_scope,
  'record_count_not_validation' AS metric_name,
  CAST(b.diagnostic_record_count AS STRING) AS expected_value,
  CAST(c.diagnostic_record_count AS STRING) AS actual_value,
  'Row count is shown for diagnosis only; it is not a pass/fail validation when grain can change' AS detail
FROM baseline b, candidate c;

-- ============================================================
-- CHECK 3: Package-level metric reconciliation.
-- Package-level sums catch row multiplication and allocation errors
-- without treating row count as the validation target.
-- ============================================================
WITH
baseline_pkg AS (
  SELECT
    _package_id,
    SUM(COALESCE(_spend, 0)) AS spend,
    SUM(COALESCE(_impressions, 0)) AS impressions,
    SUM(COALESCE(_clicks, 0)) AS clicks
  FROM `looker-studio-pro-452620.master_stg.data_model`
  GROUP BY _package_id
),
candidate_pkg AS (
  SELECT
    _package_id,
    SUM(COALESCE(_spend, 0)) AS spend,
    SUM(COALESCE(_impressions, 0)) AS impressions,
    SUM(COALESCE(_clicks, 0)) AS clicks
  FROM `looker-studio-pro-452620.mdm_publish.v_master_evidence`
  GROUP BY _package_id
),
package_diff AS (
  SELECT
    COALESCE(b._package_id, c._package_id) AS _package_id,
    b.spend AS expected_spend,
    c.spend AS actual_spend,
    b.impressions AS expected_impressions,
    c.impressions AS actual_impressions,
    b.clicks AS expected_clicks,
    c.clicks AS actual_clicks
  FROM baseline_pkg b
  FULL OUTER JOIN candidate_pkg c USING (_package_id)
),
package_issues AS (
  SELECT
    _package_id,
    expected_spend,
    actual_spend,
    expected_impressions,
    actual_impressions,
    expected_clicks,
    actual_clicks
  FROM package_diff
  WHERE ABS(COALESCE(actual_spend, 0) - COALESCE(expected_spend, 0)) > GREATEST(0.01, 0.001 * ABS(COALESCE(expected_spend, 0)))
     OR ABS(COALESCE(actual_impressions, 0) - COALESCE(expected_impressions, 0)) > GREATEST(1, 0.001 * ABS(COALESCE(expected_impressions, 0)))
     OR ABS(COALESCE(actual_clicks, 0) - COALESCE(expected_clicks, 0)) > GREATEST(1, 0.001 * ABS(COALESCE(expected_clicks, 0)))
)
SELECT
  'package_metric_reconciliation' AS check_name,
  CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS status,
  'package' AS metric_scope,
  'spend_impressions_clicks' AS metric_name,
  'all packages reconcile' AS expected_value,
  FORMAT('%d mismatched packages', COUNT(*)) AS actual_value,
  CASE
    WHEN COUNT(*) = 0 THEN 'All package-level spend, impressions, and clicks reconcile within tolerance'
    ELSE 'One or more packages have spend, impressions, or clicks drift; inspect package_issues query details'
  END AS detail
FROM package_issues;
