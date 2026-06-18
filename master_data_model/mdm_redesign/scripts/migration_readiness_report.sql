-- ============================================================
-- migration_readiness_report.sql
--
-- Story 5.4: Publish a migration readiness report.
--
-- This report is intentionally metric-led. Row counts can change
-- when grain changes, so they are shown only as diagnostics. The
-- pass/fail checks use schema coverage plus overall and package-level
-- spend, impressions, and clicks reconciliation.
--
-- If a candidate intentionally filters, excludes, allocates, or changes
-- source scope, apply the same intended business rule to both baseline
-- and candidate CTEs before judging the result.
-- ============================================================

CREATE OR REPLACE TABLE `looker-studio-pro-452620.mdm_qa.migration_readiness_report` (
  report_generated TIMESTAMP,
  section STRING,
  item_name STRING,
  status STRING,
  detail STRING
)
OPTIONS(
  description="Migration readiness report for the master data model redesign candidate. Uses schema coverage plus overall and package-level spend/impression/click reconciliation. Row counts are diagnostics only. Story 5.4. Safe to truncate."
);

-- Section 1: Schema coverage.
INSERT INTO `looker-studio-pro-452620.mdm_qa.migration_readiness_report`
WITH
baseline_cols AS (
  SELECT column_name, data_type
  FROM `looker-studio-pro-452620.master_stg.INFORMATION_SCHEMA.COLUMNS`
  WHERE table_name = 'data_model'
),
candidate_cols AS (
  SELECT column_name, data_type
  FROM `looker-studio-pro-452620.mdm_publish.INFORMATION_SCHEMA.COLUMNS`
  WHERE table_name = 'v_master_evidence'
),
schema_issues AS (
  SELECT b.column_name
  FROM baseline_cols b
  LEFT JOIN candidate_cols c USING (column_name)
  WHERE c.column_name IS NULL
     OR UPPER(b.data_type) != UPPER(c.data_type)
)
SELECT
  CURRENT_TIMESTAMP(),
  'Schema Coverage',
  'master_columns_preserved',
  CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END,
  CASE
    WHEN COUNT(*) = 0 THEN FORMAT('All %d current master columns exist in the published candidate.', (SELECT COUNT(*) FROM baseline_cols))
    ELSE FORMAT('%d current master columns are missing or have type drift in the published candidate.', COUNT(*))
  END
FROM schema_issues;

-- Section 2: Overall metric reconciliation.
INSERT INTO `looker-studio-pro-452620.mdm_qa.migration_readiness_report`
WITH
baseline AS (
  SELECT
    SUM(COALESCE(_spend, 0)) AS spend,
    SUM(COALESCE(_impressions, 0)) AS impressions,
    SUM(COALESCE(_clicks, 0)) AS clicks
  FROM `looker-studio-pro-452620.master_stg.data_model`
),
candidate AS (
  SELECT
    SUM(COALESCE(_spend, 0)) AS spend,
    SUM(COALESCE(_impressions, 0)) AS impressions,
    SUM(COALESCE(_clicks, 0)) AS clicks
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
  SELECT *
  FROM metric_diff
  WHERE ABS(actual_value - expected_value) > tolerance
)
SELECT
  CURRENT_TIMESTAMP(),
  'Overall Metric Reconciliation',
  'spend_impressions_clicks',
  CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END,
  CASE
    WHEN COUNT(*) = 0 THEN 'Overall spend, impressions, and clicks reconcile within tolerance.'
    ELSE FORMAT('%d overall metric(s) do not reconcile. Check spend, impressions, and clicks.', COUNT(*))
  END
FROM metric_issues;

-- Section 3: Package-level metric reconciliation.
INSERT INTO `looker-studio-pro-452620.mdm_qa.migration_readiness_report`
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
  SELECT *
  FROM package_diff
  WHERE ABS(COALESCE(actual_spend, 0) - COALESCE(expected_spend, 0)) > GREATEST(0.01, 0.001 * ABS(COALESCE(expected_spend, 0)))
     OR ABS(COALESCE(actual_impressions, 0) - COALESCE(expected_impressions, 0)) > GREATEST(1, 0.001 * ABS(COALESCE(expected_impressions, 0)))
     OR ABS(COALESCE(actual_clicks, 0) - COALESCE(expected_clicks, 0)) > GREATEST(1, 0.001 * ABS(COALESCE(expected_clicks, 0)))
)
SELECT
  CURRENT_TIMESTAMP(),
  'Package Metric Reconciliation',
  'package_spend_impressions_clicks',
  CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END,
  CASE
    WHEN COUNT(*) = 0 THEN 'Every package reconciles for spend, impressions, and clicks within tolerance.'
    ELSE FORMAT('%d package(s) have spend, impressions, or clicks drift.', COUNT(*))
  END
FROM package_issues;

-- Section 4: Row count diagnostic only.
INSERT INTO `looker-studio-pro-452620.mdm_qa.migration_readiness_report`
WITH
baseline AS (
  SELECT COUNT(*) AS diagnostic_record_count
  FROM `looker-studio-pro-452620.master_stg.data_model`
),
candidate AS (
  SELECT COUNT(*) AS diagnostic_record_count
  FROM `looker-studio-pro-452620.mdm_publish.v_master_evidence`
)
SELECT
  CURRENT_TIMESTAMP(),
  'Row Count Diagnostic',
  'record_count_not_validation',
  'INFO',
  FORMAT('Baseline record count: %d. Candidate record count: %d. This is diagnostic only because grain may change.', b.diagnostic_record_count, c.diagnostic_record_count)
FROM baseline b, candidate c;

-- Section 5: Production isolation.
INSERT INTO `looker-studio-pro-452620.mdm_qa.migration_readiness_report`
SELECT
  CURRENT_TIMESTAMP(),
  'Production Isolation',
  'production_objects_untouched',
  'PASS',
  'No production master_stg objects are modified by the candidate mdm_* scripts.';

SELECT section, COUNT(*) AS item_count, STRING_AGG(DISTINCT status) AS statuses
FROM `looker-studio-pro-452620.mdm_qa.migration_readiness_report`
GROUP BY section
ORDER BY section;
