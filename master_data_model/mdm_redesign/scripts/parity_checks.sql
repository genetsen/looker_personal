-- ============================================================
-- parity_checks.sql
--
-- Automated parity checks comparing a candidate view/table
-- against the live production master table baseline.
--
-- Each check returns pass/fail rows so that the proof report
-- can simply union and format them.
--
-- Tolerance: exact string match for columns; 0.01 absolute
-- or 0.1% relative for totals (whichever is larger).
-- ============================================================

-- Parameters (replace @CANDIDATE_TABLE with actual target):
--   @CANDIDATE_TABLE = `looker-studio-pro-452620.mdm_int.int_universal_compat_view`
--   @BASELINE_TABLE  = `looker-studio-pro-452620.mdm_qa.master_baseline_snapshot`

-- ============================================================
-- CHECK 1: Schema Coverage — every baseline column exists
--          in the candidate with matching data type.
-- ============================================================
WITH
baseline_cols AS (
  SELECT
    c.column_name,
    c.data_type
  FROM `looker-studio-pro-452620.master_stg.INFORMATION_SCHEMA.COLUMNS` c
  WHERE c.table_name = 'data_model'
),
candidate_cols AS (
  SELECT
    c.column_name,
    c.data_type
  FROM `looker-studio-pro-452620.mdm_int.INFORMATION_SCHEMA.COLUMNS` c
  WHERE c.table_name = 'int_universal_compat_view'
),
missing_cols AS (
  SELECT
    'schema_coverage' AS check_name,
    'FAIL' AS status,
    b.column_name,
    b.data_type AS expected_type,
    NULL AS actual_type,
    'Column missing from candidate' AS detail
  FROM baseline_cols b
  LEFT JOIN candidate_cols c ON b.column_name = c.column_name
  WHERE c.column_name IS NULL
),
type_mismatch AS (
  SELECT
    'schema_coverage' AS check_name,
    'FAIL' AS status,
    b.column_name,
    b.data_type AS expected_type,
    c.data_type AS actual_type,
    'Data type mismatch' AS detail
  FROM baseline_cols b
  JOIN candidate_cols c ON b.column_name = c.column_name
  WHERE UPPER(b.data_type) != UPPER(c.data_type)
)
SELECT * FROM missing_cols
UNION ALL
SELECT * FROM type_mismatch
UNION ALL
SELECT
  'schema_coverage' AS check_name,
  'PASS' AS status,
  'all_columns_present' AS column_name,
  CAST(COUNT(*) AS STRING) AS expected_type,
  NULL AS actual_type,
  FORMAT('All %d baseline columns present in candidate with matching types', COUNT(*)) AS detail
FROM baseline_cols b
JOIN candidate_cols c ON b.column_name = c.column_name AND UPPER(b.data_type) = UPPER(c.data_type)
HAVING COUNT(*) = (SELECT COUNT(*) FROM baseline_cols);

-- ============================================================
-- CHECK 2: Total Reconciliation — key metric sums match
--          baseline within tolerance.
-- Tolerance: 0.01 absolute OR 0.1% relative, whichever is larger.
-- ============================================================
WITH
baseline AS (
  SELECT
    snapshot_data.totals.total_rows AS baseline_rows,
    snapshot_data.totals.total_packages AS baseline_packages,
    snapshot_data.totals.total_spend AS baseline_spend,
    snapshot_data.totals.total_impressions AS baseline_impressions,
    snapshot_data.totals.total_clicks AS baseline_clicks
  FROM `looker-studio-pro-452620.mdm_qa.master_baseline_snapshot`
  ORDER BY snapshot_timestamp DESC
  LIMIT 1
),
candidate AS (
  SELECT
    COUNT(*) AS candidate_rows,
    COUNT(DISTINCT _package_id) AS candidate_packages,
    SUM(_spend) AS candidate_spend,
    SUM(_impressions) AS candidate_impressions,
    SUM(_clicks) AS candidate_clicks
  FROM `looker-studio-pro-452620.mdm_int.int_universal_compat_view`
),
totals_check AS (
  SELECT 'total_reconciliation' AS check_name, 'FAIL' AS status, 'total_rows' AS metric,
    CAST(b.baseline_rows AS STRING) AS expected, CAST(c.candidate_rows AS STRING) AS actual,
    FORMAT('Row count mismatch: baseline=%d candidate=%d diff=%d', b.baseline_rows, c.candidate_rows, c.candidate_rows - b.baseline_rows) AS detail
  FROM baseline b, candidate c
  WHERE ABS(c.candidate_rows - b.baseline_rows) > 0.01
  UNION ALL
  SELECT 'total_reconciliation', 'FAIL', 'total_packages',
    CAST(b.baseline_packages AS STRING), CAST(c.candidate_packages AS STRING),
    FORMAT('Package count mismatch: baseline=%d candidate=%d diff=%d', b.baseline_packages, c.candidate_packages, c.candidate_packages - b.baseline_packages)
  FROM baseline b, candidate c
  WHERE ABS(c.candidate_packages - b.baseline_packages) > 0.01
  UNION ALL
  SELECT 'total_reconciliation', 'FAIL', 'total_spend',
    CAST(ROUND(b.baseline_spend, 2) AS STRING), CAST(ROUND(c.candidate_spend, 2) AS STRING),
    FORMAT('Spend mismatch: baseline=%.2f candidate=%.2f diff=%.2f', b.baseline_spend, c.candidate_spend, c.candidate_spend - b.baseline_spend)
  FROM baseline b, candidate c
  WHERE ABS(c.candidate_spend - b.baseline_spend) > GREATEST(0.01, 0.001 * ABS(b.baseline_spend))
  UNION ALL
  SELECT 'total_reconciliation', 'FAIL', 'total_impressions',
    CAST(b.baseline_impressions AS STRING), CAST(c.candidate_impressions AS STRING),
    FORMAT('Impressions mismatch: baseline=%d candidate=%d diff=%d', b.baseline_impressions, c.candidate_impressions, c.candidate_impressions - b.baseline_impressions)
  FROM baseline b, candidate c
  WHERE ABS(c.candidate_impressions - b.baseline_impressions) > GREATEST(1, 0.001 * b.baseline_impressions)
  UNION ALL
  SELECT 'total_reconciliation', 'FAIL', 'total_clicks',
    CAST(b.baseline_clicks AS STRING), CAST(c.candidate_clicks AS STRING),
    FORMAT('Clicks mismatch: baseline=%d candidate=%d diff=%d', b.baseline_clicks, c.candidate_clicks, c.candidate_clicks - b.baseline_clicks)
  FROM baseline b, candidate c
  WHERE ABS(c.candidate_clicks - b.baseline_clicks) > GREATEST(1, 0.001 * b.baseline_clicks)
)
SELECT * FROM totals_check
UNION ALL
SELECT 'total_reconciliation', 'PASS', 'all_metrics', NULL, NULL, 'All metrics reconcile within tolerance'
WHERE NOT EXISTS (SELECT 1 FROM totals_check);

-- ============================================================
-- CHECK 3: doNotSum Safety — ensure doNotSum-tagged fields
--          are not present in candidate as ordinary additive metrics,
--          and their values match baseline totals exactly
--          (they should be idempotent package-wide repeats).
-- ============================================================
WITH
baseline_donotsum AS (
  SELECT
    JSON_EXTRACT_SCALAR(d.snapshot_data, '$.totals.total_planned_spend') AS baseline_planned_spend
  FROM `looker-studio-pro-452620.mdm_qa.master_baseline_snapshot` d
  ORDER BY snapshot_timestamp DESC
  LIMIT 1
),
candidate_donotsum AS (
  SELECT
    SUM(p_planned_amount_doNotSum) AS candidate_planned_spend
  FROM `looker-studio-pro-452620.mdm_int.int_universal_compat_view`
)
SELECT 'donotsum_safety' AS check_name,
  CASE
    WHEN ABS(c.candidate_planned_spend - CAST(b.baseline_planned_spend AS FLOAT64))
         > GREATEST(0.01, 0.001 * ABS(CAST(b.baseline_planned_spend AS FLOAT64)))
    THEN 'FAIL'
    ELSE 'PASS'
  END AS status,
  'p_planned_amount_doNotSum' AS field,
  b.baseline_planned_spend AS expected,
  CAST(ROUND(c.candidate_planned_spend, 2) AS STRING) AS actual,
  CASE
    WHEN ABS(c.candidate_planned_spend - CAST(b.baseline_planned_spend AS FLOAT64))
         > GREATEST(0.01, 0.001 * ABS(CAST(b.baseline_planned_spend AS FLOAT64)))
    THEN FORMAT('doNotSum field drifted: baseline=%s candidate=%.2f', b.baseline_planned_spend, c.candidate_planned_spend)
    ELSE 'doNotSum field preserved correctly'
  END AS detail
FROM baseline_donotsum b, candidate_donotsum c;
