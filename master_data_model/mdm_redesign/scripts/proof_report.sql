-- ============================================================
-- proof_report.sql
--
-- Builds a readable proof report from migration_readiness_report.
--
-- The source readiness report is metric-led:
--   - schema coverage,
--   - overall spend/impressions/clicks reconciliation,
--   - package-level spend/impressions/clicks reconciliation,
--   - row count as diagnostic only.
--
-- Run after migration_readiness_report.sql.
-- ============================================================

CREATE OR REPLACE TABLE `looker-studio-pro-452620.mdm_qa.proof_report` (
  report_generated TIMESTAMP,
  report_section STRING,
  check_name STRING,
  status STRING,
  detail STRING
)
OPTIONS(
  description="Readable proof report for the master data model redesign candidate. Summarizes metric-led readiness checks. Row counts are diagnostics only. Truncatable and safe to delete."
);

INSERT INTO `looker-studio-pro-452620.mdm_qa.proof_report`
SELECT
  CURRENT_TIMESTAMP() AS report_generated,
  section AS report_section,
  item_name AS check_name,
  status,
  detail
FROM `looker-studio-pro-452620.mdm_qa.migration_readiness_report`;

INSERT INTO `looker-studio-pro-452620.mdm_qa.proof_report`
WITH status_counts AS (
  SELECT
    COUNTIF(status = 'FAIL') AS fail_count,
    COUNTIF(status = 'PASS') AS pass_count,
    COUNTIF(status = 'INFO') AS info_count
  FROM `looker-studio-pro-452620.mdm_qa.migration_readiness_report`
)
SELECT
  CURRENT_TIMESTAMP(),
  'Proof Report Summary',
  'readiness_summary',
  CASE WHEN fail_count = 0 THEN 'PASS' ELSE 'FAIL' END,
  FORMAT('Readiness report contains %d PASS, %d FAIL, and %d INFO item(s). Row count INFO items are not validation failures.', pass_count, fail_count, info_count)
FROM status_counts;

SELECT report_section, check_name, status, detail
FROM `looker-studio-pro-452620.mdm_qa.proof_report`
ORDER BY
  CASE status WHEN 'FAIL' THEN 1 WHEN 'PASS' THEN 2 ELSE 3 END,
  report_section,
  check_name;
