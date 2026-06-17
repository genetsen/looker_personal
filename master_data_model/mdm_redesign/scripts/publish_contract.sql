-- ============================================================
-- publish_contract.sql
--
-- Story 5.1: Define The Published Output Contract
-- Story 5.3: Run Side-By-Side Parity Checks
--
-- Registers all published BI-facing objects, their grain,
-- source table, and whether they are a shortcut or SOT.
-- ============================================================

CREATE OR REPLACE TABLE `looker-studio-pro-452620.mdm_publish.publish_contract` (
  registered_at TIMESTAMP,
  object_name STRING,
  object_type STRING,
  dataset STRING,
  grain STRING,
  source_table STRING,
  is_shortcut BOOL,
  description STRING
)
OPTIONS(
  description='Published output contract for the master data model redesign. Registers every stable BI-facing object with its grain, source, and role. Story 5.1. Does not replace production objects.'
);

INSERT INTO `looker-studio-pro-452620.mdm_publish.publish_contract` VALUES
  (CURRENT_TIMESTAMP(), 'v_master_evidence', 'VIEW', 'mdm_publish', 'multi-grain', 'mdm_int.int_placeholder_semantics', FALSE,
   'Stable BI-facing source-of-truth view. Primary evidence table for dashboard consumption. Contains placeholder dimensions, metric safety labels, and audit fields.'),
  (CURRENT_TIMESTAMP(), 'mart_package_daily', 'VIEW', 'mdm_mart', 'package/date', 'mdm_int.int_universal_compat_view', TRUE,
   'Package/daily shortcut mart. Convenience view for package-level reporting. Not a separate source of truth.'),
  (CURRENT_TIMESTAMP(), 'mart_creative_daily', 'VIEW', 'mdm_mart', 'creative/date', 'mdm_int.int_pilot_dcm_creative_detail', TRUE,
   'Creative/daily shortcut mart. Pilot lower-grain detail for DCM creative reporting. Not a separate source of truth.'),
  (CURRENT_TIMESTAMP(), 'master_baseline_snapshot', 'TABLE', 'mdm_qa', 'snapshot', 'master_stg.data_model', FALSE,
   'Live master baseline snapshot for parity and reconciliation validation. QA object — not for reporting.'),
  (CURRENT_TIMESTAMP(), 'proof_report', 'TABLE', 'mdm_qa', 'report', 'mdm_qa.*', FALSE,
   'Proof report for schema coverage, total reconciliation, doNotSum safety, and production isolation. QA object — not for reporting.'),
  (CURRENT_TIMESTAMP(), 'metric_safety_checks', 'TABLE', 'mdm_qa', 'validation', 'mdm_qa.*', FALSE,
   'Metric safety QA check results. QA object — not for reporting.');

SELECT 'Publish contract registered' AS status, COUNT(*) AS objects FROM `looker-studio-pro-452620.mdm_publish.publish_contract`;
