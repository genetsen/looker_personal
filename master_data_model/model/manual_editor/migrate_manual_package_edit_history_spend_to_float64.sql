-- Migrate the Manual Data Editor's append-only history ledger to decimal spend.
--
-- Purpose:
--   Preserve every existing history row while widening current, replacement,
--   and delta spend fields from legacy STRING/INTEGER types to FLOAT64 without
--   changing the existing table's partitioning or clustering layout. The
--   temporary snapshot expires automatically after seven days and exists only
--   as a recovery point for this production schema migration.

DECLARE history_row_count_before INT64 DEFAULT (
  SELECT COUNT(*)
  FROM `looker-studio-pro-452620.landing.master_data_model_manual_package_edits_history`
);

ASSERT (
  SELECT COUNTIF(
    (current_spend IS NOT NULL AND SAFE_CAST(current_spend AS FLOAT64) IS NULL)
    OR (replacement_spend IS NOT NULL AND SAFE_CAST(replacement_spend AS FLOAT64) IS NULL)
    OR (delta_spend IS NOT NULL AND SAFE_CAST(delta_spend AS FLOAT64) IS NULL)
    OR (current_planned_spend IS NOT NULL AND SAFE_CAST(current_planned_spend AS FLOAT64) IS NULL)
    OR (replacement_planned_spend IS NOT NULL AND SAFE_CAST(replacement_planned_spend AS FLOAT64) IS NULL)
    OR (delta_planned_spend IS NOT NULL AND SAFE_CAST(delta_planned_spend AS FLOAT64) IS NULL)
  ) = 0
  FROM `looker-studio-pro-452620.landing.master_data_model_manual_package_edits_history`
) AS "History migration blocked: at least one spend value cannot be converted to FLOAT64.";

CREATE SNAPSHOT TABLE IF NOT EXISTS
  `looker-studio-pro-452620.landing.master_data_model_manual_package_edits_history_pre_float64_20260827`
CLONE `looker-studio-pro-452620.landing.master_data_model_manual_package_edits_history`
OPTIONS (
  expiration_timestamp = TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 7 DAY),
  description = "Seven-day recovery snapshot created before widening Manual Data Editor history spend fields to FLOAT64."
);

CREATE OR REPLACE TABLE
  `looker-studio-pro-452620.landing.master_data_model_manual_package_edits_history`
OPTIONS (
  description = "Append-only loader history of accepted Manual Data Editor records. The current raw table is a replaceable snapshot; this table retains each accepted loader state for recovery and audit."
)
AS
SELECT
  * REPLACE (
    SAFE_CAST(current_spend AS FLOAT64) AS current_spend,
    SAFE_CAST(replacement_spend AS FLOAT64) AS replacement_spend,
    SAFE_CAST(delta_spend AS FLOAT64) AS delta_spend,
    SAFE_CAST(current_planned_spend AS FLOAT64) AS current_planned_spend,
    SAFE_CAST(replacement_planned_spend AS FLOAT64) AS replacement_planned_spend,
    SAFE_CAST(delta_planned_spend AS FLOAT64) AS delta_planned_spend
  )
FROM `looker-studio-pro-452620.landing.master_data_model_manual_package_edits_history`;

ASSERT (
  SELECT COUNT(*)
  FROM `looker-studio-pro-452620.landing.master_data_model_manual_package_edits_history`
) = history_row_count_before
AS "History migration failed: row count changed while widening spend fields.";
