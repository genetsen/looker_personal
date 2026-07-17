-- Create landing tables for dashboard-like manual package edits.
--
-- Purpose:
--   Keep the Google Sheet edit surface separate from the daily table consumed
--   by the master data model. The loader refreshes both tables each run.

CREATE TABLE IF NOT EXISTS `looker-studio-pro-452620.landing.master_data_model_manual_package_edits_raw` (
  is_active BOOL,
  edit_id STRING,
  package_id STRING,
  man_start_date DATE,
  man_end_date DATE,
  man_flight_start_date DATE,
  man_flight_end_date DATE,
  current_row_count INT64,
  current_first_date DATE,
  current_last_date DATE,
  current_flight_start_date DATE,
  current_flight_end_date DATE,
  replacement_flight_start_date DATE,
  replacement_flight_end_date DATE,
  current_spend FLOAT64,
  replacement_spend FLOAT64,
  delta_spend FLOAT64,
  current_impressions FLOAT64,
  replacement_impressions FLOAT64,
  delta_impressions FLOAT64,
  current_planned_spend FLOAT64,
  replacement_planned_spend FLOAT64,
  delta_planned_spend FLOAT64,
  current_planned_impressions FLOAT64,
  replacement_planned_impressions FLOAT64,
  delta_planned_impressions FLOAT64,
  current_clicks FLOAT64,
  replacement_clicks FLOAT64,
  delta_clicks FLOAT64,
  current_video_plays FLOAT64,
  replacement_video_plays FLOAT64,
  delta_video_plays FLOAT64,
  current_video_comps FLOAT64,
  replacement_video_comps FLOAT64,
  delta_video_comps FLOAT64,
  current_advertiser_name STRING,
  man_advertiser_name STRING,
  current_package_type STRING,
  man_package_type STRING,
  current_channel STRING,
  man_channel STRING,
  current_campaign_name STRING,
  man_campaign_name STRING,
  current_initiative STRING,
  man_initiative STRING,
  current_supplier_code STRING,
  man_supplier_code STRING,
  current_supplier_name STRING,
  man_supplier_name STRING,
  current_package_name STRING,
  man_package_name STRING,
  current_package_name_friendly STRING,
  man_package_name_friendly STRING,
  current_ADIF_channel STRING,
  man_ADIF_channel STRING,
  current_benchmark_kpi STRING,
  man_benchmark_kpi STRING,
  current_benchmark_value FLOAT64,
  man_benchmark_value FLOAT64,
  advertiser_name STRING,
  advertiser_short_name STRING,
  campaign_name STRING,
  campaign_friendly STRING,
  product_code STRING,
  product_name STRING,
  package_type STRING,
  package_name STRING,
  package_name_friendly STRING,
  ADIF_channel STRING,
  placement_id STRING,
  placement_name STRING,
  supplier_code STRING,
  supplier_name STRING,
  supplier_logo STRING,
  p_buy_type STRING,
  p_buy_category STRING,
  channel STRING,
  channel_raw STRING,
  channel_group STRING,
  media_name STRING,
  p_cost_method STRING,
  p_planned_amount_doNotSum FLOAT64,
  p_planned_impressions_doNotSum FLOAT64,
  p_planned_units_doNotSum FLOAT64,
  p_unit_type STRING,
  p_rate FLOAT64,
  edit_reason STRING,
  editor_email STRING,
  manual_edit_at TIMESTAMP,
  manual_edit_by STRING,
  manual_edit_published_at TIMESTAMP,
  source_sheet_url STRING,
  source_sheet_modified_time TIMESTAMP,
  loaded_at TIMESTAMP,
  validation_status STRING,
  validation_messages STRING
)
OPTIONS (
  description = "Raw dashboard-like manual package edits loaded from the master data model Google Sheet. Safe to truncate via the loader."
);

ALTER TABLE `looker-studio-pro-452620.landing.master_data_model_manual_package_edits_raw`
ADD COLUMN IF NOT EXISTS current_row_count INT64;

ALTER TABLE `looker-studio-pro-452620.landing.master_data_model_manual_package_edits_raw`
ADD COLUMN IF NOT EXISTS current_first_date DATE;

ALTER TABLE `looker-studio-pro-452620.landing.master_data_model_manual_package_edits_raw`
ADD COLUMN IF NOT EXISTS current_last_date DATE;

ALTER TABLE `looker-studio-pro-452620.landing.master_data_model_manual_package_edits_raw`
ADD COLUMN IF NOT EXISTS man_flight_start_date DATE;

ALTER TABLE `looker-studio-pro-452620.landing.master_data_model_manual_package_edits_raw`
ADD COLUMN IF NOT EXISTS man_flight_end_date DATE;

ALTER TABLE `looker-studio-pro-452620.landing.master_data_model_manual_package_edits_raw`
ADD COLUMN IF NOT EXISTS current_flight_start_date DATE;

ALTER TABLE `looker-studio-pro-452620.landing.master_data_model_manual_package_edits_raw`
ADD COLUMN IF NOT EXISTS current_flight_end_date DATE;

ALTER TABLE `looker-studio-pro-452620.landing.master_data_model_manual_package_edits_raw`
ADD COLUMN IF NOT EXISTS replacement_flight_start_date DATE;

ALTER TABLE `looker-studio-pro-452620.landing.master_data_model_manual_package_edits_raw`
ADD COLUMN IF NOT EXISTS replacement_flight_end_date DATE;

ALTER TABLE `looker-studio-pro-452620.landing.master_data_model_manual_package_edits_raw`
ADD COLUMN IF NOT EXISTS current_advertiser_name STRING;

ALTER TABLE `looker-studio-pro-452620.landing.master_data_model_manual_package_edits_raw`
ADD COLUMN IF NOT EXISTS man_advertiser_name STRING;

ALTER TABLE `looker-studio-pro-452620.landing.master_data_model_manual_package_edits_raw`
ADD COLUMN IF NOT EXISTS current_package_type STRING;

ALTER TABLE `looker-studio-pro-452620.landing.master_data_model_manual_package_edits_raw`
ADD COLUMN IF NOT EXISTS man_package_type STRING;

ALTER TABLE `looker-studio-pro-452620.landing.master_data_model_manual_package_edits_raw`
ADD COLUMN IF NOT EXISTS current_channel STRING;

ALTER TABLE `looker-studio-pro-452620.landing.master_data_model_manual_package_edits_raw`
ADD COLUMN IF NOT EXISTS man_channel STRING;

ALTER TABLE `looker-studio-pro-452620.landing.master_data_model_manual_package_edits_raw`
ADD COLUMN IF NOT EXISTS current_campaign_name STRING;

ALTER TABLE `looker-studio-pro-452620.landing.master_data_model_manual_package_edits_raw`
ADD COLUMN IF NOT EXISTS man_campaign_name STRING;

ALTER TABLE `looker-studio-pro-452620.landing.master_data_model_manual_package_edits_raw`
ADD COLUMN IF NOT EXISTS current_initiative STRING;

ALTER TABLE `looker-studio-pro-452620.landing.master_data_model_manual_package_edits_raw`
ADD COLUMN IF NOT EXISTS man_initiative STRING;

ALTER TABLE `looker-studio-pro-452620.landing.master_data_model_manual_package_edits_raw`
ADD COLUMN IF NOT EXISTS current_supplier_code STRING;

ALTER TABLE `looker-studio-pro-452620.landing.master_data_model_manual_package_edits_raw`
ADD COLUMN IF NOT EXISTS man_supplier_code STRING;

ALTER TABLE `looker-studio-pro-452620.landing.master_data_model_manual_package_edits_raw`
ADD COLUMN IF NOT EXISTS current_supplier_name STRING;

ALTER TABLE `looker-studio-pro-452620.landing.master_data_model_manual_package_edits_raw`
ADD COLUMN IF NOT EXISTS man_supplier_name STRING;

ALTER TABLE `looker-studio-pro-452620.landing.master_data_model_manual_package_edits_raw`
ADD COLUMN IF NOT EXISTS current_package_name STRING;

ALTER TABLE `looker-studio-pro-452620.landing.master_data_model_manual_package_edits_raw`
ADD COLUMN IF NOT EXISTS man_package_name STRING;

ALTER TABLE `looker-studio-pro-452620.landing.master_data_model_manual_package_edits_raw`
ADD COLUMN IF NOT EXISTS current_package_name_friendly STRING;

ALTER TABLE `looker-studio-pro-452620.landing.master_data_model_manual_package_edits_raw`
ADD COLUMN IF NOT EXISTS man_package_name_friendly STRING;

ALTER TABLE `looker-studio-pro-452620.landing.master_data_model_manual_package_edits_raw`
ADD COLUMN IF NOT EXISTS current_ADIF_channel STRING;

ALTER TABLE `looker-studio-pro-452620.landing.master_data_model_manual_package_edits_raw`
ADD COLUMN IF NOT EXISTS man_ADIF_channel STRING;

ALTER TABLE `looker-studio-pro-452620.landing.master_data_model_manual_package_edits_raw`
ADD COLUMN IF NOT EXISTS current_benchmark_kpi STRING;

ALTER TABLE `looker-studio-pro-452620.landing.master_data_model_manual_package_edits_raw`
ADD COLUMN IF NOT EXISTS man_benchmark_kpi STRING;

ALTER TABLE `looker-studio-pro-452620.landing.master_data_model_manual_package_edits_raw`
ADD COLUMN IF NOT EXISTS current_benchmark_value FLOAT64;

ALTER TABLE `looker-studio-pro-452620.landing.master_data_model_manual_package_edits_raw`
ADD COLUMN IF NOT EXISTS man_benchmark_value FLOAT64;

ALTER TABLE `looker-studio-pro-452620.landing.master_data_model_manual_package_edits_raw`
ADD COLUMN IF NOT EXISTS manual_edit_at TIMESTAMP;

ALTER TABLE `looker-studio-pro-452620.landing.master_data_model_manual_package_edits_raw`
ADD COLUMN IF NOT EXISTS manual_edit_by STRING;

ALTER TABLE `looker-studio-pro-452620.landing.master_data_model_manual_package_edits_raw`
ADD COLUMN IF NOT EXISTS manual_edit_published_at TIMESTAMP;

CREATE TABLE IF NOT EXISTS `looker-studio-pro-452620.landing.master_data_model_manual_package_daily` (
  is_active BOOL,
  validation_status STRING,
  edit_id STRING,
  package_id STRING,
  date DATE,
  man_start_date DATE,
  man_end_date DATE,
  man_benchmark_kpi STRING,
  man_benchmark_value FLOAT64,
  man_daily_spend FLOAT64,
  man_daily_impressions FLOAT64,
  man_daily_planned_spend FLOAT64,
  man_daily_planned_impressions FLOAT64,
  man_daily_clicks FLOAT64,
  man_daily_video_plays FLOAT64,
  man_daily_video_comps FLOAT64,
  man_total_spend_doNotSum FLOAT64,
  man_total_impressions_doNotSum FLOAT64,
  man_total_planned_spend_doNotSum FLOAT64,
  man_total_planned_impressions_doNotSum FLOAT64,
  man_total_clicks_doNotSum FLOAT64,
  man_total_video_plays_doNotSum FLOAT64,
  man_total_video_comps_doNotSum FLOAT64,
  advertiser_name STRING,
  advertiser_short_name STRING,
  campaign_name STRING,
  campaign_friendly STRING,
  product_code STRING,
  product_name STRING,
  package_type STRING,
  package_name STRING,
  package_name_friendly STRING,
  ADIF_channel STRING,
  placement_id STRING,
  placement_name STRING,
  supplier_code STRING,
  supplier_name STRING,
  supplier_logo STRING,
  p_buy_type STRING,
  p_buy_category STRING,
  channel STRING,
  channel_raw STRING,
  channel_group STRING,
  media_name STRING,
  p_cost_method STRING,
  p_planned_amount_doNotSum FLOAT64,
  p_planned_impressions_doNotSum FLOAT64,
  p_planned_units_doNotSum FLOAT64,
  p_unit_type STRING,
  p_rate FLOAT64,
  edit_reason STRING,
  editor_email STRING,
  manual_edit_at TIMESTAMP,
  manual_edit_by STRING,
  manual_edit_published_at TIMESTAMP,
  source_sheet_url STRING,
  source_sheet_modified_time TIMESTAMP,
  loaded_at TIMESTAMP
)
OPTIONS (
  description = "Valid active manual package edits spread to daily package rows. Consumed by master_stg.data_model as top-priority man_ evidence."
);

ALTER TABLE `looker-studio-pro-452620.landing.master_data_model_manual_package_daily`
ADD COLUMN IF NOT EXISTS manual_edit_at TIMESTAMP;

ALTER TABLE `looker-studio-pro-452620.landing.master_data_model_manual_package_daily`
ADD COLUMN IF NOT EXISTS manual_edit_by STRING;

ALTER TABLE `looker-studio-pro-452620.landing.master_data_model_manual_package_daily`
ADD COLUMN IF NOT EXISTS manual_edit_published_at TIMESTAMP;

ALTER TABLE `looker-studio-pro-452620.landing.master_data_model_manual_package_daily`
ADD COLUMN IF NOT EXISTS man_benchmark_kpi STRING;

ALTER TABLE `looker-studio-pro-452620.landing.master_data_model_manual_package_daily`
ADD COLUMN IF NOT EXISTS man_benchmark_value FLOAT64;

-- Permanent ledger of accepted manual edits. The loader appends accepted rows
-- here before it replaces the disposable current raw snapshot.
CREATE TABLE IF NOT EXISTS `looker-studio-pro-452620.landing.master_data_model_manual_package_edits_history`
PARTITION BY DATE(history_recorded_at)
CLUSTER BY package_id, edit_id
OPTIONS (
  description = "Append-only loader history of accepted Manual Data Editor records. The current raw table is a replaceable snapshot; this table retains each accepted loader state for recovery and audit."
)
AS
SELECT
  CAST(NULL AS STRING) AS history_event_id,
  CAST(NULL AS STRING) AS history_event_type,
  CAST(NULL AS TIMESTAMP) AS history_recorded_at,
  CAST(NULL AS STRING) AS history_loader_run_id,
  CAST(NULL AS STRING) AS history_source,
  raw.*
FROM `looker-studio-pro-452620.landing.master_data_model_manual_package_edits_raw` AS raw
WHERE FALSE;

-- Seed the ledger once with the currently accepted raw snapshot. The raw
-- loaded_at timestamp remains the evidence for when that snapshot was made.
INSERT INTO `looker-studio-pro-452620.landing.master_data_model_manual_package_edits_history`
SELECT
  CONCAT(
    "manual-edit-history-initial-",
    REGEXP_REPLACE(COALESCE(raw.edit_id, "no-edit-id"), r"[^A-Za-z0-9_-]", "_"),
    "-",
    FORMAT_TIMESTAMP("%Y%m%dT%H%M%SZ", COALESCE(raw.loaded_at, CURRENT_TIMESTAMP())),
    "-",
    CAST(ROW_NUMBER() OVER (ORDER BY raw.package_id, raw.edit_id) AS STRING)
  ) AS history_event_id,
  "initial_backfill" AS history_event_type,
  CURRENT_TIMESTAMP() AS history_recorded_at,
  "initial_backfill" AS history_loader_run_id,
  "current_raw_snapshot" AS history_source,
  raw.*
FROM `looker-studio-pro-452620.landing.master_data_model_manual_package_edits_raw` AS raw
WHERE raw.is_active
  AND raw.validation_status = "valid"
  AND NOT EXISTS (
    SELECT 1
    FROM `looker-studio-pro-452620.landing.master_data_model_manual_package_edits_history`
    WHERE history_event_type = "initial_backfill"
  );
