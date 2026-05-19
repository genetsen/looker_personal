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
  current_row_count INT64,
  current_first_date DATE,
  current_last_date DATE,
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

CREATE TABLE IF NOT EXISTS `looker-studio-pro-452620.landing.master_data_model_manual_package_daily` (
  is_active BOOL,
  validation_status STRING,
  edit_id STRING,
  package_id STRING,
  date DATE,
  man_start_date DATE,
  man_end_date DATE,
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
  source_sheet_url STRING,
  source_sheet_modified_time TIMESTAMP,
  loaded_at TIMESTAMP
)
OPTIONS (
  description = "Valid active manual package edits spread to daily package rows. Consumed by master_stg.data_model as top-priority man_ evidence."
);
