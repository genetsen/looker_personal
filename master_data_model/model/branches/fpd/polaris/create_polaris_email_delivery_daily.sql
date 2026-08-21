-- Create the normalized MIQ Polaris Email history table. The guarded loader
-- upserts source rows only after source, mapping, row, and warehouse checks.

CREATE TABLE IF NOT EXISTS `looker-studio-pro-452620.landing.polaris_email_delivery_daily` (
  partner STRING NOT NULL,
  ingestion_path STRING NOT NULL,
  client_id STRING NOT NULL,
  connection_id STRING NOT NULL,
  source_object_uri STRING NOT NULL,
  source_feed STRING NOT NULL,
  source_row_number INT64 NOT NULL,
  platform STRING NOT NULL,
  campaign_name STRING NOT NULL,
  ad_group_name STRING NOT NULL,
  ad_name STRING NOT NULL,
  date DATE NOT NULL,
  spend NUMERIC,
  impressions INT64,
  clicks INT64,
  video_views INT64,
  video_completions INT64,
  raw_date DATE,
  raw_spend NUMERIC,
  raw_impressions INT64,
  raw_clicks INT64,
  raw_video_views INT64,
  raw_video_completions INT64,
  package_id STRING NOT NULL,
  package_friendly_label STRING NOT NULL,
  mapping_status STRING NOT NULL,
  natural_row_key STRING NOT NULL,
  loaded_at TIMESTAMP NOT NULL
)
PARTITION BY date
CLUSTER BY package_id, platform, source_feed
OPTIONS (
  description = 'Normalized daily creative-level MIQ delivery received through Polaris Email. New source objects update overlapping natural keys and append new keys; raw FPD landing evidence remains separate.'
);
