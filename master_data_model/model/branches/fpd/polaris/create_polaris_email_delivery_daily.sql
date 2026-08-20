-- Create the normalized MIQ Polaris Email snapshot table. The guarded loader
-- replaces its contents only after source, mapping, row, and warehouse checks.

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
  raw_date STRING,
  raw_spend STRING,
  raw_impressions STRING,
  raw_clicks STRING,
  raw_video_views STRING,
  raw_video_completions STRING,
  package_id STRING NOT NULL,
  package_friendly_label STRING NOT NULL,
  mapping_status STRING NOT NULL,
  natural_row_key STRING NOT NULL,
  loaded_at TIMESTAMP NOT NULL
)
PARTITION BY date
CLUSTER BY package_id, platform, source_feed
OPTIONS (
  description = 'Normalized daily creative-level MIQ delivery received through Polaris Email. This is a guarded full-snapshot landing table; raw FPD landing evidence remains separate.'
);
