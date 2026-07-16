-- Build the controlled campaign-to-product mapping used by Purely Elizabeth weekly reporting.
--
-- Purpose:
--   Preserve explicit campaign inclusion and product classification decisions.
--   Every valid row is included; campaigns absent from this table are excluded.
--
-- Output:
--   One row per advertiser and exact campaign name in
--   PE.purely_elizabeth_campaign_product_mapping.
--
-- Safe modification:
--   Run this builder to create or migrate the table structure. It never inserts, updates,
--   or restores mapping rows. Use the separate upsert and delete queries for row maintenance.

CREATE TABLE IF NOT EXISTS
  `looker-studio-pro-452620.PE.purely_elizabeth_campaign_product_mapping` (
    advertiser_name STRING,
    campaign_name STRING,
    product_group STRING,
    mapping_notes STRING,
    mapping_updated_at TIMESTAMP
  )
OPTIONS (
  description = 'Purely Elizabeth campaign-to-product reporting mappings. Every valid row is included in weekly reporting; campaigns absent from this table are excluded. Rows are preserved unless intentionally updated or removed.'
);

-- Migrate the earlier flag-based table without replacing its existing rows.
ALTER TABLE
  `looker-studio-pro-452620.PE.purely_elizabeth_campaign_product_mapping`
DROP COLUMN IF EXISTS include_flag;

ALTER TABLE
  `looker-studio-pro-452620.PE.purely_elizabeth_campaign_product_mapping`
SET OPTIONS (
  description = 'Purely Elizabeth campaign-to-product reporting mappings. Every valid row is included in weekly reporting; campaigns absent from this table are excluded. Rows change only through intentional maintenance queries.'
  );
