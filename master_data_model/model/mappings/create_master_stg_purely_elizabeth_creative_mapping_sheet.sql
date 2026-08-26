-- Create a read-only BigQuery external table and clean companion view over the
-- Purely Elizabeth Creative Mapping Google Sheet tab.
--
-- The source Sheet remains authoritative and is never edited by this SQL.
-- All columns intentionally remain STRING because the live Sheet mixes numbers,
-- percentages, descriptive benchmark text, dashes, errors, and blanks.

CREATE OR REPLACE EXTERNAL TABLE
  `looker-studio-pro-452620.master_stg.purely_elizabeth_creative_mapping_sheet_raw` (
    supplier_code STRING,
    initiative STRING,
    creative_name STRING,
    impressions_sum_through_8_23 STRING,
    spend_sum_through_8_23 STRING,
    friendly_creative_name STRING,
    ctr_bm_do_not_use STRING,
    bm_ctr_range_min STRING,
    bm_ctr_range_max STRING,
    bm_social_er_bm STRING,
    vcr_bm_do_not_use STRING,
    bm_vcr_range_min STRING,
    bm_vcr_range_max STRING
  )
OPTIONS (
  format = 'GOOGLE_SHEETS',
  uris = [
    'https://docs.google.com/spreadsheets/d/15LXvL0_DRY0GBDGN_omx2p5Ast4BCptelE463bvj8Bk'
  ],
  sheet_range = 'Creative Mapping!A:M',
  skip_leading_rows = 1,
  description = 'Raw read-only live link to the Purely Elizabeth Creative Mapping Google Sheet tab. Includes blank rows from the Sheet grid; all source cells remain STRING. Query the companion purely_elizabeth_creative_mapping_sheet view for populated source rows.'
);

CREATE OR REPLACE VIEW
  `looker-studio-pro-452620.master_stg.purely_elizabeth_creative_mapping_sheet`
OPTIONS (
  description = 'Clean live view of populated rows from the Purely Elizabeth Creative Mapping Google Sheet. Supplier code identifies a real source row; all source fields remain STRING. This view does not apply mapping logic or refresh V3.'
)
AS
SELECT
  -- LIVE VIEW NOTE: Filters only unused blank Sheet-grid rows. Every populated
  -- mapping or benchmark row remains visible, including blank creative names.
  *
FROM
  `looker-studio-pro-452620.master_stg.purely_elizabeth_creative_mapping_sheet_raw`
WHERE
  NULLIF(TRIM(supplier_code), '') IS NOT NULL;
