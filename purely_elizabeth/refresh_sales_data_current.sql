-- Purpose: Rebuild the current Purely Elizabeth SPINS sales table from the
--          newest schema-approved CSV published under the GCS bigquery_ready prefix.
-- Reads:   gs://looker-studio-pro-452620-pe/bigquery_ready/*.csv.
-- Produces: PE.sales_data_current with all 22 source fields plus load lineage.
-- Schedule: Every 6 hours in US through BigQuery Scheduled Queries.
-- Safety:  Every assertion runs before CREATE OR REPLACE touches reporting data.


-- * SECTION [1]: EXTERNAL SOURCE CONTRACT

CREATE OR REPLACE EXTERNAL TABLE `looker-studio-pro-452620.PE.sales_data_bigquery_ready_external` (
  geography STRING,
  time_period STRING,
  time_period_end_date STRING,
  product_universe STRING,
  product_level STRING,
  category STRING,
  subcategory STRING,
  brand STRING,
  upc STRING,
  description STRING,
  flavor STRING,
  nfp_protein STRING,
  product_type STRING,
  packaging_type_primary STRING,
  size STRING,
  dollars STRING,
  units STRING,
  avg_percent_acv STRING,
  tdp STRING,
  number_of_stores_selling STRING,
  average_weekly_dollars_per_store_selling_per_item STRING,
  average_weekly_units_per_store_selling_per_item STRING
)
OPTIONS (
  format = 'CSV',
  uris = ['gs://looker-studio-pro-452620-pe/bigquery_ready/*.csv'],
  skip_leading_rows = 1,
  allow_quoted_newlines = TRUE,
  encoding = 'UTF-8',
  description = 'Schema-controlled Purely Elizabeth SPINS CSV snapshots published by the Gmail attachment Apps Script. Files remain external text until the scheduled validation and typed-table refresh succeeds.'
);


-- * SECTION [2]: APPROVED FILE SELECTION

CREATE TEMP TABLE ready_file_profiles AS
SELECT
  _FILE_NAME AS source_object,
  SAFE.PARSE_DATE(
    '%Y-%m-%d',
    REGEXP_EXTRACT(_FILE_NAME, r'/(\d{4}-\d{2}-\d{2})\.csv$')
  ) AS source_snapshot_date,
  COUNT(*) AS source_row_count,
  COUNTIF(
    SAFE.PARSE_DATE('%m/%d/%Y', NULLIF(TRIM(time_period_end_date), '')) IS NULL
  ) AS invalid_date_count,
  MIN(SAFE.PARSE_DATE('%m/%d/%Y', NULLIF(TRIM(time_period_end_date), '')))
    AS earliest_data_date,
  MAX(SAFE.PARSE_DATE('%m/%d/%Y', NULLIF(TRIM(time_period_end_date), '')))
    AS latest_data_date
FROM `looker-studio-pro-452620.PE.sales_data_bigquery_ready_external`
GROUP BY
  source_object,
  source_snapshot_date;

ASSERT (SELECT COUNT(*) FROM ready_file_profiles) > 0
  AS 'No BigQuery-ready Purely Elizabeth CSV was found in GCS';

ASSERT (
  SELECT COUNTIF(
    source_snapshot_date IS NULL
    OR invalid_date_count != 0
    OR source_snapshot_date != latest_data_date
  )
  FROM ready_file_profiles
) = 0 AS 'A BigQuery-ready file failed its filename or date contract';

CREATE TEMP TABLE latest_ready_file AS
SELECT
  source_object,
  source_snapshot_date,
  source_row_count
FROM ready_file_profiles
QUALIFY ROW_NUMBER() OVER (
  ORDER BY source_snapshot_date DESC, source_object DESC
) = 1;

CREATE TEMP TABLE latest_snapshot AS
SELECT
  source.*,
  source._FILE_NAME AS source_object,
  latest.source_snapshot_date
FROM `looker-studio-pro-452620.PE.sales_data_bigquery_ready_external` AS source
INNER JOIN latest_ready_file AS latest
  ON source._FILE_NAME = latest.source_object;


-- * SECTION [3]: FAIL-CLOSED ROW VALIDATION

ASSERT (
  SELECT COUNT(*)
  FROM latest_snapshot
) = (
  SELECT source_row_count
  FROM latest_ready_file
) AS 'Selected snapshot row count changed during external-table reading';

ASSERT (
  SELECT COUNTIF(
    NULLIF(TRIM(geography), '') IS NULL
    OR NULLIF(TRIM(product_level), '') IS NULL
    OR SAFE.PARSE_DATE('%m/%d/%Y', NULLIF(TRIM(time_period_end_date), '')) IS NULL
    OR SAFE_CAST(REPLACE(REPLACE(REPLACE(NULLIF(TRIM(dollars), ''), ',', ''), '$', ''), '%', '') AS FLOAT64) IS NULL
    OR SAFE_CAST(REPLACE(REPLACE(REPLACE(NULLIF(TRIM(units), ''), ',', ''), '$', ''), '%', '') AS FLOAT64) IS NULL
    OR SAFE_CAST(REPLACE(REPLACE(REPLACE(NULLIF(TRIM(avg_percent_acv), ''), ',', ''), '$', ''), '%', '') AS FLOAT64) IS NULL
    OR SAFE_CAST(REPLACE(REPLACE(REPLACE(NULLIF(TRIM(tdp), ''), ',', ''), '$', ''), '%', '') AS FLOAT64) IS NULL
    OR SAFE_CAST(REPLACE(REPLACE(REPLACE(NULLIF(TRIM(number_of_stores_selling), ''), ',', ''), '$', ''), '%', '') AS INT64) IS NULL
  )
  FROM latest_snapshot
) = 0 AS 'Required fields contain blanks or invalid typed values';

ASSERT (
  SELECT COUNTIF(
    (
      NULLIF(TRIM(average_weekly_dollars_per_store_selling_per_item), '') IS NOT NULL
      AND SAFE_CAST(
        REPLACE(REPLACE(REPLACE(
          NULLIF(TRIM(average_weekly_dollars_per_store_selling_per_item), ''),
          ',',
          ''
        ), '$', ''), '%', '')
        AS FLOAT64
      ) IS NULL
    )
    OR (
      NULLIF(TRIM(average_weekly_units_per_store_selling_per_item), '') IS NOT NULL
      AND SAFE_CAST(
        REPLACE(REPLACE(REPLACE(
          NULLIF(TRIM(average_weekly_units_per_store_selling_per_item), ''),
          ',',
          ''
        ), '$', ''), '%', '')
        AS FLOAT64
      ) IS NULL
    )
  )
  FROM latest_snapshot
) = 0 AS 'Optional weekly-velocity fields contain invalid numeric values';

ASSERT (
  SELECT COUNTIF(product_level NOT IN ('BRAND', 'UPC'))
  FROM latest_snapshot
) = 0 AS 'Unexpected Product Level values are present';

ASSERT (
  SELECT
    COUNTIF(product_level = 'BRAND') > 0
    AND COUNTIF(product_level = 'UPC') > 0
  FROM latest_snapshot
) AS 'Both BRAND and UPC rows are required';

ASSERT (
  SELECT COUNT(*)
  FROM (
    SELECT
      geography,
      time_period,
      time_period_end_date,
      product_universe,
      product_level,
      category,
      subcategory,
      brand,
      upc,
      description,
      flavor,
      nfp_protein,
      product_type,
      packaging_type_primary,
      size
    FROM latest_snapshot
    GROUP BY
      geography,
      time_period,
      time_period_end_date,
      product_universe,
      product_level,
      category,
      subcategory,
      brand,
      upc,
      description,
      flavor,
      nfp_protein,
      product_type,
      packaging_type_primary,
      size
    HAVING COUNT(*) > 1
  )
) = 0 AS 'Duplicate 15-field source dimension keys are present';


-- * SECTION [4]: VALIDATED CURRENT SNAPSHOT

CREATE OR REPLACE TABLE `looker-studio-pro-452620.PE.sales_data_current`
OPTIONS (
  description = 'Current Purely Elizabeth SPINS rolling snapshot. Rebuilt by the scheduled GCS validation query only after the newest bigquery_ready CSV passes its source, date, type, and uniqueness checks.',
  labels = [('pipeline', 'purely_elizabeth'), ('purpose', 'current_sales')]
)
AS
SELECT
  geography,
  time_period,
  SAFE.PARSE_DATE('%m/%d/%Y', NULLIF(TRIM(time_period_end_date), ''))
    AS time_period_end_date,
  product_universe,
  product_level,
  category,
  subcategory,
  brand,
  upc,
  description,
  flavor,
  nfp_protein,
  product_type,
  packaging_type_primary,
  size,
  SAFE_CAST(REPLACE(REPLACE(REPLACE(NULLIF(TRIM(dollars), ''), ',', ''), '$', ''), '%', '') AS FLOAT64)
    AS dollars,
  SAFE_CAST(REPLACE(REPLACE(REPLACE(NULLIF(TRIM(units), ''), ',', ''), '$', ''), '%', '') AS FLOAT64)
    AS units,
  SAFE_CAST(REPLACE(REPLACE(REPLACE(NULLIF(TRIM(avg_percent_acv), ''), ',', ''), '$', ''), '%', '') AS FLOAT64)
    AS avg_percent_acv,
  SAFE_CAST(REPLACE(REPLACE(REPLACE(NULLIF(TRIM(tdp), ''), ',', ''), '$', ''), '%', '') AS FLOAT64)
    AS tdp,
  SAFE_CAST(REPLACE(REPLACE(REPLACE(NULLIF(TRIM(number_of_stores_selling), ''), ',', ''), '$', ''), '%', '') AS INT64)
    AS number_of_stores_selling,
  SAFE_CAST(
    REPLACE(REPLACE(REPLACE(
      NULLIF(TRIM(average_weekly_dollars_per_store_selling_per_item), ''),
      ',',
      ''
    ), '$', ''), '%', '')
    AS FLOAT64
  ) AS average_weekly_dollars_per_store_selling_per_item,
  SAFE_CAST(
    REPLACE(REPLACE(REPLACE(
      NULLIF(TRIM(average_weekly_units_per_store_selling_per_item), ''),
      ',',
      ''
    ), '$', ''), '%', '')
    AS FLOAT64
  ) AS average_weekly_units_per_store_selling_per_item,
  source_object AS _source_object,
  source_snapshot_date AS _source_snapshot_date,
  CURRENT_TIMESTAMP() AS _loaded_at
FROM latest_snapshot;
