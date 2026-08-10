-- Purpose: Provide production weekly sales rows for category, subcategory,
--          and Brand reporting. Unlike the UPC comparison view, this view
--          contains summarized totals and does not contain individual UPC rows.
-- Reads:   looker-studio-pro-452620.PE.sales_data_current.
-- Produces: One row per sales week, geography, and product_level.
-- Safety:  Charts must filter to one product_level. Combining parent and
--          child levels would intentionally count the same sales more than once.


-- * SECTION [1]: UPC SALES BASE ROWS

  -- Description: Establish the UPC-only sales population used for comparable
  --              category and subcategory totals.

WITH upc_rows AS (
  SELECT
    time_period_end_date,
    geography,
    category,
    subcategory,
    dollars,
    units,
    tdp
  FROM `looker-studio-pro-452620.PE.sales_data_current`
  WHERE brand = 'PURELY ELIZABETH'
    AND product_level = 'UPC'
),


-- * SECTION [2]: SUBCATEGORY AND CATEGORY TOTALS

  -- Description: Calculate category and subcategory totals only from UPC rows.
  --              SPINS defines TDP as the sum of item distribution points, so
  --              UPC tdp is additive for these category and subcategory rows.

subcategories AS (
  SELECT
    time_period_end_date,
    geography,
    category,
    subcategory,
    SUM(dollars) AS dollars,
    SUM(units) AS units,
    SUM(tdp) AS tdp
  FROM upc_rows
  GROUP BY
    time_period_end_date,
    geography,
    category,
    subcategory
),

categories AS (
  SELECT
    time_period_end_date,
    geography,
    category,
    SUM(dollars) AS dollars,
    SUM(units) AS units,
    SUM(tdp) AS tdp
  FROM upc_rows
  GROUP BY
    time_period_end_date,
    geography,
    category
),


-- * SECTION [3]: BRAND TOTALS

  -- Description: Retain dollars, units, and tdp from the source rows where
  --              product_level is BRAND instead of rebuilding Brand from UPC.

brand_level AS (
  SELECT
    time_period_end_date,
    geography,
    SUM(dollars) AS dollars,
    SUM(units) AS units,
    SUM(tdp) AS tdp
  FROM `looker-studio-pro-452620.PE.sales_data_current`
  WHERE brand = 'PURELY ELIZABETH'
    AND product_level = 'BRAND'
  GROUP BY
    time_period_end_date,
    geography
)


-- * SECTION [4]: CHART ROWS

  -- Description: Stack source-named product levels into one chart-ready output.
  --              Select one product_level before summing dollars.

SELECT
  -- Filter product_level before summing dollars to avoid parent-child overlap.
  time_period_end_date,
  geography,
  'CATEGORY' AS product_level,
  category,
  CAST(NULL AS STRING) AS subcategory,
  dollars,
  units,
  tdp
FROM categories

UNION ALL

SELECT
  time_period_end_date,
  geography,
  'SUBCATEGORY' AS product_level,
  category,
  subcategory,
  dollars,
  units,
  tdp
FROM subcategories

UNION ALL

SELECT
  time_period_end_date,
  geography,
  'BRAND' AS product_level,
  CAST(NULL AS STRING) AS category,
  CAST(NULL AS STRING) AS subcategory,
  dollars,
  units,
  tdp
FROM brand_level
