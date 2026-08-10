-- Purpose: Compare each Purely Elizabeth UPC with its subcategory and Brand.
-- Reads:   looker-studio-pro-452620.PE.sales_data_current.
-- Produces: One row per sales week, geography, and UPC.
-- Safety:  This query does not remove duplicates. The source week + geography
--          + UPC grain was directly verified before this production view.


-- * SECTION [1]: UPC SALES BASE ROWS

  -- Description: Keep only rows where the source product_level is UPC.

WITH upc_rows AS (
  SELECT
    time_period_end_date,
    geography,
    category,
    subcategory,
    upc,
    description,
    flavor,
    dollars,
    units,
    tdp
  FROM `looker-studio-pro-452620.PE.sales_data_current`
  WHERE brand = 'PURELY ELIZABETH'

    -- Excluding Brand rows prevents parent and child sales from being added.
    AND product_level = 'UPC'
),


-- * SECTION [2]: BRAND TOTAL

  -- Description: Retain dollars from rows where product_level is BRAND.

brand_level AS (
  SELECT
    time_period_end_date,
    geography,
    SUM(dollars) AS brand_dollars
  FROM `looker-studio-pro-452620.PE.sales_data_current`
  WHERE brand = 'PURELY ELIZABETH'
    AND product_level = 'BRAND'
  GROUP BY
    time_period_end_date,
    geography
),


-- * SECTION [3]: SUBCATEGORY TOTAL

  -- Description: Calculate subcategory dollars from the source UPC rows.

upc_rollups AS (
  SELECT
    upc_rows.*,
    SUM(dollars) OVER (
      PARTITION BY time_period_end_date, geography, subcategory
    ) AS subcategory_dollars
  FROM upc_rows
)


-- * SECTION [4]: UPC COMPARISONS

  -- Description: Compare UPC dollars with subcategory and Brand dollars.

SELECT
  upc_rollups.*,
  brand_level.brand_dollars,
  SAFE_DIVIDE(
    dollars,
    subcategory_dollars
  ) AS upc_share_of_subcategory,
  SAFE_DIVIDE(
    dollars,
    brand_level.brand_dollars
  ) AS upc_share_of_brand
FROM upc_rollups
LEFT JOIN brand_level
  USING (time_period_end_date, geography);
