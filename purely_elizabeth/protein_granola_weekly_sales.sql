-- Purpose: Define weekly Purely Elizabeth Total Brand, Brand Granola, and Protein Granola sales.
-- Reads: PE.sales_data_current.
-- Produces: One row per week combining MULO and Natural Expanded for three named sales comparisons.
-- Safety: Validate with the paired QA manifest before replacing the live view.

SELECT
  time_period_end_date,
  ROUND(SUM(IF(
    product_level = 'BRAND'
    AND category IN ('SHELF STABLE COLD CEREALS', 'SHELF STABLE HOT CEREALS'),
    dollars,
    0
  )), 2) AS total_brand_sales_dollars,
  ROUND(SUM(IF(
    product_level = 'BRAND'
    AND category IN ('SHELF STABLE COLD CEREALS', 'SHELF STABLE HOT CEREALS'),
    tdp,
    0
  )), 1) AS total_brand_tdp,
  ROUND(SUM(IF(
    product_level = 'BRAND' AND subcategory = 'SS GRANOLA & MUESLI',
    dollars,
    0
  )), 2) AS total_brand_granola_sales_dollars,
  ROUND(SUM(IF(
    product_level = 'BRAND' AND subcategory = 'SS GRANOLA & MUESLI',
    tdp,
    0
  )), 1) AS total_brand_granola_tdp,
  ROUND(SUM(IF(
    product_level = 'UPC'
    AND subcategory = 'SS GRANOLA & MUESLI'
    AND upc IN ('08-10589-03260', '08-10589-03261', '08-10589-03259'),
    dollars,
    0
  )), 2) AS protein_granola_sales_dollars,
  ROUND(SUM(IF(
    product_level = 'UPC'
    AND subcategory = 'SS GRANOLA & MUESLI'
    AND upc IN ('08-10589-03260', '08-10589-03261', '08-10589-03259'),
    tdp,
    0
  )), 1) AS protein_granola_tdp
FROM `looker-studio-pro-452620.PE.sales_data_current`
WHERE geography IN ('TOTAL US - MULO', 'TOTAL US - NATURAL EXPANDED CHANNEL')
  AND brand = 'PURELY ELIZABETH'
GROUP BY time_period_end_date
ORDER BY time_period_end_date;
