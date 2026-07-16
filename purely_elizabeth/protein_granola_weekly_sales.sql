-- Purpose: Define weekly Purely Elizabeth Protein Granola sales for reporting.
-- Reads: PE.sales_data_260709.
-- Produces: One row per week combining MULO and Natural Expanded for three approved UPCs.
-- Safety: This file is a SELECT-only view definition. Validate it with the paired QA manifest
-- before deploying or replacing the live view.

SELECT
  time_period_end_date,
  ROUND(SUM(dollars), 2) AS total_dollar_sales,
  ROUND(SUM(tdp), 1) AS total_tdp
FROM `looker-studio-pro-452620.PE.sales_data_260709`
WHERE geography IN (
    'TOTAL US - MULO',
    'TOTAL US - NATURAL EXPANDED CHANNEL'
  )
  AND brand = 'PURELY ELIZABETH'
  AND subcategory = 'SS GRANOLA & MUESLI'
  AND product_level = 'UPC'
  AND upc IN (
    '08-10589-03260',
    '08-10589-03261',
    '08-10589-03259'
  )
GROUP BY time_period_end_date
ORDER BY time_period_end_date;
