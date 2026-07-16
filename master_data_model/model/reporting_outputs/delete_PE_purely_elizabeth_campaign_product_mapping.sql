-- Remove one Purely Elizabeth campaign from weekly reporting.
--
-- Safe use:
--   1. Replace the EDIT value below with the exact campaign name.
--   2. Run the complete script in BigQuery Studio.
--   3. Confirm the final SELECT returns zero rows.

DECLARE campaign_name_to_remove STRING DEFAULT 'EDIT: exact campaign name';

ASSERT NOT STARTS_WITH(campaign_name_to_remove, 'EDIT:')
  AS 'Replace campaign_name_to_remove before running this query.';

DELETE FROM
  `looker-studio-pro-452620.PE.purely_elizabeth_campaign_product_mapping`
WHERE
  advertiser_name = 'Purely Elizabeth'
  AND campaign_name = campaign_name_to_remove;

SELECT *
FROM `looker-studio-pro-452620.PE.purely_elizabeth_campaign_product_mapping`
WHERE
  advertiser_name = 'Purely Elizabeth'
  AND campaign_name = campaign_name_to_remove;
