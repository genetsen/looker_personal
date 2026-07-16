-- Add a Purely Elizabeth campaign mapping or update its product group and notes.
--
-- Safe use:
--   1. Replace the three EDIT values below.
--   2. Run the complete script in BigQuery Studio.
--   3. Confirm the final SELECT shows the intended row.

DECLARE campaign_name_to_save STRING DEFAULT 'EDIT: exact campaign name';
DECLARE product_group_to_save STRING DEFAULT 'EDIT: year-free product group';
DECLARE mapping_notes_to_save STRING DEFAULT 'EDIT: reason for this mapping';

ASSERT NOT STARTS_WITH(campaign_name_to_save, 'EDIT:')
  AS 'Replace campaign_name_to_save before running this query.';
ASSERT NOT STARTS_WITH(product_group_to_save, 'EDIT:')
  AS 'Replace product_group_to_save before running this query.';
ASSERT NOT REGEXP_CONTAINS(product_group_to_save, r'[0-9]{4}')
  AS 'Use a year-free product group, such as PROTEIN GRANOLA.';

MERGE
  `looker-studio-pro-452620.PE.purely_elizabeth_campaign_product_mapping` AS existing
USING (
  SELECT
    'Purely Elizabeth' AS advertiser_name,
    campaign_name_to_save AS campaign_name,
    product_group_to_save AS product_group,
    mapping_notes_to_save AS mapping_notes
) AS requested
ON
  existing.advertiser_name = requested.advertiser_name
  AND existing.campaign_name = requested.campaign_name
WHEN MATCHED THEN
  UPDATE SET
    product_group = requested.product_group,
    mapping_notes = requested.mapping_notes,
    mapping_updated_at = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN
  INSERT (
    advertiser_name,
    campaign_name,
    product_group,
    mapping_notes,
    mapping_updated_at
  )
  VALUES (
    requested.advertiser_name,
    requested.campaign_name,
    requested.product_group,
    requested.mapping_notes,
    CURRENT_TIMESTAMP()
  );

SELECT *
FROM `looker-studio-pro-452620.PE.purely_elizabeth_campaign_product_mapping`
WHERE
  advertiser_name = 'Purely Elizabeth'
  AND campaign_name = campaign_name_to_save;
