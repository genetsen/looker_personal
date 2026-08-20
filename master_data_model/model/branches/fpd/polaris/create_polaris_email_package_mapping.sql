-- Create the central MIQ Polaris Email package mapping owner and add the five
-- approved MVP keys without deleting or overwriting mappings added later.

CREATE TABLE IF NOT EXISTS `looker-studio-pro-452620.landing.polaris_email_package_mapping` (
  partner STRING NOT NULL,
  ingestion_path STRING NOT NULL,
  client_id STRING NOT NULL,
  connection_id STRING NOT NULL,
  source_feed STRING NOT NULL,
  platform STRING NOT NULL,
  campaign_name STRING NOT NULL,
  ad_group_name STRING NOT NULL,
  package_id STRING NOT NULL,
  package_friendly_label STRING NOT NULL,
  is_active BOOL NOT NULL,
  created_at TIMESTAMP NOT NULL,
  updated_at TIMESTAMP NOT NULL,
  updated_by STRING NOT NULL
)
OPTIONS (
  description = 'Central source-key-to-Prisma-package mappings for MIQ delivery received through the Polaris Email ingestion path.'
);

MERGE `looker-studio-pro-452620.landing.polaris_email_package_mapping` AS target
USING (
  SELECT *
  FROM UNNEST([
    STRUCT('MIQ' AS partner, 'Polaris Email' AS ingestion_path, 'C70545844' AS client_id, '11694' AS connection_id, 'meta' AS source_feed, 'Facebook' AS platform, 'Awareness Campaign' AS campaign_name, 'ACR - Facebook' AS ad_group_name, 'P3HF7QB' AS package_id, 'Facebook Awareness' AS package_friendly_label),
    STRUCT('MIQ', 'Polaris Email', 'C70545844', '11694', 'meta', 'Facebook', 'Awareness Campaign', 'Interests - Facebook', 'P3HF7QB', 'Facebook Awareness'),
    STRUCT('MIQ', 'Polaris Email', 'C70545844', '11694', 'meta', 'Instagram', 'Awareness Campaign', 'ACR - Instagram', 'P3HF7T8', 'Instagram'),
    STRUCT('MIQ', 'Polaris Email', 'C70545844', '11694', 'meta', 'Instagram', 'Awareness Campaign', 'Interests - Instagram', 'P3HF7T8', 'Instagram'),
    STRUCT('MIQ', 'Polaris Email', 'C70545844', '11694', 'tiktok', 'TikTok', 'Purely Elizabeth - Awareness Q3', 'Interests', 'P3HF88Q', 'TikTok')
  ])
) AS seed
ON target.client_id = seed.client_id
  AND target.connection_id = seed.connection_id
  AND LOWER(TRIM(target.source_feed)) = LOWER(TRIM(seed.source_feed))
  AND LOWER(TRIM(target.platform)) = LOWER(TRIM(seed.platform))
  AND LOWER(TRIM(target.campaign_name)) = LOWER(TRIM(seed.campaign_name))
  AND LOWER(TRIM(target.ad_group_name)) = LOWER(TRIM(seed.ad_group_name))
WHEN NOT MATCHED THEN
  INSERT (
    partner, ingestion_path, client_id, connection_id, source_feed, platform,
    campaign_name, ad_group_name, package_id, package_friendly_label,
    is_active, created_at, updated_at, updated_by
  )
  VALUES (
    seed.partner, seed.ingestion_path, seed.client_id, seed.connection_id,
    seed.source_feed, seed.platform, seed.campaign_name, seed.ad_group_name,
    seed.package_id, seed.package_friendly_label,
    TRUE, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), SESSION_USER()
  );
