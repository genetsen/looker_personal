-- Candidate-only creative mapping projection for SQL Change Guard.
-- Reads the live V3 table and canonical mapping table, then reapplies the
-- mapping from the preserved raw name. This is intentionally idempotent so it
-- remains a valid candidate after a concurrent/manual V3 refresh.
WITH creative_mapping AS (
  SELECT
    advertiser,
    match_scope,
    initiative,
    source_creative_name,
    mapped_creative_name
  FROM `looker-studio-pro-452620.master_stg.creative_mapping`
)
SELECT
  -- QA OBJECT NOTE: Isolated candidate for the V3 creative-name mapping.
  -- Safe to delete after Guard passes and production is verified.
  candidate.* REPLACE (
    COALESCE(
      tactic_map.mapped_creative_name,
      creative_map.mapped_creative_name,
      candidate._creative_name_raw
    ) AS _creative_name
  )
FROM `looker-studio-pro-452620.master_stg.data_model_v3` AS candidate
LEFT JOIN creative_mapping AS tactic_map
  ON tactic_map.match_scope = 'tactic_creative'
 AND LOWER(TRIM(candidate._advertiser)) = LOWER(TRIM(tactic_map.advertiser))
 AND LOWER(TRIM(candidate.initiative)) = LOWER(TRIM(tactic_map.initiative))
 AND NULLIF(LOWER(TRIM(candidate._creative_name_raw)), '') IS NOT DISTINCT FROM
     NULLIF(LOWER(TRIM(tactic_map.source_creative_name)), '')
LEFT JOIN creative_mapping AS creative_map
  ON creative_map.match_scope = 'creative'
 AND LOWER(TRIM(candidate._advertiser)) = LOWER(TRIM(creative_map.advertiser))
 AND NULLIF(LOWER(TRIM(candidate._creative_name_raw)), '') IS NOT DISTINCT FROM
     NULLIF(LOWER(TRIM(creative_map.source_creative_name)), '')
