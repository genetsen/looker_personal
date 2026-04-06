WITH dcm_prepped AS (
  SELECT
    dcm.*,
    dcm.package_roadblock LIKE '%MASS%' AS is_mass_row,
    LOWER(TRIM(dcm.campaign)) AS campaign_norm,
    LOWER(TRIM(dcm.placement_id)) AS placement_id_norm,
    REGEXP_REPLACE(LOWER(TRIM(dcm.creative)), r'px', '') AS creative_norm,
    REGEXP_REPLACE(
      REGEXP_REPLACE(
        REGEXP_REPLACE(LOWER(TRIM(dcm.creative)), r'px', ''),
        r'\s+',
        ''
      ),
      r'[_-]?\d+x\d+$',
      ''
    ) AS creative_loose_norm,
    REGEXP_REPLACE(
      REGEXP_REPLACE(
        REGEXP_REPLACE(
          REGEXP_REPLACE(LOWER(TRIM(dcm.creative)), r'px', ''),
          r'\s+',
          ''
        ),
        r'[_.-]?(jpg|jpeg|png|gif|webp|html5|mp4)$',
        ''
      ),
      r'[_-]?\d+x\d+$',
      ''
    ) AS creative_extless_norm
  FROM `looker-studio-pro-452620.final_views.dcm` AS dcm
),

utm_exact AS (
  SELECT * EXCEPT(rn_exact)
  FROM (
    SELECT
      utm.*,
      ROW_NUMBER() OVER (
        PARTITION BY utm.placement_id, utm.creative_assignment
        ORDER BY utm.last_updated DESC, utm.placement_end_date DESC, utm.start DESC
      ) AS rn_exact
    FROM `looker-studio-pro-452620.final_views.utms_view` AS utm
  )
  WHERE rn_exact = 1
),

utm_norm AS (
  SELECT * EXCEPT(rn_norm)
  FROM (
    SELECT
      utm.*,
      LOWER(TRIM(utm.campaign)) AS campaign_norm,
      LOWER(TRIM(utm.placement_id)) AS placement_id_norm,
      REGEXP_REPLACE(LOWER(TRIM(utm.creative_assignment)), r'px', '') AS creative_norm,
      ROW_NUMBER() OVER (
        PARTITION BY
          LOWER(TRIM(utm.campaign)),
          LOWER(TRIM(utm.placement_id)),
          REGEXP_REPLACE(LOWER(TRIM(utm.creative_assignment)), r'px', '')
        ORDER BY utm.last_updated DESC, utm.placement_end_date DESC, utm.start DESC
      ) AS rn_norm
    FROM `looker-studio-pro-452620.final_views.utms_view` AS utm
  )
  WHERE rn_norm = 1
),

utm_loose AS (
  SELECT * EXCEPT(rn_loose)
  FROM (
    SELECT
      utm.*,
      LOWER(TRIM(utm.campaign)) AS campaign_norm,
      LOWER(TRIM(utm.placement_id)) AS placement_id_norm,
      REGEXP_REPLACE(
        REGEXP_REPLACE(
          REGEXP_REPLACE(LOWER(TRIM(utm.creative_assignment)), r'px', ''),
          r'\s+',
          ''
        ),
        r'[_-]?\d+x\d+$',
        ''
      ) AS creative_loose_norm,
      ROW_NUMBER() OVER (
        PARTITION BY
          LOWER(TRIM(utm.campaign)),
          LOWER(TRIM(utm.placement_id)),
          REGEXP_REPLACE(
            REGEXP_REPLACE(
              REGEXP_REPLACE(LOWER(TRIM(utm.creative_assignment)), r'px', ''),
              r'\s+',
              ''
            ),
            r'[_-]?\d+x\d+$',
            ''
          )
        ORDER BY utm.last_updated DESC, utm.placement_end_date DESC, utm.start DESC
      ) AS rn_loose
    FROM `looker-studio-pro-452620.final_views.utms_view` AS utm
  )
  WHERE rn_loose = 1
),

utm_extless AS (
  SELECT * EXCEPT(rn_extless)
  FROM (
    SELECT
      utm.*,
      LOWER(TRIM(utm.campaign)) AS campaign_norm,
      LOWER(TRIM(utm.placement_id)) AS placement_id_norm,
      REGEXP_REPLACE(
        REGEXP_REPLACE(
          REGEXP_REPLACE(
            REGEXP_REPLACE(LOWER(TRIM(utm.creative_assignment)), r'px', ''),
            r'\s+',
            ''
          ),
          r'[_.-]?(jpg|jpeg|png|gif|webp|html5|mp4)$',
          ''
        ),
        r'[_-]?\d+x\d+$',
        ''
      ) AS creative_extless_norm,
      ROW_NUMBER() OVER (
        PARTITION BY
          LOWER(TRIM(utm.campaign)),
          LOWER(TRIM(utm.placement_id)),
          REGEXP_REPLACE(
            REGEXP_REPLACE(
              REGEXP_REPLACE(
                REGEXP_REPLACE(LOWER(TRIM(utm.creative_assignment)), r'px', ''),
                r'\s+',
                ''
              ),
              r'[_.-]?(jpg|jpeg|png|gif|webp|html5|mp4)$',
              ''
            ),
            r'[_-]?\d+x\d+$',
            ''
          )
        ORDER BY utm.last_updated DESC, utm.placement_end_date DESC, utm.start DESC
      ) AS rn_extless
    FROM `looker-studio-pro-452620.final_views.utms_view` AS utm
  )
  WHERE rn_extless = 1
),

utm_by_campaign_placement AS (
  SELECT DISTINCT
    LOWER(TRIM(campaign)) AS campaign_norm,
    LOWER(TRIM(placement_id)) AS placement_id_norm
  FROM `looker-studio-pro-452620.final_views.utms_view`
),

joined_four_pass AS (
  SELECT
    dcm.date,
    dcm.campaign,
    dcm.package_roadblock,
    dcm.placement_id,
    dcm.creative,
    dcm.impressions,
    CASE
      WHEN utm_exact.placement_id IS NOT NULL THEN 'exact'
      WHEN utm_norm.placement_id IS NOT NULL THEN 'normalized'
      WHEN utm_loose.placement_id IS NOT NULL THEN 'loose_norm'
      WHEN utm_extless.placement_id IS NOT NULL THEN 'extless_norm'
      ELSE 'unmatched'
    END AS match_type
  FROM dcm_prepped AS dcm
  LEFT JOIN utm_exact
    ON utm_exact.placement_id = dcm.placement_id
   AND utm_exact.creative_assignment = dcm.creative
  LEFT JOIN utm_norm
    ON utm_exact.placement_id IS NULL
   AND dcm.is_mass_row
   AND utm_norm.campaign_norm = dcm.campaign_norm
   AND utm_norm.placement_id_norm = dcm.placement_id_norm
   AND utm_norm.creative_norm = dcm.creative_norm
  LEFT JOIN utm_loose
    ON utm_exact.placement_id IS NULL
   AND utm_norm.placement_id IS NULL
   AND dcm.is_mass_row
   AND utm_loose.campaign_norm = dcm.campaign_norm
   AND utm_loose.placement_id_norm = dcm.placement_id_norm
   AND utm_loose.creative_loose_norm = dcm.creative_loose_norm
  LEFT JOIN utm_extless
    ON utm_exact.placement_id IS NULL
   AND utm_norm.placement_id IS NULL
   AND utm_loose.placement_id IS NULL
   AND dcm.is_mass_row
   AND utm_extless.campaign_norm = dcm.campaign_norm
   AND utm_extless.placement_id_norm = dcm.placement_id_norm
   AND utm_extless.creative_extless_norm = dcm.creative_extless_norm
),

ranked_four_pass AS (
  SELECT
    joined_four_pass.*,
    ROW_NUMBER() OVER (
      PARTITION BY TO_JSON_STRING(joined_four_pass)
      ORDER BY placement_id
    ) AS rn
  FROM joined_four_pass
),

unmatched AS (
  SELECT * EXCEPT(rn)
  FROM ranked_four_pass
  WHERE rn = 1
    AND date >= DATE '2025-01-01'
    AND package_roadblock LIKE '%MASS%'
    AND impressions > 10
    AND match_type = 'unmatched'
)

SELECT
  u.campaign,
  u.package_roadblock,
  u.placement_id,
  u.creative,
  CASE
    WHEN ucp.placement_id_norm IS NOT NULL THEN 'placement_exists_same_campaign'
    ELSE 'no_placement_in_utms_same_campaign'
  END AS exception_type,
  ARRAY_AGG(DISTINCT utm.creative_assignment IGNORE NULLS ORDER BY utm.creative_assignment LIMIT 10) AS candidate_utm_creatives,
  COUNT(DISTINCT utm.creative_assignment) AS candidate_utm_creative_count,
  COUNT(*) AS row_count,
  SUM(u.impressions) AS unmatched_impressions
FROM unmatched AS u
LEFT JOIN utm_by_campaign_placement AS ucp
  ON ucp.campaign_norm = LOWER(TRIM(u.campaign))
 AND ucp.placement_id_norm = LOWER(TRIM(u.placement_id))
LEFT JOIN `looker-studio-pro-452620.final_views.utms_view` AS utm
  ON LOWER(TRIM(utm.campaign)) = LOWER(TRIM(u.campaign))
 AND LOWER(TRIM(utm.placement_id)) = LOWER(TRIM(u.placement_id))
GROUP BY 1, 2, 3, 4, 5
ORDER BY unmatched_impressions DESC, campaign, placement_id, creative;
