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

joined_two_pass AS (
  SELECT
    dcm.* EXCEPT(
      is_mass_row,
      campaign_norm,
      placement_id_norm,
      creative_norm,
      creative_loose_norm,
      creative_extless_norm
    ),
    CONCAT(dcm.placement_id, ' || ', dcm.creative) AS utm_key,
    COALESCE(utm_exact.utm_source, utm_norm.utm_source) AS utm_source,
    COALESCE(utm_exact.ad_name, utm_norm.ad_name) AS utm_ad_name,
    COALESCE(utm_exact.placement_name, utm_norm.placement_name) AS placement_name,
    COALESCE(utm_exact.placement_id, utm_norm.placement_id) AS utm_placement_id,
    COALESCE(utm_exact.utm_campaign, utm_norm.utm_campaign) AS utm_campaign,
    COALESCE(utm_exact.utm_medium, utm_norm.utm_medium) AS utm_medium,
    COALESCE(utm_exact.utm_content, utm_norm.utm_content) AS utm_content,
    COALESCE(utm_exact.utm_term, utm_norm.utm_term) AS utm_term,
    COALESCE(utm_exact.creative_assignment, utm_norm.creative_assignment) AS utm_creative_assignment,
    CONCAT(
      COALESCE(utm_exact.placement_id, utm_norm.placement_id),
      ' || ',
      COALESCE(utm_exact.creative_assignment, utm_norm.creative_assignment)
    ) AS utm_utm_key,
    CASE
      WHEN utm_exact.placement_id IS NOT NULL THEN 'exact'
      WHEN utm_norm.placement_id IS NOT NULL THEN 'normalized'
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
),

joined_three_pass AS (
  SELECT
    dcm.* EXCEPT(
      is_mass_row,
      campaign_norm,
      placement_id_norm,
      creative_norm,
      creative_loose_norm,
      creative_extless_norm
    ),
    CONCAT(dcm.placement_id, ' || ', dcm.creative) AS utm_key,
    COALESCE(utm_exact.utm_source, utm_norm.utm_source, utm_loose.utm_source) AS utm_source,
    COALESCE(utm_exact.ad_name, utm_norm.ad_name, utm_loose.ad_name) AS utm_ad_name,
    COALESCE(utm_exact.placement_name, utm_norm.placement_name, utm_loose.placement_name) AS placement_name,
    COALESCE(utm_exact.placement_id, utm_norm.placement_id, utm_loose.placement_id) AS utm_placement_id,
    COALESCE(utm_exact.utm_campaign, utm_norm.utm_campaign, utm_loose.utm_campaign) AS utm_campaign,
    COALESCE(utm_exact.utm_medium, utm_norm.utm_medium, utm_loose.utm_medium) AS utm_medium,
    COALESCE(utm_exact.utm_content, utm_norm.utm_content, utm_loose.utm_content) AS utm_content,
    COALESCE(utm_exact.utm_term, utm_norm.utm_term, utm_loose.utm_term) AS utm_term,
    COALESCE(
      utm_exact.creative_assignment,
      utm_norm.creative_assignment,
      utm_loose.creative_assignment
    ) AS utm_creative_assignment,
    CONCAT(
      COALESCE(utm_exact.placement_id, utm_norm.placement_id, utm_loose.placement_id),
      ' || ',
      COALESCE(
        utm_exact.creative_assignment,
        utm_norm.creative_assignment,
        utm_loose.creative_assignment
      )
    ) AS utm_utm_key,
    CASE
      WHEN utm_exact.placement_id IS NOT NULL THEN 'exact'
      WHEN utm_norm.placement_id IS NOT NULL THEN 'normalized'
      WHEN utm_loose.placement_id IS NOT NULL THEN 'loose_norm'
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
),

joined_four_pass AS (
  SELECT
    dcm.* EXCEPT(
      is_mass_row,
      campaign_norm,
      placement_id_norm,
      creative_norm,
      creative_loose_norm,
      creative_extless_norm
    ),
    CONCAT(dcm.placement_id, ' || ', dcm.creative) AS utm_key,
    COALESCE(
      utm_exact.utm_source,
      utm_norm.utm_source,
      utm_loose.utm_source,
      utm_extless.utm_source
    ) AS utm_source,
    COALESCE(
      utm_exact.ad_name,
      utm_norm.ad_name,
      utm_loose.ad_name,
      utm_extless.ad_name
    ) AS utm_ad_name,
    COALESCE(
      utm_exact.placement_name,
      utm_norm.placement_name,
      utm_loose.placement_name,
      utm_extless.placement_name
    ) AS placement_name,
    COALESCE(
      utm_exact.placement_id,
      utm_norm.placement_id,
      utm_loose.placement_id,
      utm_extless.placement_id
    ) AS utm_placement_id,
    COALESCE(
      utm_exact.utm_campaign,
      utm_norm.utm_campaign,
      utm_loose.utm_campaign,
      utm_extless.utm_campaign
    ) AS utm_campaign,
    COALESCE(
      utm_exact.utm_medium,
      utm_norm.utm_medium,
      utm_loose.utm_medium,
      utm_extless.utm_medium
    ) AS utm_medium,
    COALESCE(
      utm_exact.utm_content,
      utm_norm.utm_content,
      utm_loose.utm_content,
      utm_extless.utm_content
    ) AS utm_content,
    COALESCE(
      utm_exact.utm_term,
      utm_norm.utm_term,
      utm_loose.utm_term,
      utm_extless.utm_term
    ) AS utm_term,
    COALESCE(
      utm_exact.creative_assignment,
      utm_norm.creative_assignment,
      utm_loose.creative_assignment,
      utm_extless.creative_assignment
    ) AS utm_creative_assignment,
    CONCAT(
      COALESCE(
        utm_exact.placement_id,
        utm_norm.placement_id,
        utm_loose.placement_id,
        utm_extless.placement_id
      ),
      ' || ',
      COALESCE(
        utm_exact.creative_assignment,
        utm_norm.creative_assignment,
        utm_loose.creative_assignment,
        utm_extless.creative_assignment
      )
    ) AS utm_utm_key,
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

ranked_two_pass AS (
  SELECT
    joined_two_pass.*,
    ROW_NUMBER() OVER (
      PARTITION BY TO_JSON_STRING(joined_two_pass)
      ORDER BY placement_id
    ) AS rn
  FROM joined_two_pass
),

ranked_three_pass AS (
  SELECT
    joined_three_pass.*,
    ROW_NUMBER() OVER (
      PARTITION BY TO_JSON_STRING(joined_three_pass)
      ORDER BY placement_id
    ) AS rn
  FROM joined_three_pass
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

final_two_pass AS (
  SELECT * EXCEPT(rn)
  FROM ranked_two_pass
  WHERE rn = 1
    AND date >= DATE '2025-01-01'
    AND package_roadblock LIKE '%MASS%'
    AND impressions > 10
),

final_three_pass AS (
  SELECT * EXCEPT(rn)
  FROM ranked_three_pass
  WHERE rn = 1
    AND date >= DATE '2025-01-01'
    AND package_roadblock LIKE '%MASS%'
    AND impressions > 10
),

final_four_pass AS (
  SELECT * EXCEPT(rn)
  FROM ranked_four_pass
  WHERE rn = 1
    AND date >= DATE '2025-01-01'
    AND package_roadblock LIKE '%MASS%'
    AND impressions > 10
),

summary AS (
  SELECT
    'two_pass_baseline' AS validation_stage,
    COUNT(*) AS row_count,
    COUNTIF(match_type = 'exact') AS exact_row_count,
    COUNTIF(match_type = 'normalized') AS normalized_row_count,
    0 AS loose_norm_row_count,
    0 AS extless_norm_row_count,
    COUNTIF(match_type = 'unmatched') AS unmatched_row_count,
    COUNTIF(placement_name IS NULL OR TRIM(placement_name) = '') AS blank_placement_name_row_count,
    COUNTIF(utm_source IS NULL OR TRIM(utm_source) = '') AS blank_utm_source_row_count,
    COUNTIF(utm_campaign IS NULL OR TRIM(utm_campaign) = '') AS blank_utm_campaign_row_count,
    COUNTIF(utm_medium IS NULL OR TRIM(utm_medium) = '') AS blank_utm_medium_row_count,
    COUNTIF(utm_content IS NULL OR TRIM(utm_content) = '') AS blank_utm_content_row_count,
    COUNTIF(utm_term IS NULL OR TRIM(utm_term) = '') AS blank_utm_term_row_count,
    SUM(impressions) AS total_impressions,
    SUM(IF(match_type = 'unmatched', impressions, 0)) AS unmatched_impressions
  FROM final_two_pass

  UNION ALL

  SELECT
    'three_pass_loose_norm' AS validation_stage,
    COUNT(*) AS row_count,
    COUNTIF(match_type = 'exact') AS exact_row_count,
    COUNTIF(match_type = 'normalized') AS normalized_row_count,
    COUNTIF(match_type = 'loose_norm') AS loose_norm_row_count,
    0 AS extless_norm_row_count,
    COUNTIF(match_type = 'unmatched') AS unmatched_row_count,
    COUNTIF(placement_name IS NULL OR TRIM(placement_name) = '') AS blank_placement_name_row_count,
    COUNTIF(utm_source IS NULL OR TRIM(utm_source) = '') AS blank_utm_source_row_count,
    COUNTIF(utm_campaign IS NULL OR TRIM(utm_campaign) = '') AS blank_utm_campaign_row_count,
    COUNTIF(utm_medium IS NULL OR TRIM(utm_medium) = '') AS blank_utm_medium_row_count,
    COUNTIF(utm_content IS NULL OR TRIM(utm_content) = '') AS blank_utm_content_row_count,
    COUNTIF(utm_term IS NULL OR TRIM(utm_term) = '') AS blank_utm_term_row_count,
    SUM(impressions) AS total_impressions,
    SUM(IF(match_type = 'unmatched', impressions, 0)) AS unmatched_impressions
  FROM final_three_pass

  UNION ALL

  SELECT
    'four_pass_extless_norm' AS validation_stage,
    COUNT(*) AS row_count,
    COUNTIF(match_type = 'exact') AS exact_row_count,
    COUNTIF(match_type = 'normalized') AS normalized_row_count,
    COUNTIF(match_type = 'loose_norm') AS loose_norm_row_count,
    COUNTIF(match_type = 'extless_norm') AS extless_norm_row_count,
    COUNTIF(match_type = 'unmatched') AS unmatched_row_count,
    COUNTIF(placement_name IS NULL OR TRIM(placement_name) = '') AS blank_placement_name_row_count,
    COUNTIF(utm_source IS NULL OR TRIM(utm_source) = '') AS blank_utm_source_row_count,
    COUNTIF(utm_campaign IS NULL OR TRIM(utm_campaign) = '') AS blank_utm_campaign_row_count,
    COUNTIF(utm_medium IS NULL OR TRIM(utm_medium) = '') AS blank_utm_medium_row_count,
    COUNTIF(utm_content IS NULL OR TRIM(utm_content) = '') AS blank_utm_content_row_count,
    COUNTIF(utm_term IS NULL OR TRIM(utm_term) = '') AS blank_utm_term_row_count,
    SUM(impressions) AS total_impressions,
    SUM(IF(match_type = 'unmatched', impressions, 0)) AS unmatched_impressions
  FROM final_four_pass
)

SELECT *
FROM summary
ORDER BY validation_stage;
