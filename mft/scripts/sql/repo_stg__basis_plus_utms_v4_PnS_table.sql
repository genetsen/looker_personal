-- Purpose: enrich Basis delivery with UTM fields while preserving exact-key
-- behavior, resolving FY26 audio trafficking-name differences, and restoring
-- 18 approved FY26 CTV placement-and-creative mappings.
-- Reads: repo_stg.basis_delivery, utm_scrap.b_sup_pivt_unioned_tab, and
-- repo_stg.dcm_plus_utms_upload. Replaces: repo_stg.basis_plus_utms_v4_PnS_table.
-- Safety: exact matches win. The CTV fallback is limited to an explicit
-- placement-and-creative allowlist. The audio fallback is usable only when the
-- normalized placement-and-creative key has one complete partner URL.

CREATE OR REPLACE VIEW `looker-studio-pro-452620.repo_stg.basis_plus_utms_v4_PnS_table` AS
WITH
delivery AS (
  SELECT *
  FROM `looker-studio-pro-452620.repo_stg.basis_delivery`
  WHERE campaign NOT LIKE '%GE%'
    AND campaign NOT LIKE 'Ritual%'
),

dcm_utm AS (
  SELECT
    placement,
    REGEXP_REPLACE(
      REGEXP_REPLACE(
        LOWER(
          REGEXP_REPLACE(
            REGEXP_REPLACE(creative, r'0x0', ''),
            r'(?:_?\d+x\d+.*)?$',
            ''
          )
        ),
        r'[^A-Za-z0-9\s]',
        ''
      ),
      r'\s+',
      ''
    ) AS normalized_creative,
    utm_source,
    utm_medium,
    utm_campaign,
    utm_content,
    utm_term
  FROM `looker-studio-pro-452620.repo_stg.dcm_plus_utms_upload`
),

basis_utm_base AS (
  SELECT
    tag_placement AS placement,
    name AS creative_name,
    url,
    REGEXP_REPLACE(
      REGEXP_REPLACE(
        LOWER(
          REGEXP_REPLACE(
            REGEXP_REPLACE(name, r'0x0', ''),
            r'(?:_?\d+x\d+.*)?$',
            ''
          )
        ),
        r'[^A-Za-z0-9\s]',
        ''
      ),
      r'\s+',
      ''
    ) AS normalized_creative,
    REGEXP_EXTRACT(url, r'[?&]utm_source=([^&#]*)') AS utm_source,
    REGEXP_EXTRACT(url, r'[?&]utm_medium=([^&#]*)') AS utm_medium,
    REGEXP_EXTRACT(url, r'[?&]utm_campaign=([^&#]*)') AS utm_campaign,
    REGEXP_EXTRACT(url, r'[?&]utm_content=([^&#]*)') AS utm_content,
    REGEXP_EXTRACT(url, r'[?&]utm_term=([^&#]*)') AS utm_term
  FROM `looker-studio-pro-452620.utm_scrap.b_sup_pivt_unioned_tab`
),

exact_utm AS (
  SELECT DISTINCT
    placement,
    normalized_creative,
    utm_source,
    utm_medium,
    utm_campaign,
    utm_content,
    utm_term,
    CONCAT(LOWER(placement), ' || ', normalized_creative) AS utm_key
  FROM basis_utm_base

  UNION DISTINCT

  SELECT DISTINCT
    placement,
    normalized_creative,
    utm_source,
    utm_medium,
    utm_campaign,
    utm_content,
    utm_term,
    CONCAT(LOWER(placement), ' || ', normalized_creative) AS utm_key
  FROM dcm_utm
),

audio_fallback_candidates AS (
  SELECT DISTINCT
    placement,
    REGEXP_REPLACE(
      normalized_creative,
      r'(^audio|companionbannertfimm\d+$)',
      ''
    ) AS audio_creative,
    url,
    utm_source,
    utm_medium,
    utm_campaign,
    utm_content,
    utm_term
  FROM basis_utm_base
  WHERE REGEXP_CONTAINS(placement, r'\[CPM\]_Audio_')
    AND NULLIF(TRIM(url), '') IS NOT NULL
    AND NULLIF(TRIM(utm_source), '') IS NOT NULL
    AND NULLIF(TRIM(utm_medium), '') IS NOT NULL
    AND NULLIF(TRIM(utm_campaign), '') IS NOT NULL
    AND NULLIF(TRIM(utm_content), '') IS NOT NULL
    AND NULLIF(TRIM(utm_term), '') IS NOT NULL
),

audio_fallback_unique AS (
  SELECT * EXCEPT(candidate_url_count, candidate_rank)
  FROM (
    SELECT
      placement,
      audio_creative,
      utm_source,
      utm_medium,
      utm_campaign,
      utm_content,
      utm_term,
      CONCAT(LOWER(placement), ' || ', audio_creative) AS utm_key,
      COUNT(DISTINCT url) OVER (
        PARTITION BY LOWER(placement), audio_creative
      ) AS candidate_url_count,
      ROW_NUMBER() OVER (
        PARTITION BY LOWER(placement), audio_creative
        ORDER BY url
      ) AS candidate_rank
    FROM audio_fallback_candidates
  )
  WHERE candidate_url_count = 1
    AND candidate_rank = 1
),

ctv_fallback_targets AS (
  SELECT *
  FROM UNNEST([
    STRUCT('3657572' AS placement_id, 'atnight30' AS delivery_creative, 'atnight' AS creative_family, '30vid' AS duration_token, 'same_family_duration' AS build_mode),
    STRUCT('3657572', 'galleryopening30', 'galleryopening', '30vid', 'same_family_duration'),
    STRUCT('3657572', 'homeoffice30', 'homeoffice', '30vid', 'same_family_duration'),
    STRUCT('3657577', 'welcometoflorida15', 'welcometoflorida', '15svid', 'placement_template'),
    STRUCT('3657581', 'atnight30', 'atnight', '30vid', 'same_family_duration'),
    STRUCT('3657581', 'autograph15streamingburnedincaptions16x9', 'autograph', '15svid', 'placement_template'),
    STRUCT('3657581', 'onemanshow15', 'onemanshow', '15svid', 'placement_template'),
    STRUCT('3657581', 'welcometoflorida15', 'welcometoflorida', '15svid', 'placement_template'),
    STRUCT('3657582', 'autograph15streamingburnedincaptions16x9', 'autograph', '15svid', 'placement_template'),
    STRUCT('3657582', 'onemanshow15', 'onemanshow', '15svid', 'placement_template'),
    STRUCT('3657582', 'playbyplay15streamingburnedincaptions16x9', 'playbyplay', '15svid', 'placement_template'),
    STRUCT('3657582', 'welcometoflorida15', 'welcometoflorida', '15svid', 'placement_template'),
    STRUCT('3657591', 'autograph15streamingburnedincaptions16x9', 'autograph', '15svid', 'placement_template'),
    STRUCT('3657591', 'playbyplay15streamingburnedincaptions16x9', 'playbyplay', '15svid', 'placement_template'),
    STRUCT('3661857', 'welcometoflorida15', 'welcometoflorida', '15svid', 'placement_template'),
    STRUCT('3661860', 'atnight30', 'atnight', '30vid', 'same_family_duration'),
    STRUCT('3671269', 'autograph30streamingburnedincaptions16x9', 'autograph', '30vid', 'placement_template'),
    STRUCT('3671269', 'playbyplay30streamingburnedincaptions16x9', 'playbyplay', '30vid', 'placement_template')
  ])
),

ctv_fallback_candidates AS (
  SELECT
    target.placement_id,
    target.delivery_creative,
    ARRAY_AGG(source.placement ORDER BY source.placement LIMIT 1)[OFFSET(0)] AS placement,
    ARRAY_AGG(source.utm_source ORDER BY source.utm_source LIMIT 1)[OFFSET(0)] AS utm_source,
    ARRAY_AGG(source.utm_medium ORDER BY source.utm_medium LIMIT 1)[OFFSET(0)] AS utm_medium,
    ARRAY_AGG(source.utm_campaign ORDER BY source.utm_campaign LIMIT 1)[OFFSET(0)] AS utm_campaign,
    ARRAY_AGG(source.utm_term ORDER BY source.utm_term LIMIT 1)[OFFSET(0)] AS utm_term,
    CASE target.build_mode
      WHEN 'same_family_duration' THEN REGEXP_REPLACE(
        ARRAY_AGG(source.utm_content ORDER BY source.utm_content LIMIT 1)[OFFSET(0)],
        r'^[^_]+',
        target.duration_token
      )
      ELSE CONCAT(
        target.duration_token,
        '_',
        target.creative_family,
        '_learn_fimm_',
        ARRAY_AGG(
          REGEXP_EXTRACT(source.utm_content, r'_(\d+-\d+.*)$')
          ORDER BY REGEXP_EXTRACT(source.utm_content, r'_(\d+-\d+.*)$')
          LIMIT 1
        )[OFFSET(0)]
      )
    END AS utm_content
  FROM ctv_fallback_targets AS target
  JOIN basis_utm_base AS source
    ON REGEXP_EXTRACT(source.placement, r'MASSMUTUAL005CP_(\d+)') = target.placement_id
   AND (
     target.build_mode = 'placement_template'
     OR REGEXP_REPLACE(source.normalized_creative, r'(15|30)$', '') = target.creative_family
   )
  GROUP BY
    target.placement_id,
    target.delivery_creative,
    target.creative_family,
    target.duration_token,
    target.build_mode
  HAVING COUNT(DISTINCT source.utm_source) = 1
    AND COUNT(DISTINCT source.utm_medium) = 1
    AND COUNT(DISTINCT source.utm_campaign) = 1
    AND COUNT(DISTINCT source.utm_term) = 1
    AND (
      (
        target.build_mode = 'same_family_duration'
        AND COUNT(DISTINCT source.utm_content) = 1
      )
      OR (
        target.build_mode = 'placement_template'
        AND COUNT(DISTINCT REGEXP_EXTRACT(source.utm_content, r'_(\d+-\d+.*)$')) = 1
      )
    )
),

ctv_fallback_unique AS (
  SELECT
    placement,
    utm_source,
    utm_medium,
    utm_campaign,
    utm_content,
    utm_term,
    CONCAT(LOWER(placement), ' || ', delivery_creative) AS utm_key
  FROM ctv_fallback_candidates
),

joined AS (
  SELECT
    delivery.*,
    COALESCE(exact_utm.placement, ctv_fallback_unique.placement, audio_fallback_unique.placement) AS placement__utms,
    COALESCE(exact_utm.utm_source, ctv_fallback_unique.utm_source, audio_fallback_unique.utm_source) AS utm_source,
    COALESCE(exact_utm.utm_medium, ctv_fallback_unique.utm_medium, audio_fallback_unique.utm_medium) AS utm_medium,
    COALESCE(exact_utm.utm_campaign, ctv_fallback_unique.utm_campaign, audio_fallback_unique.utm_campaign) AS utm_campaign,
    COALESCE(exact_utm.utm_term, ctv_fallback_unique.utm_term, audio_fallback_unique.utm_term) AS utm_term,
    COALESCE(exact_utm.utm_content, ctv_fallback_unique.utm_content, audio_fallback_unique.utm_content) AS utm_content,
    COALESCE(exact_utm.utm_key, ctv_fallback_unique.utm_key, audio_fallback_unique.utm_key) AS utm_key,
    delivery.del_key AS master_key
  FROM delivery
  LEFT JOIN exact_utm
    ON delivery.del_key = exact_utm.utm_key
  LEFT JOIN ctv_fallback_unique
    ON exact_utm.utm_key IS NULL
   AND delivery.del_key = ctv_fallback_unique.utm_key
  LEFT JOIN audio_fallback_unique
    ON exact_utm.utm_key IS NULL
   AND ctv_fallback_unique.utm_key IS NULL
   AND delivery.del_key = audio_fallback_unique.utm_key
),

ranked AS (
  SELECT
    joined.*,
    ROW_NUMBER() OVER (
      PARTITION BY date, master_key
      ORDER BY placement
    ) AS row_rank
  FROM joined
)

SELECT * EXCEPT(
  row_rank,
  meta_data_date_pull,
  package,
  gmail_dt,
  meta_data_date_range
)
FROM ranked
WHERE row_rank = 1;
