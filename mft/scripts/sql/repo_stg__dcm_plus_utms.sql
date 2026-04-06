-- =============================================================================
-- VIEW: repo_stg.dcm_plus_utms
-- Purpose: Enriches DCM (Campaign Manager) ad delivery data with UTM tracking
--          parameters sourced from the UTMs sheet. Uses a four-tier join
--          strategy:
--          (1) exact match on placement_id + creative_assignment
--          (2) normalized Mass-row fallback
--          (3) size-stripped Mass-row fallback
--          (4) file-suffix-stripped Mass-row fallback
-- Source tables:
--   - final_views.dcm        : DCM delivery data (impressions, clicks, cost, etc.)
--   - final_views.utms_view  : UTM tagging data keyed by placement + creative
-- =============================================================================
CREATE OR REPLACE VIEW `looker-studio-pro-452620.repo_stg.dcm_plus_utms` AS

-- -----------------------------------------------------------------------------
-- CTE 1: dcm_prepped
-- Add Mass-scope flags and reusable normalized creative keys on the DCM side.
-- Non-exact fallback joins are only allowed for rows whose package_roadblock
-- indicates the Mass branch.
-- -----------------------------------------------------------------------------
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

-- -----------------------------------------------------------------------------
-- CTE 2: utm_exact
-- Deduplicate UTMs by exact placement_id + creative_assignment key.
-- -----------------------------------------------------------------------------
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

-- -----------------------------------------------------------------------------
-- CTE 3: utm_norm
-- Mass-row fallback using lowercased and trimmed keys with "px" removed.
-- -----------------------------------------------------------------------------
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

-- -----------------------------------------------------------------------------
-- CTE 4: utm_loose
-- Mass-row fallback that also removes whitespace and trailing size tokens such
-- as "_0x0", "_0 x 0", or "_1x1".
-- -----------------------------------------------------------------------------
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

-- -----------------------------------------------------------------------------
-- CTE 5: utm_extless
-- Final Mass-row fallback that removes common file-type suffixes after the
-- same whitespace and size normalization used above.
-- -----------------------------------------------------------------------------
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

-- -----------------------------------------------------------------------------
-- CTE 6: utm_placement_campaign
-- Placement-name-only rescue for Mass rows where the placement exists in the
-- UTM source for the same campaign, but the creative still does not match.
-- This does not backfill creative-level UTM fields such as utm_content.
-- -----------------------------------------------------------------------------
utm_placement_campaign AS (
  SELECT * EXCEPT(rn_placement_campaign)
  FROM (
    SELECT
      utm.placement_id,
      utm.placement_name,
      LOWER(TRIM(utm.campaign)) AS campaign_norm,
      LOWER(TRIM(utm.placement_id)) AS placement_id_norm,
      ROW_NUMBER() OVER (
        PARTITION BY
          LOWER(TRIM(utm.campaign)),
          LOWER(TRIM(utm.placement_id))
        ORDER BY utm.last_updated DESC, utm.placement_end_date DESC, utm.start DESC
      ) AS rn_placement_campaign
    FROM `looker-studio-pro-452620.final_views.utms_view` AS utm
  )
  WHERE rn_placement_campaign = 1
),

-- -----------------------------------------------------------------------------
-- CTE 7: utm_placement_any
-- Final placement-name-only rescue keyed just by placement_id. Used only after
-- same-campaign placement rescue misses, to catch placement rows where the
-- campaign text differs but the placement_id itself exists in the UTM source.
-- -----------------------------------------------------------------------------
utm_placement_any AS (
  SELECT * EXCEPT(rn_placement_any)
  FROM (
    SELECT
      utm.placement_id,
      utm.placement_name,
      LOWER(TRIM(utm.placement_id)) AS placement_id_norm,
      ROW_NUMBER() OVER (
        PARTITION BY LOWER(TRIM(utm.placement_id))
        ORDER BY utm.last_updated DESC, utm.placement_end_date DESC, utm.start DESC
      ) AS rn_placement_any
    FROM `looker-studio-pro-452620.final_views.utms_view` AS utm
  )
  WHERE rn_placement_any = 1
),

-- -----------------------------------------------------------------------------
-- CTE 8: joined
-- Core join: attach UTM fields to every DCM delivery row.
--
-- Join strategy:
--   Pass 1 (utm_exact):    strict match on placement_id + creative_assignment.
--   Pass 2 (utm_norm):     Mass-only fallback using lower/trim/"px" removal.
--   Pass 3 (utm_loose):    Mass-only fallback that also removes whitespace and
--                          trailing size tokens.
--   Pass 4 (utm_extless):  Mass-only fallback that also strips common file
--                          suffixes such as "_jpg".
--   Pass 5 (utm_placement_campaign): placement-name-only fallback for same-
--                          campaign placement matches when creative matching
--                          still fails.
--   Pass 6 (utm_placement_any): placement-name-only fallback keyed on
--                          placement_id alone when the placement exists in the
--                          UTM source but campaign text differs.
--   Pass 7 (dcm placement): final placement-name-only fallback from the live
--                          DCM placement field when no UTM placement exists.
--
-- COALESCE on output columns keeps exact matches first, then progressively
-- looser Mass-only creative fallbacks, then placement-name-only rescue.
-- -----------------------------------------------------------------------------
joined AS (
  SELECT
    -- --- DCM delivery dimensions ---
    dcm.date,
    dcm.campaign,
    dcm.package_roadblock,
    dcm.package_id,
    dcm.placement_id,

    -- --- DCM delivery metrics ---
    dcm.impressions,
    dcm.KEY,
    dcm.ad,
    dcm.click_rate,
    dcm.clicks,
    dcm.creative,
    dcm.media_cost,
    dcm.rich_media_video_completions,
    dcm.rich_media_video_plays,
    dcm.total_conversions,

    -- --- Planned/pacing fields (prefixed p_) ---
    dcm.p_cost_method,
    dcm.p_package_friendly,
    dcm.p_start_date,
    dcm.p_end_date,
    dcm.p_total_days,
    dcm.p_pkg_daily_planned_cost,
    dcm.p_pkg_total_planned_cost,
    dcm.p_pkg_daily_planned_imps,
    dcm.p_pkg_total_planned_imps,
    dcm.p_channel_group,
    dcm.p_advertiser_name,

    -- --- Flight/pacing status flags ---
    dcm.flight_date_flag,
    dcm.flight_status_flag,
    dcm.rate_raw,
    dcm.n_of_placements,
    dcm.d_min_date,
    dcm.d_max_date,
    dcm.min_flight_date,
    dcm.max_flight_date,

    -- --- Impression delivery aggregates ---
    dcm.pkg_total_imps,
    dcm.total_inflight_impressions,
    dcm.pkg_daily_imps,
    dcm.pkg_daily_imps_perc,
    dcm.pkg_total_imps_perc,
    dcm.pkg_inflight_imps_perc,
    dcm.days_live,

    -- --- Cost / impression recalculation fields ---
    dcm.prorated_planned_cost_pk,
    dcm.prorated_planned_imps_pk,
    dcm.cpm_overdelivery_flag,
    dcm.daily_cpm,
    dcm.daily_recalculated_cost,
    dcm.daily_recalculated_cost_flag,
    dcm.daily_recalculated_imps,

    CONCAT(dcm.placement_id, ' || ', dcm.creative) AS utm_key,

    -- --- UTM fields: exact first, then progressively looser Mass-only fallbacks ---
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
      utm_extless.placement_name,
      utm_placement_campaign.placement_name,
      utm_placement_any.placement_name,
      dcm.placement
    ) AS placement_name,
    COALESCE(
      utm_exact.placement_id,
      utm_norm.placement_id,
      utm_loose.placement_id,
      utm_extless.placement_id,
      utm_placement_campaign.placement_id,
      utm_placement_any.placement_id,
      dcm.placement_id
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
    ) AS utm_utm_key

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

  LEFT JOIN utm_placement_campaign
    ON utm_exact.placement_id IS NULL
   AND utm_norm.placement_id IS NULL
   AND utm_loose.placement_id IS NULL
   AND utm_extless.placement_id IS NULL
   AND dcm.is_mass_row
   AND utm_placement_campaign.campaign_norm = dcm.campaign_norm
   AND utm_placement_campaign.placement_id_norm = dcm.placement_id_norm

  LEFT JOIN utm_placement_any
    ON utm_exact.placement_id IS NULL
   AND utm_norm.placement_id IS NULL
   AND utm_loose.placement_id IS NULL
   AND utm_extless.placement_id IS NULL
   AND utm_placement_campaign.placement_id IS NULL
   AND dcm.is_mass_row
   AND utm_placement_any.placement_id_norm = dcm.placement_id_norm
),

-- -----------------------------------------------------------------------------
-- CTE 9: ranked
-- Deduplication safety net: if the same fully-serialized row appears more than
-- once (for example due to lookup fanout), keep only one copy.
-- -----------------------------------------------------------------------------
ranked AS (
  SELECT
    joined.*,
    ROW_NUMBER() OVER (
      PARTITION BY TO_JSON_STRING(joined)
      ORDER BY placement_id
    ) AS rn
  FROM joined
)

SELECT * EXCEPT(rn)
FROM ranked
WHERE rn = 1;
