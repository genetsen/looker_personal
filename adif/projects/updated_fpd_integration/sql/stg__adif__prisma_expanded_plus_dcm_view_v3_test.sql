/*******************************************************************************
STAGING VIEW: ADIF Base View v3 Test
********************************************************************************
Purpose: Base daily ADIF staging view that combines DCM, filtered original FPD,
         and Prisma planning data.

Original FPD source:
- `looker-studio-pro-452620.landing.fpd_data_ranged_shortcutsFolder`
- Includes rows where `source_file` contains `De Beers`
- Includes rows where `source_file` starts with
  `FMUS | Partner Data Collection |`

Join grain:
- package_id + date

Last Updated: 2026-03-16
*******************************************************************************/

WITH
-- 1) DCM (already unique at package_id + date)
a AS (
  SELECT
    CASE
      WHEN package_id = 'P3923KK' THEN 'P37K96P'
      WHEN package_id = 'P37K96T' THEN 'P37K96P'
      WHEN package_id = 'P37MLHQ' THEN 'P37K96P'
      WHEN package_id = 'P37MLHP' THEN 'P37K96P'
      WHEN package_id = 'P37K96S' THEN 'P37K96P'
      WHEN package_id = 'P37DSDJ' THEN 'P37DSDR'
      ELSE package_id
    END AS package_id,
    flight_status_flag,
    DATE(date) AS date,
    SUM(daily_recalculated_cost) AS d_daily_recalculated_cost,
    SUM(daily_recalculated_imps) AS d_daily_recalculated_imps,
    SUM(impressions) AS d_impressions,
    SUM(media_cost) AS d_media_cost,
    SUM(clicks) AS d_clicks,
    SUM(rich_media_video_plays) AS d_video_plays,
    SUM(rich_media_video_completions) AS d_video_comps,
    MIN(d_min_date) AS d_min_date,
    MAX(d_max_date) AS d_max_date,
    MIN(prorated_planned_cost_pk) AS d_prorated_planned_cost_pk,
    MIN(prorated_planned_imps_pk) AS d_prorated_planned_imps_pk,
    MIN(min_flight_date) AS d_min_flight_date,
    MAX(max_flight_date) AS d_max_flight_date,
    AVG(daily_cpm) AS d_daily_cpm,
    MAX(pkg_total_imps) AS d_total_delivered_imps,
    MAX(total_inflight_impressions) AS d_total_del_inflight_imps
  FROM `looker-studio-pro-452620.DCM.20250505_costModel_v5`
  WHERE advertiser = 'Forevermark US'
  GROUP BY package_id, flight_status_flag, DATE(date)
),

-- 2) Original FPD from filtered shortcuts-folder source
fpd_raw AS (
  SELECT
    CASE
      WHEN package_id = 'P3923KK' THEN 'P37K96P'
      WHEN package_id = 'P37K96T' THEN 'P37K96P'
      WHEN package_id = 'P37MLHQ' THEN 'P37K96P'
      WHEN package_id = 'P37MLHP' THEN 'P37K96P'
      WHEN package_id = 'P37K96S' THEN 'P37K96P'
      WHEN package_id = 'P37DSDJ' THEN 'P37DSDR'
      ELSE package_id
    END AS package_id,
    DATE(date_final) AS date,
    impressions,
    clicks,
    spend,
    sends,
    opens,
    benchmark,
    benchmark_metric,
    partner_creative_name AS creative
  FROM `looker-studio-pro-452620.landing.fpd_data_ranged_shortcutsFolder`
  WHERE REGEXP_CONTAINS(source_file, r'(?i)De Beers')
     OR REGEXP_CONTAINS(source_file, r'^FMUS \| Partner Data Collection \|')
),

-- 3) FPD daily (force 1 row per package_id + date)
fpd AS (
  SELECT
    package_id,
    date,
    SUM(impressions) AS fpd_impressions,
    SUM(clicks) AS fpd_clicks,
    SUM(spend) AS fpd_spend,
    SAFE_CAST(ROUND(SUM(sends)) AS INT64) AS fpd_sends,
    SAFE_CAST(ROUND(SUM(opens)) AS INT64) AS fpd_opens,
    MAX(benchmark) AS fpd_benchmark,
    SAFE_CAST(ROUND(MAX(benchmark_metric)) AS INT64) AS fpd_benchmark_metric,
    STRING_AGG(
      DISTINCT CAST(creative AS STRING),
      ' | '
      ORDER BY CAST(creative AS STRING)
    ) AS fpd_creative
  FROM fpd_raw
  GROUP BY package_id, date
),

-- 4) Prisma daily
b_raw AS (
  SELECT
    package_id,
    DATE(date) AS date,
    planned_daily_spend_pk,
    planned_daily_impressions_pk,
    report_date
  FROM `looker-studio-pro-452620.20250327_data_model.prisma_expanded_full`
  WHERE advertiser_name = 'Forevermark US'
    AND package_type != 'Child'
),
b AS (
  SELECT
    package_id,
    date,
    SUM(planned_daily_spend_pk) AS planned_daily_spend_pk,
    SUM(planned_daily_impressions_pk) AS planned_daily_impressions_pk,
    MAX(report_date) AS max_prismaEXP_report_date
  FROM b_raw
  GROUP BY package_id, date
),

-- 5) FULL OUTER JOIN DCM + FPD
a_fpd AS (
  SELECT
    COALESCE(da.package_id, df.package_id) AS package_id_joined,
    COALESCE(da.date, df.date) AS date,
    da.* EXCEPT(package_id, date),
    df.* EXCEPT(package_id, date)
  FROM a AS da
  FULL OUTER JOIN fpd AS df
    ON da.package_id = df.package_id
   AND da.date = df.date
),

-- 6) FULL OUTER JOIN (DCM + FPD) + Prisma
t AS (
  SELECT
    COALESCE(af.package_id_joined, db.package_id) AS package_id_joined,
    COALESCE(af.date, db.date) AS date,
    af.* EXCEPT(package_id_joined, date),
    db.* EXCEPT(package_id, date)
  FROM a_fpd AS af
  FULL OUTER JOIN b AS db
    ON af.package_id_joined = db.package_id
   AND af.date = db.date
),

-- 7) Row-level coalesced delivery fields
final AS (
  SELECT
    t.*,
    COALESCE(fpd_spend, d_daily_recalculated_cost) AS final_spend,
    COALESCE(fpd_impressions, d_impressions) AS final_impressions,
    COALESCE(fpd_clicks, d_clicks) AS final_clicks,
    fpd_sends AS final_sends,
    fpd_opens AS final_opens
  FROM t
),

-- 8) Package-level rollups and flags
pkg AS (
  SELECT
    f.*,
    SUM(COALESCE(planned_daily_spend_pk, 0))
      OVER (PARTITION BY package_id_joined) AS pkg_est_spend,
    SUM(COALESCE(final_spend, 0))
      OVER (PARTITION BY package_id_joined) AS pkg_act_spend,
    SUM(COALESCE(planned_daily_impressions_pk, 0))
      OVER (PARTITION BY package_id_joined) AS pkg_est_imps,
    SUM(COALESCE(final_impressions, 0))
      OVER (PARTITION BY package_id_joined) AS pkg_act_imps,
    (
      SUM(COALESCE(planned_daily_spend_pk, 0))
        OVER (PARTITION BY package_id_joined)
      > SUM(COALESCE(final_spend, 0))
        OVER (PARTITION BY package_id_joined)
    ) AS pkg_over_bool,
    IF(
      SUM(COALESCE(planned_daily_spend_pk, 0))
        OVER (PARTITION BY package_id_joined)
      > SUM(COALESCE(final_spend, 0))
        OVER (PARTITION BY package_id_joined),
      1,
      0
    ) AS pkg_over_flag
  FROM final AS f
),

-- 9) Prisma package metadata
meta_src AS (
  SELECT * EXCEPT(planned_daily_spend_pk, planned_daily_impressions_pk, date)
  FROM `looker-studio-pro-452620.20250327_data_model.prisma_expanded_full`
  WHERE package_type != 'Child'
),
meta AS (
  SELECT x.*
  FROM (
    SELECT
      (ARRAY_AGG(m ORDER BY m.report_date DESC NULLS LAST))[OFFSET(0)] AS x
    FROM meta_src AS m
    GROUP BY m.package_id
  )
),

-- 10) Join delivery+rollups to package metadata
dm AS (
  SELECT
    p.*,
    m.*
  FROM pkg AS p
  FULL OUTER JOIN meta AS m
    ON p.package_id_joined = m.package_id
),

-- 11) Higher-level any-package-over flags
roll AS (
  SELECT
    dm.*,
    MAX(pkg_over_flag) OVER (PARTITION BY channel) AS channel_any_pkg_over,
    MAX(pkg_over_flag) OVER (PARTITION BY supplier_code) AS site_any_pkg_over,
    CASE
      WHEN package_name IS NULL OR package_name = '' THEN NULL
      WHEN REGEXP_CONTAINS(package_name, r'(?i)(iHeart|SiriusXM|WeAreAOk|Wonder)') THEN 'Audio'
      WHEN REGEXP_CONTAINS(package_name, r'(?i)(Peacock|DISNED|ESPN|Hulu|Roku|HBO|Paramount|Tubi|YouTube|NBCU)') THEN 'Video'
      WHEN REGEXP_CONTAINS(package_name, r'(?i)(NBC|WBD|CBS|Playfly|Audience Express|Disney)') THEN 'Linear'
      WHEN REGEXP_CONTAINS(supplier_code, r'(?i)(PROJEX|Quan|QUAN)') THEN 'OOH Regional'
      WHEN REGEXP_CONTAINS(package_name, r'(?i)(PROJEX|Quan|QUAN)') THEN 'OOH Regional'
      WHEN REGEXP_CONTAINS(package_name, r'(?i)(ScreenVision|NCM)') THEN 'Cinema Regional'
      WHEN REGEXP_CONTAINS(package_name, r'(?i)(Vox|CondeNast|NYT|Meredi|TINYBE|NATVLY)') THEN 'Publisher Partnership'
      WHEN REGEXP_CONTAINS(package_name, r'(?i)People First') THEN 'Influencer'
      WHEN REGEXP_CONTAINS(package_name, r'(?i)People') THEN 'Publisher Partnership'
      WHEN REGEXP_CONTAINS(package_name, r'(?i)(Jeweler|JCK|Instore|Gem|AGS|Centurion|Zimnisky|KENIL|RELX)') THEN 'Trade'
      WHEN REGEXP_CONTAINS(package_name, r'(?i)MIQ') THEN 'Programmatic'
      WHEN REGEXP_CONTAINS(package_name, r'(?i)feeorder') THEN 'fee'
      WHEN REGEXP_CONTAINS(package_type, r'(?i)Print') THEN 'Publisher Partnership'
      ELSE 'Unmapped'
    END AS gsMediaTeam_channel
  FROM dm
)

SELECT
  * EXCEPT(advertiser_short_name),
  CURRENT_TIMESTAMP() AS final_table_refresh_date,
  COALESCE(initative, package_name) AS initiative
FROM roll
WHERE package_id_joined IS NOT NULL;
