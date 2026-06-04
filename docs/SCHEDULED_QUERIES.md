# BigQuery Scheduled Queries

Documentation of scheduled queries configured in the `looker-studio-pro-452620` project, including known failed/deprecated jobs.

## Overview

```
┌─────────────────────────────────────────────────────────────────────────────────────┐
│                           SCHEDULED QUERY EXECUTION ORDER                            │
├─────────────────────────────────────────────────────────────────────────────────────┤
│                                                                                     │
│  03:00  ──► mart__dcm__joined_0519 (FAILED - disabled?)                            │
│                                                                                     │
│  05:00  ──► 250813_crossplatform_dedupe_history                                     │
│             (Facebook, Google Ads, TikTok history deduplication)                    │
│                                                                                     │
│  07:00  ──► process_prisma ──► Prisma_expanded                                      │
│             (Prisma processing + expansion to daily records)                        │
│                                                                                     │
│  08:00  ──► UTM UPDATES                                                             │
│             (Refresh all UTM source tables + build master)                          │
│                                                                                     │
│  09:00  ──► adif update ──► adif_prisma_expanded_plus_dcm_v2                        │
│             (ADIF FPD + DCM + Prisma integration)                                   │
│                                                                                     │
│  EVERY   ──► ADIF_FullDataRefresh_2604                                               │
│  10 HRS  │    (Main ADIF scheduled refresh to v2 shadow table)                      │
│                                                                                     │
│  10:00  ──► basis_update ──► stg__olipop__crossplatform_raw_tbl_sched              │
│         ──► mart__pacing_table ──► ext_mm_mft_scheadule                             │
│             (Basis merge, Olipop video, Pacing, MFT export)                         │
│                                                                                     │
│  EVERY   ──► mm_dcm_costmodel (4 hours)                                             │
│  N HRS   ──► prisma__stg__digital_plus_linear (8 hours)                             │
│                                                                                     │
└─────────────────────────────────────────────────────────────────────────────────────┘
```

---

## Summary Table

| # | Query Name | Schedule | Status | Target Table(s) |
|---|-----------|----------|--------|-----------------|
| 1 | `mm_dcm_costmodel` | Every 4 hours | ✅ SUCCEEDED | `DCM.20250505_costModel_v5` |
| 2 | `prisma__stg__digital_plus_linear` | Every 8 hours | ✅ SUCCEEDED | `Prisma.prisma__stg__digital_plus_linear_view` |
| 3 | `250813_crossplatform_dedupe_history` | Daily 05:00 UTC | ✅ SUCCEEDED | `repo_facebook.*`, `repo_google_ads.*`, `repo_tiktok.*` |
| 4 | `process_prisma` | Daily 07:00 UTC | ✅ SUCCEEDED | `20250327_data_model.prisma_porcessed`, `*.prisma_porcessed_with_placements` |
| 5 | `Prisma_expanded` | Mon-Fri 07:00 UTC | ✅ SUCCEEDED | `20250327_data_model.prisma_expanded_full`, `*.prisma_expanded_summary`, `Prisma.prismaExpanded_x_dcmDelivery` |
| 6 | `UTM UPDATES` | Daily 08:00 UTC | ✅ SUCCEEDED | `mm_utms_snapshot`, `b_sup_pivt_unioned_tab`, `master_utms_raw` |
| 7 | `adif update` | Daily 09:00 UTC | ✅ SUCCEEDED | `landing.adif_fpd_data_ranged` |
| 8 | `adif_prisma_expanded_plus_dcm_v2` | Daily 09:00 UTC | ✅ SUCCEEDED | `repo_stg.adif__prisma_expanded_plus_dcm_view_v3_test` |
| 9 | `ADIF_FullDataRefresh_2604` | Every 10 hours | ✅ SUCCEEDED | `repo_stg.adif__mainDataTable_notebook_v2_test` |
| 10 | `basis_update` | Daily 10:00 UTC | ✅ SUCCEEDED | `repo_stg.basis_master2` |
| 11 | `stg__olipop__crossplatform_raw_tbl_sched` | Daily 10:00 UTC | ✅ SUCCEEDED | `repo_stg.stg__olipop__crossplatform_raw_tbl` |
| 12 | `mart__pacing_table` | Daily 10:00 UTC | ✅ SUCCEEDED | `repo_mart.fct_crossplatform_pacing_daily` |
| 13 | `ext_mm_mft_scheadule` | Daily 10:00 UTC | ✅ SUCCEEDED | External export |
| 14 | `mart__dcm__joined_0519` | Daily 03:00 UTC | ❌ FAILED | `repo_tables.dcm` |

---

> Note: Some identifiers (for example `prisma_porcessed`, `ext_mm_mft_scheadule`) may be historical asset names. Verify actual BigQuery object names before renaming in docs or code.

## Detailed Query Documentation

### 1. `mm_dcm_costmodel`

**Schedule**: Every 4 hours UTC
**Status**: ✅ SUCCEEDED
**Last Updated**: Dec 16, 2025

#### Purpose
Core DCM cost model that joins delivery data with Prisma planning metadata. This is the **foundation table** used by MFT, ADIF, and other downstream pipelines.

#### Target
```
looker-studio-pro-452620.DCM.20250505_costModel_v5
```

#### Source Tables
- `giant-spoon-299605.data_model_2025.new_md` (DCM delivery)
- `looker-studio-pro-452620.20250327_data_model.prisma_porcessed` (Prisma metadata)

#### Key Transformations
1. **base_delivery_metadata**: Joins delivery to Prisma, applies 60-day email flight extension
2. **delivery_rollups**: Package-level aggregates (min/max dates, placement counts)
3. **daily_recalculated_cost**: Normalizes daily spend across package lifecycle
4. **daily_recalculated_imps**: Normalizes daily impressions

#### Key Logic
```sql
-- Flight date flag: 0 = in-flight, 1 = out-of-flight
CASE
  WHEN a.date BETWEEN b.start_date AND
    CASE WHEN a.package_roadblock LIKE '%email%'
         THEN DATE_ADD(b.end_date, INTERVAL 60 DAY)
         ELSE b.end_date END
  THEN 0 ELSE 1
END AS flight_date_flag

-- Flight status flag
CASE
  WHEN CURRENT_DATE() > b.end_date THEN 'ended'
  ELSE 'live'
END AS flight_status_flag
```

#### Output Metrics
- 129,733 rows
- Daily cost, impression, click, video metrics
- Package-level rollups and flags

---

### 2. `prisma__stg__digital_plus_linear`

**Schedule**: Every 8 hours UTC
**Status**: ✅ SUCCEEDED

#### Purpose
Combines digital Prisma planning with linear (TV) estimates into a unified planning view.

#### Target
```
looker-studio-pro-452620.Prisma.prisma__stg__digital_plus_linear_view
```

#### Source Tables
- Digital: `20250327_data_model.prisma_expanded_full`
- Linear: `landing.tv_national_estimates`, `landing.tv_local_estimates`

---

### 3. `250813_crossplatform_dedupe_history`

**Schedule**: Daily 05:00 UTC
**Status**: ✅ SUCCEEDED
**Last Updated**: Aug 13, 2025

#### Purpose
Deduplicates history tables from multiple ad platforms using a reusable stored procedure.

#### Platforms Processed

**Facebook** (5 tables):
- `campaign_history` → `repo_facebook.campaign_history_deduped`
- `ad_set_history` → `repo_facebook.ad_set_history_deduped`
- `ad_history` → `repo_facebook.ad_history_deduped`
- `ad_video_history` → `repo_facebook.ad_video_history_deduped`
- `creative_history` → `repo_facebook.creative_history_deduped`

**Google Ads** (4 tables):
- `campaign_history` → `repo_google_ads.campaign_history_deduped`
- `campaign_budget_history` → `repo_google_ads.campaign_budget_history_deduped`
- `ad_group_history` → `repo_google_ads.ad_group_history_deduped`
- `video_ad_history` → `repo_google_ads.video_ad_history_deduped`

**TikTok** (4 tables):
- `campaign_history` → `repo_tiktok.stg__campaign_history_deduped`
- `adgroup_history` → `repo_tiktok.stg__adgroup_history_deduped`
- `ad_history` → `repo_tiktok.stg__ad_history_deduped`
- `video_history` → `repo_tiktok.stg__video_history_deduped`

#### Stored Procedure
```sql
CALL `looker-studio-pro-452620.repo_util.dedupe_table_by_primary_id_v3`(
  source_project,
  source_dataset,
  source_table,
  target_project,
  target_dataset,
  primary_key_column
);
```

---

### 4. `process_prisma`

**Schedule**: Daily 07:00 UTC
**Status**: ✅ SUCCEEDED
**Last Updated**: Dec 4, 2025

#### Purpose
Processes raw Prisma data into two aggregated views:
1. Package-level summary
2. Package + Placement-level detail

#### Targets
```
looker-studio-pro-452620.20250327_data_model.prisma_porcessed (TABLE)
looker-studio-pro-452620.20250327_data_model.prisma_porcessed_with_placements (VIEW)
```

#### Source
```
looker-studio-pro-452620.landing.prisma_master_2025
```

#### Key Transformations
- Aggregates by `package_id` (Query 1) or `package_id + placement_id` (Query 2)
- Calculates `planned_daily_spend_pk` and `planned_daily_impressions_pk`
- Creates `p_package_friendly` identifier string
- Filters out fee packages (`%feeorder%`, `%fees_%`, `%fee_%`)
- Uses `STRING_AGG(DISTINCT INITATIVE)` for multi-initiative packages

#### Related Prisma Actuals Views
- `looker-studio-pro-452620.Prisma.prisma_processed_plusDCMimps` is the existing package-level Prisma + DCM sibling view.
- `looker-studio-pro-452620.Prisma.prisma_processed_plusDCMFPD` is the new package-level Prisma + DCM + FPD sibling view defined in [`/Users/eugenetsenter/Looker_clonedRepo/looker_personal/Prisma/prisma_processed_plusDCMFPD.sql`](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/Prisma/prisma_processed_plusDCMFPD.sql).
- The new view keeps the `prisma_porcessed` package grain, preserves the existing DCM package rollups, adds package-level FPD rollups from `landing.fpd_data_ranged_shortcutsFolder`, and treats FPD as valid tracking only when DCM is absent.

#### Output Schema (Key Fields)
| Field | Calculation |
|-------|-------------|
| `planned_cost_pk` | `SUM(PLANNED_AMOUNT)` |
| `planned_imps_pk` | `SUM(PLANNED_IMPRESSIONS)` |
| `total_days` | `DATE_DIFF(end_date, start_date, DAY) + 1` |
| `planned_daily_spend_pk` | `planned_cost_pk / total_days` |
| `planned_daily_impressions_pk` | `planned_imps_pk / total_days` |
| `n_of_placements` | `COUNT(DISTINCT PlacementName)` |

---

### 5. `Prisma_expanded`

**Schedule**: Mon-Fri 07:00 UTC
**Status**: ✅ SUCCEEDED
**Last Updated**: Dec 12, 2025

#### Purpose
Three-step pipeline that:
1. Expands Prisma to daily records (one row per package per day)
2. Creates summary aggregation
3. Joins expanded Prisma with DCM delivery

#### Targets
```
Step 1: looker-studio-pro-452620.20250327_data_model.prisma_expanded_full (TABLE)
Step 2: looker-studio-pro-452620.20250327_data_model.prisma_expanded_summary (TABLE)
Step 3: looker-studio-pro-452620.Prisma.prismaExpanded_x_dcmDelivery (TABLE)
```

#### Key Logic - Step 1 (Date Expansion)
```sql
SELECT
  pm.*,
  date_day AS date,  -- Generated date
  pm.planned_amount / (DATE_DIFF(pm.end_date, pm.start_date, DAY) + 1) AS daily_spend,
  pm.planned_impressions / (DATE_DIFF(pm.end_date, pm.start_date, DAY) + 1) AS daily_impressions
FROM prisma_master pm,
UNNEST(GENERATE_DATE_ARRAY(pm.start_date, pm.end_date)) AS date_day
```

#### Key Logic - Step 3 (DCM Join with Package ID Normalization)
```sql
-- Package ID alias mapping (6 cases)
CASE
  WHEN package_id = 'P3923KK' THEN 'P37K96P'
  WHEN package_id = 'P37K96T' THEN 'P37K96P'
  WHEN package_id = 'P37MLHQ' THEN 'P37K96P'
  WHEN package_id = 'P37MLHP' THEN 'P37K96P'
  WHEN package_id = 'P37K96S' THEN 'P37K96P'
  ELSE package_id
END AS package_id
```

---

### 6. `UTM UPDATES`

**Schedule**: Daily 08:00 UTC
**Status**: ✅ SUCCEEDED
**Last Updated**: Sep 30, 2025

#### Purpose
Refreshes all UTM source tables and builds a unified master UTM table.

#### Targets
```
Query 1: giant-spoon-299605.data_model_2025.mm_utms_snapshot (TABLE)
Query 2: looker-studio-pro-452620.utm_scrap.b_sup_pivt_unioned_tab (TABLE)
Query 3: looker-studio-pro-452620.utm_scrap.master_utms_raw (TABLE)
```

#### Data Flow
```
mm_utms (Google Sheet) ──► mm_utms_snapshot
b_sup_pivt_unioned (view) ──► b_sup_pivt_unioned_tab

mm_utms_snapshot ─────┐
b_sup_pivt_unioned ───┼──► master_utms_raw (UNION ALL)
dcm_plus_utms_upload ─┘
```

#### Master UTM Schema
| Field | Source |
|-------|--------|
| `placement_raw` | Direct from source |
| `creative_raw` | Direct from source |
| `id_fromPlacement` | `REGEXP_EXTRACT(placement, r'_(\d+)_')` |
| `id_fromUtmContent` | `REGEXP_EXTRACT(utm_content, r'-(\d+)')` |
| `utm_source/medium/campaign/content/term` | Extracted or direct |
| `sourced_from` | Source table identifier |

---

### 7. `adif update`

**Schedule**: Daily 09:00 UTC
**Status**: ✅ SUCCEEDED

#### Purpose
Refreshes ADIF First-Party Data (FPD) with daily range expansion.

#### Target
```
looker-studio-pro-452620.landing.adif_fpd_data_ranged
```

#### Source
Google Sheets FPD data collected via `util_collect_fpd_v2.r`

---

### 8. `adif_prisma_expanded_plus_dcm_v2`

**Schedule**: Daily 09:00 UTC
**Status**: ✅ SUCCEEDED

#### Purpose
Complex multi-source integration for ADIF pipeline using FULL OUTER JOINs.

#### Target
```
looker-studio-pro-452620.repo_stg.adif__prisma_expanded_plus_dcm_view_v3_test
```

#### Source Tables
- `DCM.20250505_costModel_v5` (DCM delivery)
- `landing.adif_fpd_data_ranged` (First-party data)
- `20250327_data_model.prisma_expanded_full` (Prisma planning)

#### Join Strategy
```
DCM ──┐
      ├──► FULL OUTER JOIN ──► DCM+FPD
FPD ──┘                            │
                                   ├──► FULL OUTER JOIN ──► Final
Prisma ────────────────────────────┘
```

#### Key Feature: Data Prioritization
```sql
-- FPD takes precedence over DCM
COALESCE(fpd_spend, d_daily_recalculated_cost) AS final_spend,
COALESCE(fpd_impressions, d_daily_recalculated_imps) AS final_impressions
```

---

### 9. `ADIF_FullDataRefresh_2604`

**Schedule**: Every 10 hours
**Status**: ✅ SUCCEEDED
**Verified Against Live Config**: Apr 8, 2026

#### Purpose
This is the current main ADIF scheduled refresh. It rebuilds the V2 shadow output table with one scheduled query that combines:

- the updated digital ADIF base view
- normalized ADIF social rows
- social pacing inputs

#### Live Transfer Config
```text
projects/671028410185/locations/us/transferConfigs/6a40bbfa-0000-2ee2-a61f-582429bc84e0
```

#### Live Metadata
- Display name: `ADIF_FullDataRefresh_2604`
- Data source: `scheduled_query`
- Owner: `gene.tsenter@giantspoon.com`
- Last config update: `2026-04-07T22:03:00.832621Z`
- Next observed run time: `2026-04-09T04:03:00Z`

#### Target
```text
looker-studio-pro-452620.repo_stg.adif__mainDataTable_notebook_v2_test
```

#### Source Tables
- `looker-studio-pro-452620.repo_stg.adif__prisma_expanded_plus_dcm_updated_fpd_view`
- `looker-studio-pro-452620.repo_stg.stg__adif__social_crossplatform`
- `looker-studio-pro-452620.repo_int.crossplatform_pacing`

#### Important Drift Note
- Earlier ADIF docs in this repo often described the notebook output table `repo_stg.adif__mainDataTable_notebook` as the main production target.
- The live transfer config verified on Apr 8, 2026 currently writes `repo_stg.adif__mainDataTable_notebook_v2_test` instead.
- At inspection time, `repo_stg.adif__mainDataTable_notebook_v2_test` had 16,680 rows and was modified on `2026-04-08T18:03:43Z`.
- The older `repo_stg.adif__mainDataTable_notebook` table still existed with 16,663 rows and an older modification time of `2026-04-06T14:02:15Z`.

#### Query Shape
The live SQL is a single-pass V2 rebuild, not the older two-step notebook insert pattern:

- `CREATE OR REPLACE TABLE repo_stg.adif__mainDataTable_notebook_v2_test`
- build digital rows from the updated ADIF base view
- build social rows with token parsing, pacing allocation, and source-aware metric handling
- `UNION ALL` the two branches
- derive canonical final metrics and package-level source labels

---

### 10. `basis_update`

**Schedule**: Daily 10:00 UTC
**Status**: ✅ SUCCEEDED
**Last Updated**: Nov 19, 2025

#### Purpose
MERGE (UPSERT) operation to synchronize Basis delivery data from Google Sheets.

#### Target
```
looker-studio-pro-452620.repo_stg.basis_master2
```

#### Source
```
giant-spoon-299605.data_model_2025.basis_gsheet2
WHERE latest_record = 1
  AND day >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
```

#### MERGE Logic
```sql
MERGE INTO basis_master2 AS TARGET
USING (filtered_source) AS SOURCE
ON TARGET.date = SOURCE.day
   AND TARGET.campaign = SOURCE.campaign_name
   AND TARGET.package_roadblock = SOURCE.line_item_name
   AND TARGET.tactic = SOURCE.basis_tactic
   AND TARGET.placement = SOURCE.placement
   AND TARGET.creative_name = SOURCE.creative_name
   AND TARGET.creative_grouping = SOURCE.creative_grouping_creative_grouping
   AND TARGET.basis_dsp_tactic_group = SOURCE.basis_dsp_tactic_group

WHEN MATCHED THEN UPDATE SET
  impressions, clicks, media_cost, video_audio_plays,
  video_views, video_audio_fully_played, viewable_impressions

WHEN NOT MATCHED THEN INSERT (all_columns)
```

---

### 11. `stg__olipop__crossplatform_raw_tbl_sched`

**Schedule**: Daily 10:00 UTC
**Status**: ✅ SUCCEEDED
**Last Updated**: May 29, 2026

#### Purpose
Builds `repo_stg.stg__olipop__crossplatform_raw_tbl`, which is the raw social fact table that eventually feeds the Olipop social branch of `Olipop.MMM_crossplatform`.

More specifically, the live scheduled query:

- picks one of two candidate ad-delivery tables at runtime
- uses the most recently updated candidate as the delivery source for that run
- left joins cross-platform video metrics from `repo_stg.stg__olipop_videoviews_crossplatform`
- merges Apollo rows from [WP normalized staging](https://console.cloud.google.com/bigquery?project=looker-studio-pro-452620&p=looker-studio-pro-452620&d=repo_stg&t=stg__wp__search_data_template_daily&page=table), with WP nonblank values primary and standard rows/fields as fallback
- carries transformed WP creative image URLs in `wp_creative_img`
- retains cross-campaign Apollo ad-ID rows with visible `publish_pending_source_owner_review` provenance pending source-owner clarification
- writes the finished result into `repo_stg.stg__olipop__crossplatform_raw_tbl`

The two delivery-source options are:

- `giant-spoon-299605.ad_reporting_transformed.ad_reporting__ad_report`
- `giant-spoon-299605.ad_reporting_reports.ad_reporting__ad_report`

The query checks metadata from each dataset's `__TABLES__` table and chooses whichever copy of `ad_reporting__ad_report` has the newer `last_modified_time`.

Why it appears to do this:

- I can prove the mechanism from the live SQL, but the exact business reason is an inference
- the most likely reason is operational resilience during source lag or source transitions
- in plain English, the build is trying to use the freshest available version of the ad-delivery table instead of assuming one dataset is always current

Which version is out of date:

- the live production scheduled query is newer and should be treated as correct for current operations
- the stale copy is the local repo SQL in [`sql/marts/olipop/mart__olipop__crossplatform.sql`](../sql/marts/olipop/mart__olipop__crossplatform.sql), which still hardcodes `ad_reporting_transformed.ad_reporting__ad_report` and does not include the runtime source-selection block

#### Target
```
looker-studio-pro-452620.repo_stg.stg__olipop__crossplatform_raw_tbl
```

#### Source Tables
- `giant-spoon-299605.ad_reporting_transformed.__TABLES__` (metadata used to check table freshness)
- `giant-spoon-299605.ad_reporting_reports.__TABLES__` (metadata used to check table freshness)
- `giant-spoon-299605.ad_reporting_transformed.ad_reporting__ad_report` (delivery candidate)
- `giant-spoon-299605.ad_reporting_reports.ad_reporting__ad_report` (delivery candidate)
- `repo_stg.stg__olipop_videoviews_crossplatform` (video metrics)
- `repo_stg.stg__wp__search_data_template_daily` (controlled WP production Sheet snapshot)

#### WP Production Rule

| Rule | Production behavior |
| --- | --- |
| Record grain | Date, normalized platform, campaign, ad group, and ad. |
| Overlap handling | WP values win cell by cell for matching Apollo rows; numeric zero is a supplied value. |
| Google classification | `_Search_` becomes Paid Search and `_YT_` becomes Online Video; Sheet channel is the fallback. |
| Pending identity question | Cross-campaign rows sharing date/platform/ad ID are included and flagged until the source owner clarifies the rule. |
| Sheet refresh | The staging loader remains a controlled manual refresh while clarification is pending; this scheduled SQL rebuild consumes the current staged snapshot. |

Production and rollback SQL:

- [WP production scheduled builder](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/wp/sql/create_stg_crossplatform_wp_primary_production.sql)
- [Pre-WP shared-social rollback builder](/Users/eugenetsenter/Looker_clonedRepo/looker_personal/wp/sql/rollback_stg_crossplatform_pre_wp_production.sql)

#### Runtime Source Selection
```sql
WITH source_choice AS (
  SELECT source_name
  FROM (
    SELECT
      'transformed' AS source_name,
      TIMESTAMP_MILLIS(last_modified_time) AS last_modified_time
    FROM `giant-spoon-299605.ad_reporting_transformed.__TABLES__`
    WHERE table_id = 'ad_reporting__ad_report'

    UNION ALL

    SELECT
      'reports' AS source_name,
      TIMESTAMP_MILLIS(last_modified_time) AS last_modified_time
    FROM `giant-spoon-299605.ad_reporting_reports.__TABLES__`
    WHERE table_id = 'ad_reporting__ad_report'
  )
  ORDER BY last_modified_time DESC, source_name DESC
  LIMIT 1
)
```

#### Join Keys
```sql
ON a.source_relation = b.source_relation
AND a.date_day = b.date_day
AND a.campaign_id = b.campaign_id
AND a.ad_group_id = b.ad_group_id
AND a.ad_id = b.ad_id
```

#### Video Flag Logic
```sql
CASE
  WHEN COALESCE(b.video_play, 0) + COALESCE(b.video_view, 0) > 0
  THEN 'video'
  ELSE NULL
END AS video_flag
```

> Note: older name-based `video_flag` checks still appear as commented-out lines inside the live query text, but they are not active in production.

#### Related Documentation
- [`olipop/README.md`](../olipop/README.md) for the full `Olipop.MMM_crossplatform` dependency graph and a fuller local-vs-live drift explanation

---

### 12. `mart__pacing_table`

**Schedule**: Daily 10:00 UTC
**Status**: ✅ SUCCEEDED
**Last Updated**: Aug 18, 2025

#### Purpose
Incremental refresh of cross-platform pacing data with 15-day rolling window.

#### Target
```
looker-studio-pro-452620.repo_mart.fct_crossplatform_pacing_daily
```

#### Source Tables
- `giant-spoon-299605.ad_reporting_transformed.ad_reporting__ad_report` (spend)
- `repo_int.crossplatform_pacing` (budget/flight metadata)

#### Incremental Strategy
```sql
DECLARE window_days INT64 DEFAULT 15;
DECLARE window_start DATE DEFAULT DATE_SUB(CURRENT_DATE(), INTERVAL window_days DAY);

-- 1. Compute recent window into temp table
CREATE OR REPLACE TEMP TABLE recent AS ...

-- 2. Delete old window data
DELETE FROM fct_crossplatform_pacing_daily WHERE date >= window_start;

-- 3. Insert recomputed data
INSERT INTO fct_crossplatform_pacing_daily SELECT * FROM recent;
```

#### Pacing Metrics
| Metric | Calculation |
|--------|-------------|
| `progress_ratio` | `progress_days / total_days` |
| `expected_spend_to_date` | `progress_ratio * final_budget` |
| `pace_ratio` | `spend / expected_spend_to_date` |
| `variance_amount` | `spend - expected_spend_to_date` |
| `status_flag` | `under` (<90%), `on_target`, `over` (>110%) |

---

### 13. `ext_mm_mft_scheadule`

**Schedule**: Daily 10:00 UTC
**Status**: ✅ SUCCEEDED

#### Purpose
External export of MFT data for downstream consumption.

#### Target
External system / Google Sheets

---

### 14. `mart__dcm__joined_0519` ❌ FAILED

**Schedule**: Daily 03:00 UTC
**Status**: ❌ FAILED
**Last Updated**: May 19, 2025

#### Purpose
Creates DCM + UTM joined table (appears to be superseded by `mm_dcm_costmodel`).

#### Target
```
looker-studio-pro-452620.repo_tables.dcm
```

#### Issue
This query has been failing since May 2025. Consider disabling or investigating the root cause.

---

## Dependency Graph

```
                         ┌─────────────────────────┐
                         │   RAW DATA SOURCES      │
                         │ (Google Sheets, APIs)   │
                         └───────────┬─────────────┘
                                     │
        ┌────────────────────────────┼────────────────────────────┐
        │                            │                            │
        ▼                            ▼                            ▼
┌───────────────┐          ┌─────────────────┐          ┌─────────────────┐
│ process_prisma│          │ 250813_dedupe   │          │ basis_update    │
│ (07:00)       │          │ (05:00)         │          │ (10:00)         │
└───────┬───────┘          └────────┬────────┘          └────────┬────────┘
        │                           │                            │
        ▼                           │                            │
┌───────────────┐                   │                            │
│Prisma_expanded│                   │                            │
│ (07:00)       │                   │                            │
└───────┬───────┘                   │                            │
        │                           │                            │
        ├───────────────────────────┼────────────────────────────┤
        │                           │                            │
        ▼                           ▼                            ▼
┌───────────────────────────────────────────────────────────────────────┐
│                        mm_dcm_costmodel (Every 4 hours)               │
│                     DCM.20250505_costModel_v5                         │
└───────────────────────────────────┬───────────────────────────────────┘
                                    │
        ┌───────────────────────────┼───────────────────────────┐
        │                           │                           │
        ▼                           ▼                           ▼
┌───────────────┐          ┌─────────────────┐          ┌───────────────┐
│ adif update   │          │ UTM UPDATES     │          │stg__olipop    │
│ (09:00)       │          │ (08:00)         │          │ (10:00)       │
└───────┬───────┘          └────────┬────────┘          └───────┬───────┘
        │                           │                           │
        ▼                           │                           ▼
┌───────────────┐                   │                  ┌───────────────┐
│adif_prisma_   │                   │                  │mart__pacing   │
│expanded (09:00)                   │                  │ (10:00)       │
└───────┬───────┘                   │                  └───────────────┘
        │                           │
        ▼                           │
┌──────────────────────┐            │
│ ADIF_FullDataRefresh │            │
│ (every 10 hours)     │            │
└───────────┬──────────┘            │
            │                       │
            ▼                       ▼
   ┌─────────────────────┐  ┌─────────────────────┐
   │  ADIF V2 shadow     │  │   MART LAYER        │
   │  scheduled output   │  │ (mft_view, adif,    │
   │  repo_stg.adif__    │  │  pacing, olipop)    │
   │  mainDataTable_...  │  └─────────────────────┘
   └─────────────────────┘
```

---

## Maintenance Notes

### Health Check Query
```sql
-- Check recent scheduled query runs only
SELECT
  creation_time,
  end_time,
  job_id,
  state,
  error_result,
  (SELECT l.value FROM UNNEST(labels) l WHERE l.key = 'dts_config_id' LIMIT 1) AS transfer_config_id,
  (SELECT l.value FROM UNNEST(labels) l WHERE l.key = 'dts_run_id' LIMIT 1) AS transfer_run_id
FROM `region-us`.INFORMATION_SCHEMA.JOBS
WHERE job_type = 'QUERY'
  AND creation_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR)
  AND EXISTS (
    SELECT 1
    FROM UNNEST(labels) l
    WHERE l.key = 'data_source_id'
      AND l.value = 'scheduled_query'
  )
ORDER BY creation_time DESC
```

### Common Issues

1. **`mart__dcm__joined_0519` FAILED**:
   - Appears deprecated - consider disabling
   - Functionality covered by `mm_dcm_costmodel`

2. **Timing Dependencies**:
   - `Prisma_expanded` depends on `process_prisma` (both at 07:00 UTC; race risk)
   - Recommended: run `Prisma_expanded` at 07:15 UTC or add retry/backoff
   - `adif_prisma_expanded_plus_dcm_v2` depends on DCM cost model (09:00 UTC vs 4-hour cadence); monitor for stale upstream data
   - `ADIF_FullDataRefresh_2604` depends on the updated ADIF base view plus social and pacing inputs; if shadow-vs-production promotion status matters, compare `repo_stg.adif__mainDataTable_notebook_v2_test` against `repo_stg.adif__mainDataTable_notebook` before changing downstream consumers

3. **Fee Filtering**:
   - Multiple queries filter `%feeorder%`, `%fees_%`, `%fee_%`
   - Ensure consistent filtering across pipelines

### Recommended Monitoring
- Set up failure email alerts for all queries
- Monitor `mm_dcm_costmodel` latency (runs every 4 hours)
- Track row counts in target tables for anomaly detection

---

**Last Updated**: April 8, 2026
