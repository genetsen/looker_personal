# Social Layering Pipeline

This sub-project owns the social append branch for ADIF.
The current live automated refresh is the BigQuery scheduled query `ADIF_FullDataRefresh_2604`, which writes to `looker-studio-pro-452620.repo_stg.adif__mainDataTable_notebook_v2_test`.
The notebook (`build__adif__prisma_expanded_plus_dcm_with_social_tbl.ipynb`) remains an important reference for the earlier two-stage build path.

## Lineage Segment to Final Output

```mermaid
flowchart LR
  subgraph raw_social["Raw Social Inputs"]
    social_raw["repo_stg.stg__olipop__crossplatform_raw_tbl"]
    pacing["repo_int.crossplatform_pacing"]
  end

  subgraph social_models["Social Layer Models"]
    social_stg["repo_stg.stg__adif__social_crossplatform"]
    social_nb_s2["Notebook Section 2 (social insert)"]
  end

  subgraph notebook["Notebook Build"]
    social_nb_s1["Live scheduled query digital branch"]
    target_tbl["repo_stg.adif__mainDataTable_notebook_v2_test"]
  end

  social_raw --> social_stg
  social_stg --> social_nb_s2
  pacing --> social_nb_s2
  core_upd["repo_stg.adif__prisma_expanded_plus_dcm_updated_fpd_view"] --> social_nb_s1
  social_nb_s1 --> target_tbl
  social_nb_s2 --> target_tbl
```

## Current Live Refresh

- Transfer config: `projects/671028410185/locations/us/transferConfigs/6a40bbfa-0000-2ee2-a61f-582429bc84e0`
- Display name: `ADIF_FullDataRefresh_2604`
- Schedule: `every 10 hours`
- Verified on: `2026-04-10`
- Live target table: `looker-studio-pro-452620.repo_stg.adif__mainDataTable_notebook_v2_test`

## Reference Notebook and Archived SQL

- Reference notebook: `projects/social_layering/build__adif__prisma_expanded_plus_dcm_with_social_tbl.ipynb`
- Older notebook output table: `looker-studio-pro-452620.repo_stg.adif__mainDataTable_notebook`
- Legacy scheduled SQL and duplicate notebook copies: `projects/social_layering/archive/legacy_scheduled_sql/`

## Source Coverage

### Live V2 Digital Branch (`CREATE OR REPLACE TABLE`)
- Source: `looker-studio-pro-452620.repo_stg.adif__prisma_expanded_plus_dcm_updated_fpd_view`
- Output: `looker-studio-pro-452620.repo_stg.adif__mainDataTable_notebook_v2_test`
- Behavior: rebuilds the full target in one scheduled query instead of relying on a separate notebook Section 1 run

### Live V2 Social Logic
- Social source: `looker-studio-pro-452620.repo_stg.stg__adif__social_crossplatform`
- Pacing source: `looker-studio-pro-452620.repo_int.crossplatform_pacing`
- Behavior:
  - Normalizes social platform to `meta` / `tiktok`
  - Maps `ad_set -> package`, `ad -> placement`
  - Computes package pacing rollups and over/under flags
  - Unions social rows into the rebuilt target table inside the V2 single-pass scheduled query

### `repo_int.crossplatform_pacing` upstream views used by live scheduled-query logic
- `looker-studio-pro-452620.repo_tables.int__tiktok__combined_history_dedupe_view`
- `looker-studio-pro-452620.repo_facebook.stg__fb_combined_history`
- `looker-studio-pro-452620.repo_google_ads.stg__ga_combined_history`

## Verification Notes

Warehouse-side verification completed on `2026-04-10`:

- `repo_stg.adif__mainDataTable_notebook_v2_test` exists and had `16,701` rows at inspection time
- `repo_stg.adif__mainDataTable_notebook` also exists and had `16,663` rows at inspection time
- The V2 test table was modified on `2026-04-10T10:03:13Z`, which is newer than the older notebook table modification time `2026-04-06T14:02:15Z`
- `repo_stg.adif__prisma_expanded_plus_dcm_updated_fpd_view`, `repo_stg.stg__adif__social_crossplatform`, and `repo_int.crossplatform_pacing` still match the documented live lineage chain

The notebook still contains useful validation queries for:
- Table totals (`row_count`, `min_date`, `max_date`, `total_spend`, `total_impressions`) for 2026
- Breakdown by `data_source_primary` for 2026
- Breakdown by `supplier_code`, `p_package_friendly` for 2026
- Cross-check source totals from `repo_stg.stg__adif__social_crossplatform` by platform for 2026

## Editable Mapping Matrix

- `projects/social_layering/social_mapping_matrix_editable.csv`
- Purpose: editable mapping table for social ad set/ad level to main-table package/placement with sample mapped values.

## Validation Script

- `projects/social_layering/sql/test__adif__social_mapping_v2_vs_current.sql`
- Purpose: validates proposed v2 social mapping totals and pacing against raw social data and current social table output.

## QA Dashboard Snapshot

- File: `projects/social_layering/dashboard/adif__mainDataTable_notebook_v2_test_qa_dashboard.html`
- Purpose:
  - Shows upstream freshness across direct and leaf sources used by `repo_stg.adif__mainDataTable_notebook_v2_test`
  - Summarizes row-source mix and package-source mix in the target table
  - Adds an interactive lineage map so each pipeline table can be clicked for snapshot details
  - Highlights carry-forward QA for digital, social, and updated FPD lanes
- Snapshot notes:
  - This HTML is a point-in-time export built from BigQuery results captured on `2026-04-07`
  - Status logic stays pinned to that same reference day so the snapshot stays stable when reopened later
  - Treat the HTML row counts and freshness badges as a snapshot, not as live production truth

## One-Page Guide

- File: `projects/social_layering/ADIF_MAIN_PIPELINE_LINEAGE_1PAGER.md`
- Purpose: beginner-friendly summary of the current production lineage from raw inputs to `repo_stg.adif__mainDataTable_notebook_v2_test`
